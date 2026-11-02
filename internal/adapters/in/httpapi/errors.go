package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// statusClientClosedRequest is logged when the client disconnects first.
const statusClientClosedRequest = 499

// errBadSignature means a webhook failed signature verification.
var errBadSignature = errors.New("invalid webhook signature")

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

// writeError maps an error to an HTTP response. It is the only place that
// knows which error means which status code. Unexpected errors are logged
// here, once, and the client gets a generic message without internals.
func writeError(w http.ResponseWriter, r *http.Request, log *slog.Logger, err error) {
	var verr *payment.ValidationError
	var maxBytes *http.MaxBytesError
	switch {
	case errors.As(err, &verr):
		writeJSON(w, http.StatusBadRequest, errorBody{Code: "invalid_argument", Message: verr.Error(), Field: verr.Field})
	case errors.As(err, &maxBytes):
		writeJSON(w, http.StatusRequestEntityTooLarge, errorBody{Code: "too_large", Message: "request body too large"})
	case errors.Is(err, errBadSignature):
		writeJSON(w, http.StatusUnauthorized, errorBody{Code: "unauthorized", Message: err.Error()})
	case errors.Is(err, payment.ErrNotFound):
		writeJSON(w, http.StatusNotFound, errorBody{Code: "not_found", Message: err.Error()})
	case errors.Is(err, payment.ErrIdempotencyConflict):
		writeJSON(w, http.StatusUnprocessableEntity, errorBody{Code: "idempotency_conflict", Message: err.Error()})
	case errors.Is(err, ports.ErrTooManyWaiters):
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusTooManyRequests, errorBody{Code: "too_many_waiters", Message: "too many open long polls, retry shortly"})
	case errors.Is(err, ports.ErrUnavailable):
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, errorBody{Code: "unavailable", Message: "temporarily unavailable"})
	case errors.Is(err, context.Canceled) && r.Context().Err() != nil:
		// The client has gone; nobody will read a response. 499 ("client
		// closed request", an nginx convention) only reaches the access log,
		// so abandoned long polls are not counted as successes.
		log.DebugContext(r.Context(), "client went away", "err", err)
		w.WriteHeader(statusClientClosedRequest)
	default:
		log.ErrorContext(r.Context(), "request failed", "err", err)
		writeJSON(w, http.StatusInternalServerError, errorBody{Code: "internal", Message: "internal error"})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// decodeJSON reads a size-limited JSON body and rejects unknown fields, so a
// misspelt field fails loudly instead of being ignored.
func decodeJSON(w http.ResponseWriter, r *http.Request, maxBytes int64, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return err
		}
		return &payment.ValidationError{Field: "body", Reason: "malformed JSON: " + err.Error()}
	}
	if dec.More() {
		return &payment.ValidationError{Field: "body", Reason: "must contain a single JSON object"}
	}
	return nil
}
