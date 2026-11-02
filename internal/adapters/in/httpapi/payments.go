package httpapi

import (
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/app"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
)

const maxCreateBody = 16 << 10

type createPaymentJSON struct {
	MerchantID  string `json:"merchant_id"`
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
}

type createdJSON struct {
	ID      uuid.UUID `json:"id"`
	Status  string    `json:"status"`
	Version int64     `json:"version"`
}

type paymentJSON struct {
	ID          uuid.UUID `json:"id"`
	MerchantID  string    `json:"merchant_id"`
	AmountMinor int64     `json:"amount_minor"`
	Currency    string    `json:"currency"`
	Status      string    `json:"status"`
	Version     int64     `json:"version"`
	GatewayRef  string    `json:"gateway_ref,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// waitJSON is the long-poll answer: the payment as of the answer, and
// whether its version moved past the client's marker.
type waitJSON struct {
	paymentJSON
	Modified bool `json:"modified"`
}

func toJSON(p payment.Payment) paymentJSON {
	return paymentJSON{
		ID:          p.ID,
		MerchantID:  p.MerchantID,
		AmountMinor: p.Amount.AmountMinor,
		Currency:    string(p.Amount.Currency),
		Status:      string(p.Status),
		Version:     p.Version,
		GatewayRef:  p.GatewayRef,
		CreatedAt:   p.CreatedAt.UTC(),
		UpdatedAt:   p.UpdatedAt.UTC(),
	}
}

// createPayment handles POST /v1/payments. 201 for a new payment, 200 for a
// replay with the same Idempotency-Key and body, 422 for the same key with a
// different body.
func (h *handler) createPayment(w http.ResponseWriter, r *http.Request) {
	var in createPaymentJSON
	if err := decodeJSON(w, r, maxCreateBody, &in); err != nil {
		writeError(w, r, h.log, err)
		return
	}
	res, err := h.creator.CreatePayment(r.Context(), app.CreatePaymentRequest{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		MerchantID:     in.MerchantID,
		AmountMinor:    in.AmountMinor,
		Currency:       in.Currency,
	})
	if err != nil {
		writeError(w, r, h.log, err)
		return
	}
	status := http.StatusOK
	if res.Created {
		status = http.StatusCreated
		w.Header().Set("Location", "/v1/payments/"+res.Payment.ID.String())
	}
	writeJSON(w, status, createdJSON{ID: res.Payment.ID, Status: string(res.Payment.Status), Version: res.Payment.Version})
}

// payment handles GET /v1/payments/{id}.
func (h *handler) payment(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeError(w, r, h.log, err)
		return
	}
	p, err := h.reader.Payment(r.Context(), id)
	if err != nil {
		writeError(w, r, h.log, err)
		return
	}
	writeJSON(w, http.StatusOK, toJSON(p))
}

// waitPayment handles GET /v1/payments/{id}/wait?version=N, the long poll.
// It answers when the version exceeds N or after the soft timeout, with
// "modified" telling the two apart. The client sends the version it last
// saw and repeats.
func (h *handler) waitPayment(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeError(w, r, h.log, err)
		return
	}
	version, err := strconv.ParseInt(r.URL.Query().Get("version"), 10, 64)
	if err != nil {
		writeError(w, r, h.log, &payment.ValidationError{Field: "version", Reason: "query parameter must be an integer"})
		return
	}
	res, err := h.reader.WaitPayment(r.Context(), id, version)
	if err != nil {
		writeError(w, r, h.log, err)
		return
	}
	writeJSON(w, http.StatusOK, waitJSON{paymentJSON: toJSON(res.Payment), Modified: res.Modified})
}

func pathID(r *http.Request) (uuid.UUID, error) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		return uuid.Nil, &payment.ValidationError{Field: "id", Reason: "must be a UUID"}
	}
	return id, nil
}
