package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/app"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/pkg/hmacsig"
)

const (
	maxEventBody = 64 << 10
	// signatureTolerance bounds clock skew and delivery delay; older
	// signatures are treated as replays.
	signatureTolerance = 5 * time.Minute
)

// gatewayEventJSON is the webhook body sent by the gateway.
type gatewayEventJSON struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Created int64  `json:"created"`
	Data    struct {
		Ref       string `json:"ref"`
		Reference string `json:"reference"`
		Status    string `json:"status"`
	} `json:"data"`
}

// gatewayEvent handles POST /v1/gateway/events. It authenticates the
// signature before parsing anything, stores the event as a task and answers
// 202. Duplicate deliveries get 202 too; the task is deduplicated by event
// id. A non-2xx answer tells the gateway to deliver again later.
func (h *handler) gatewayEvent(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxEventBody))
	if err != nil {
		writeError(w, r, h.log, err)
		return
	}
	if err := hmacsig.Verify(h.secret, r.Header.Get(hmacsig.Header), body, h.clock.Now(), signatureTolerance); err != nil {
		h.log.WarnContext(r.Context(), "webhook rejected", "err", err)
		writeError(w, r, h.log, errBadSignature)
		return
	}
	// Unlike our own API, a third-party payload is decoded leniently:
	// the gateway may add fields at any time, and rejecting them would stop
	// every webhook until we redeploy.
	var in gatewayEventJSON
	if err := json.Unmarshal(body, &in); err != nil {
		writeError(w, r, h.log, &payment.ValidationError{Field: "body", Reason: "malformed JSON: " + err.Error()})
		return
	}
	paymentID, err := uuid.Parse(in.Data.Reference)
	if err != nil {
		writeError(w, r, h.log, &payment.ValidationError{Field: "data.reference", Reason: "must be a payment id"})
		return
	}
	err = h.events.AcceptGatewayEvent(r.Context(), app.GatewayEvent{
		ID:         in.ID,
		PaymentID:  paymentID,
		GatewayRef: in.Data.Ref,
		Status:     payment.Status(in.Data.Status),
	})
	if err != nil {
		writeError(w, r, h.log, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"received": true})
}
