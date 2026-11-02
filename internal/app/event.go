package app

import (
	"context"
	"encoding/json"
	"fmt"
	"unicode"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// MaxGatewayEventIDLen bounds the gateway's event id.
const MaxGatewayEventIDLen = 128

// GatewayEvent is a status notification pushed by the payment gateway. It is
// also the payload of the apply_gateway_event task.
type GatewayEvent struct {
	ID         string         `json:"id"`
	PaymentID  uuid.UUID      `json:"payment_id"`
	GatewayRef string         `json:"gateway_ref"`
	Status     payment.Status `json:"status"`
}

// AcceptGatewayEvent stores a webhook as an inbox task and returns quickly.
// The gateway's event id is the dedup key, so a redelivered event is accepted
// again but applied once.
func (s *Service) AcceptGatewayEvent(ctx context.Context, e GatewayEvent) error {
	if err := validateGatewayEvent(e); err != nil {
		return err
	}
	if _, err := s.payments.Payment(ctx, e.PaymentID); err != nil {
		return err
	}
	payload, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("encode gateway event: %w", err)
	}
	task := ports.TaskSpec{
		Kind:      ports.TaskApplyGatewayEvent,
		PaymentID: e.PaymentID,
		DedupKey:  e.ID,
		Payload:   payload,
		RunAfter:  s.clock.Now(),
	}
	if _, err := s.tasks.Enqueue(ctx, task); err != nil {
		return fmt.Errorf("enqueue gateway event: %w", err)
	}
	return nil
}

func validateGatewayEvent(e GatewayEvent) error {
	if e.ID == "" || len(e.ID) > MaxGatewayEventIDLen {
		return &payment.ValidationError{Field: "id", Reason: fmt.Sprintf("must be 1 to %d characters", MaxGatewayEventIDLen)}
	}
	for _, r := range e.ID {
		if r > unicode.MaxASCII || !unicode.IsPrint(r) {
			return &payment.ValidationError{Field: "id", Reason: "must be printable ASCII"}
		}
	}
	if e.PaymentID == uuid.Nil {
		return &payment.ValidationError{Field: "payment_id", Reason: "is required"}
	}
	if !e.Status.Valid() || e.Status == payment.StatusPending {
		return &payment.ValidationError{Field: "status", Reason: "is not a gateway status"}
	}
	return nil
}

// ApplyGatewayEvent is the task handler for a stored gateway event.
func (s *Service) ApplyGatewayEvent(ctx context.Context, payload json.RawMessage) error {
	var e GatewayEvent
	if err := json.Unmarshal(payload, &e); err != nil {
		return ports.Permanent(fmt.Errorf("decode gateway event: %w", err))
	}
	return s.applyStatus(ctx, e.PaymentID, e.Status, e.GatewayRef)
}
