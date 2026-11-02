package payment

import (
	"time"

	"github.com/google/uuid"
)

// EventType names an integration event published through the outbox.
type EventType string

// Event types.
const (
	EventCreated       EventType = "payment.created"
	EventStatusChanged EventType = "payment.status_changed"
)

// Event is a fact about a payment that other systems (statements,
// notifications to the client, analytics) may consume. It is written to the
// outbox in the same transaction as the change it describes. Consumers must
// tolerate duplicates, because the relay is at-least-once, and use Version to
// discard stale or reordered events.
type Event struct {
	Type        EventType `json:"type"`
	PaymentID   uuid.UUID `json:"payment_id"`
	MerchantID  string    `json:"merchant_id"`
	Status      Status    `json:"status"`
	Version     int64     `json:"version"`
	AmountMinor int64     `json:"amount_minor"`
	Currency    Currency  `json:"currency"`
	OccurredAt  time.Time `json:"occurred_at"`
}

// NewEvent snapshots p into an event of type t.
func NewEvent(t EventType, p Payment) Event {
	return Event{
		Type:        t,
		PaymentID:   p.ID,
		MerchantID:  p.MerchantID,
		Status:      p.Status,
		Version:     p.Version,
		AmountMinor: p.Amount.AmountMinor,
		Currency:    p.Amount.Currency,
		OccurredAt:  p.UpdatedAt,
	}
}
