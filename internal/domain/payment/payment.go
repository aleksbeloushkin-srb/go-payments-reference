package payment

import (
	"fmt"
	"log/slog"
	"time"
	"unicode"

	"github.com/google/uuid"
)

// MaxMerchantIDLen bounds the merchant identifier.
const MaxMerchantIDLen = 64

// Payment is the aggregate root: one transfer of money from a bank client to
// a merchant. Version grows by one on every persisted status change; clients
// use it as a change marker for long polling, and event consumers use it to
// drop stale or reordered events.
type Payment struct {
	ID             uuid.UUID
	MerchantID     string
	Amount         Money
	Status         Status
	Version        int64
	GatewayRef     string
	IdempotencyKey string
	RequestHash    string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// New builds a payment in the pending status with version 1.
func New(id uuid.UUID, merchantID string, amount Money, idempotencyKey, requestHash string, now time.Time) (Payment, error) {
	if err := validateMerchantID(merchantID); err != nil {
		return Payment{}, err
	}
	return Payment{
		ID:             id,
		MerchantID:     merchantID,
		Amount:         amount,
		Status:         StatusPending,
		Version:        1,
		IdempotencyKey: idempotencyKey,
		RequestHash:    requestHash,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, nil
}

func validateMerchantID(id string) error {
	if id == "" || len(id) > MaxMerchantIDLen {
		return &ValidationError{Field: "merchant_id", Reason: fmt.Sprintf("must be 1 to %d characters", MaxMerchantIDLen)}
	}
	for _, r := range id {
		if r > unicode.MaxASCII || !unicode.IsPrint(r) {
			return &ValidationError{Field: "merchant_id", Reason: "must be printable ASCII"}
		}
	}
	return nil
}

// TransitionTo moves the payment to next if the state machine allows it and
// bumps the version. Moving to the current status is a no-op
// (changed == false), which makes duplicate events harmless. Anything else,
// including any attempt to leave a final status, returns
// ErrInvalidTransition and leaves the payment untouched.
func (p *Payment) TransitionTo(next Status, now time.Time) (changed bool, err error) {
	if !next.Valid() {
		return false, fmt.Errorf("%w: unknown status %q", ErrInvalidTransition, next)
	}
	if p.Status == next {
		return false, nil
	}
	if !p.Status.CanTransitionTo(next) {
		return false, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, p.Status, next)
	}
	p.Status = next
	p.Version++
	p.UpdatedAt = now
	return true, nil
}

// LogValue limits logs to an allowlist of fields. The idempotency key and
// the request hash are client data and stay out of logs.
func (p Payment) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("id", p.ID.String()),
		slog.String("merchant_id", p.MerchantID),
		slog.String("status", string(p.Status)),
		slog.Int64("version", p.Version),
		slog.Int64("amount_minor", p.Amount.AmountMinor),
		slog.String("currency", string(p.Amount.Currency)),
	)
}
