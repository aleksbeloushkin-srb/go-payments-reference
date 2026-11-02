package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"unicode"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// MaxIdempotencyKeyLen bounds the Idempotency-Key header.
const MaxIdempotencyKeyLen = 255

// CreatePaymentRequest is the input of CreatePayment.
type CreatePaymentRequest struct {
	IdempotencyKey string
	MerchantID     string
	AmountMinor    int64
	Currency       string
}

// CreatePaymentResult is the stored payment and whether this call created
// it. Created is false for a replay of an earlier request with the same key.
type CreatePaymentResult struct {
	Payment payment.Payment
	Created bool
}

// CreatePayment accepts a payment idempotently. In one transaction it
// inserts the payment, schedules the start task and records the
// payment.created event, so either all three exist or none does.
//
// A request with a known Idempotency-Key returns the stored payment when the
// body is the same, and payment.ErrIdempotencyConflict when it differs.
func (s *Service) CreatePayment(ctx context.Context, req CreatePaymentRequest) (CreatePaymentResult, error) {
	if err := validateIdempotencyKey(req.IdempotencyKey); err != nil {
		return CreatePaymentResult{}, err
	}
	amount, err := payment.NewMoney(req.AmountMinor, req.Currency)
	if err != nil {
		return CreatePaymentResult{}, err
	}
	id, err := s.newID()
	if err != nil {
		return CreatePaymentResult{}, fmt.Errorf("generate payment id: %w", err)
	}
	now := s.clock.Now()
	p, err := payment.New(id, req.MerchantID, amount, req.IdempotencyKey, requestHash(req.MerchantID, amount), now)
	if err != nil {
		return CreatePaymentResult{}, err
	}

	var res CreatePaymentResult
	err = s.tx.Run(ctx, func(ctx context.Context) error {
		stored, inserted, err := s.payments.Insert(ctx, p)
		if err != nil {
			return fmt.Errorf("store payment: %w", err)
		}
		if !inserted {
			if stored.RequestHash != p.RequestHash {
				return payment.ErrIdempotencyConflict
			}
			res = CreatePaymentResult{Payment: stored}
			return nil
		}
		start := ports.TaskSpec{Kind: ports.TaskStartPayment, PaymentID: stored.ID, DedupKey: "start", RunAfter: now}
		if _, err := s.tasks.Enqueue(ctx, start); err != nil {
			return fmt.Errorf("enqueue start task: %w", err)
		}
		if err := s.events.Record(ctx, payment.NewEvent(payment.EventCreated, stored)); err != nil {
			return fmt.Errorf("record event: %w", err)
		}
		res = CreatePaymentResult{Payment: stored, Created: true}
		return nil
	})
	if err != nil {
		return CreatePaymentResult{}, err
	}
	if res.Created {
		s.metrics.PaymentCreated()
		s.log.InfoContext(ctx, "payment created", "payment", res.Payment)
	}
	return res, nil
}

func validateIdempotencyKey(key string) error {
	if key == "" || len(key) > MaxIdempotencyKeyLen {
		return &payment.ValidationError{
			Field:  "Idempotency-Key",
			Reason: fmt.Sprintf("header is required and must be at most %d characters", MaxIdempotencyKeyLen),
		}
	}
	for _, r := range key {
		if r > unicode.MaxASCII || !unicode.IsPrint(r) {
			return &payment.ValidationError{Field: "Idempotency-Key", Reason: "must be printable ASCII"}
		}
	}
	return nil
}

// requestHash fingerprints the business content of a create request. JSON
// encoding of a fixed struct gives an unambiguous canonical form.
func requestHash(merchantID string, amount payment.Money) string {
	canonical, _ := json.Marshal(struct {
		MerchantID  string `json:"merchant_id"`
		AmountMinor int64  `json:"amount_minor"`
		Currency    string `json:"currency"`
	}{merchantID, amount.AmountMinor, string(amount.Currency)})
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}
