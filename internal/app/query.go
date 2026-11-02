package app

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
)

// Payment returns the current state of a payment.
func (s *Service) Payment(ctx context.Context, id uuid.UUID) (payment.Payment, error) {
	return s.payments.Payment(ctx, id)
}

// WaitResult is the outcome of a long poll. Modified is false when the soft
// timeout expired (or the instance is shutting down) before the version
// moved past the client's marker; Payment then holds the last state read.
type WaitResult struct {
	Payment  payment.Payment
	Modified bool
}

// WaitPayment blocks until the payment's version exceeds seenVersion, the
// soft timeout expires, or ctx is cancelled because the client went away.
//
// The order of steps matters. The waiter is registered first, so a full
// registry answers 429 without touching the database, and a change that
// lands between the read below and the wait is still caught: the batcher
// compares versions, it does not rely on being told about the change.
func (s *Service) WaitPayment(ctx context.Context, id uuid.UUID, seenVersion int64) (WaitResult, error) {
	if seenVersion < 0 {
		return WaitResult{}, &payment.ValidationError{Field: "version", Reason: "must not be negative"}
	}
	changes, cancel, err := s.watcher.Watch(id, seenVersion)
	if err != nil {
		return WaitResult{}, err
	}
	defer cancel()

	p, err := s.readForWait(ctx, id)
	if err != nil {
		return WaitResult{}, err
	}
	if p.Version > seenVersion {
		return WaitResult{Payment: p, Modified: true}, nil
	}

	// A timer, not a derived context: the two ways out must stay apart.
	// Our own timeout is a normal "not modified" answer; ctx.Done means the
	// client has gone and nobody will read the response.
	timer := time.NewTimer(s.cfg.WaitTimeout)
	defer timer.Stop()
	select {
	case _, ok := <-changes:
		if !ok {
			return WaitResult{Payment: p}, nil
		}
	case <-timer.C:
		return WaitResult{Payment: p}, nil
	case <-ctx.Done():
		return WaitResult{}, ctx.Err()
	}

	p, err = s.readForWait(ctx, id)
	if err != nil {
		return WaitResult{}, err
	}
	return WaitResult{Payment: p, Modified: true}, nil
}

func (s *Service) readForWait(ctx context.Context, id uuid.UUID) (payment.Payment, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.WaitReadTimeout)
	defer cancel()
	return s.payments.Payment(ctx, id)
}
