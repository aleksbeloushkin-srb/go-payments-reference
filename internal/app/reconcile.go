package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// Reconcile outcomes, reported to ports.Metrics.
const (
	reconcileFound     = "found"
	reconcileNotFound  = "not_found"
	reconcileAbandoned = "abandoned"
	reconcileDuplicate = "duplicate"
	reconcileError     = "error"
)

// ReconcilePayment settles an ambiguous gateway attempt by asking the
// gateway which payments it holds for our reference:
//
//   - one: the call did happen; its status is applied as in tx2 and the
//     attempt is done;
//   - none, with certainty: the call may still become visible, so the lookup
//     repeats; after ReconcileNotFoundLimit answers the attempt is abandoned
//     and a start task for the next attempt, with a new key, is enqueued;
//   - several: the client may have been charged twice. That needs a person,
//     so the task goes dead and the error is logged;
//   - the lookup fails: retried with backoff, never read as "none".
func (s *Service) ReconcilePayment(ctx context.Context, id uuid.UUID, payload json.RawMessage) error {
	var in reconcilePayload
	if err := json.Unmarshal(payload, &in); err != nil || in.Attempt < 1 {
		return ports.Permanent(fmt.Errorf("decode reconcile payload %q: %v", payload, err))
	}
	a, err := s.attempts.Get(ctx, id, in.Attempt)
	if errors.Is(err, ports.ErrAttemptNotFound) {
		return ports.Permanent(err)
	}
	if err != nil {
		return fmt.Errorf("read attempt: %w", err)
	}
	if a.State != ports.AttemptUnknown {
		return nil // settled meanwhile
	}

	found, err := s.gateway.FindByReference(ctx, id.String())
	if err != nil {
		s.metrics.Reconciled(reconcileError)
		return fmt.Errorf("look up payment at gateway: %w", err)
	}
	switch len(found) {
	case 0:
		return s.reconcileNotFound(ctx, a)
	case 1:
		if err := s.finishAttempt(ctx, a, found[0].Status, found[0].Ref); err != nil {
			return err
		}
		s.metrics.Reconciled(reconcileFound)
		return nil
	default:
		s.metrics.Reconciled(reconcileDuplicate)
		s.log.ErrorContext(ctx, "gateway holds several payments for one bank payment",
			"payment_id", id, "attempt", a.Number, "count", len(found))
		return ports.Permanent(fmt.Errorf("gateway holds %d payments for reference %s", len(found), id))
	}
}

// reconcileNotFound counts a certain "not found" answer and abandons the
// attempt once the limit is reached.
func (s *Service) reconcileNotFound(ctx context.Context, a ports.Attempt) error {
	abandoned := false
	err := s.tx.Run(ctx, func(ctx context.Context) error {
		p, err := s.payments.Lock(ctx, a.PaymentID)
		if err != nil {
			return fmt.Errorf("lock payment: %w", err)
		}
		cur, err := s.attempts.Get(ctx, a.PaymentID, a.Number)
		if err != nil {
			return err
		}
		if cur.State != ports.AttemptUnknown {
			return nil
		}
		cur.NotFound++
		cur.UpdatedAt = s.clock.Now()
		if cur.NotFound < s.cfg.ReconcileNotFoundLimit {
			return s.attempts.Update(ctx, cur, ports.AttemptUnknown)
		}
		cur.State = ports.AttemptAbandoned
		if err := s.attempts.Update(ctx, cur, ports.AttemptUnknown); err != nil {
			return err
		}
		abandoned = true
		if p.Status != payment.StatusPending {
			return nil // a gateway event moved the payment: nothing to restart
		}
		restart := ports.TaskSpec{
			Kind:      ports.TaskStartPayment,
			PaymentID: p.ID,
			DedupKey:  fmt.Sprintf("start:%d", cur.Number+1),
			RunAfter:  s.clock.Now(),
		}
		if _, err := s.tasks.Enqueue(ctx, restart); err != nil {
			return fmt.Errorf("enqueue start task: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if abandoned {
		s.metrics.Reconciled(reconcileAbandoned)
		s.log.WarnContext(ctx, "gateway attempt abandoned after repeated not-found answers; starting a new attempt",
			"payment_id", a.PaymentID, "attempt", a.Number)
		return nil
	}
	s.metrics.Reconciled(reconcileNotFound)
	return ports.RetryLater(s.cfg.ReconcileInterval, "gateway does not know the payment yet")
}
