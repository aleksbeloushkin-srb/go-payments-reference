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

// startTwoPhase is the version 2 start (PAYMENTS_START_MODE=two_phase,
// docs/proposals/0001-gateway-call-outside-transaction.md):
//
//  1. tx1 locks the payment, records the attempt as an intent (or reuses the
//     open intent and its idempotency key) and commits;
//  2. the gateway call runs with no transaction, connection or row lock
//     held. Its deadline comes from the task context, which the worker keeps
//     shorter than the task lease;
//  3. tx2 locks the payment again and, if the attempt is still open, applies
//     the answer and closes the attempt.
//
// An ambiguous answer marks the attempt unknown and hands it to
// reconciliation instead of guessing. Whatever crashes where, the stored
// attempt says what to check next, so a client is never charged twice as
// long as the gateway deduplicates by key or can be searched by reference.
func (s *Service) startTwoPhase(ctx context.Context, id uuid.UUID) error {
	attempt, p, call, err := s.beginAttempt(ctx, id)
	if err != nil || !call {
		return err
	}

	res, err := s.gateway.CreatePayment(ctx, ports.GatewayRequest{
		IdempotencyKey: attempt.IdempotencyKey,
		PaymentID:      p.ID,
		MerchantID:     p.MerchantID,
		Amount:         p.Amount,
	})
	switch {
	case err == nil:
		return s.finishAttempt(ctx, attempt, res.Status, res.Ref)
	case errors.Is(err, ports.ErrGatewayRejected):
		s.log.WarnContext(ctx, "gateway rejected payment", "payment", p, "err", err)
		return s.finishAttempt(ctx, attempt, payment.StatusFailed, "")
	case errors.Is(err, ports.ErrGatewayAmbiguous):
		return s.markUnknown(ctx, attempt, err)
	default:
		// Not executed (unavailable, misconfigured): the attempt stays an
		// intent and the task retries with backoff, reusing its key.
		if errors.Is(err, ports.ErrGatewayMisconfigured) {
			s.log.ErrorContext(ctx, "gateway refused our configuration; the payment stays pending and is retried",
				"payment", p, "err", err)
		}
		return fmt.Errorf("create payment at gateway: %w", err)
	}
}

// beginAttempt is tx1. It reports call == false when no gateway call is
// needed: the payment has already left pending, or an ambiguous attempt is
// being reconciled.
func (s *Service) beginAttempt(ctx context.Context, id uuid.UUID) (attempt ports.Attempt, p payment.Payment, call bool, err error) {
	err = s.tx.Run(ctx, func(ctx context.Context) error {
		var err error
		p, err = s.payments.Lock(ctx, id)
		if errors.Is(err, payment.ErrNotFound) {
			return ports.Permanent(err)
		}
		if err != nil {
			return fmt.Errorf("lock payment: %w", err)
		}
		if p.Status != payment.StatusPending {
			// Started already, or a gateway event got here first.
			return s.ensurePoll(ctx, p)
		}
		open, ok, err := s.attempts.Open(ctx, id)
		if err != nil {
			return err
		}
		switch {
		case ok && open.State == ports.AttemptUnknown:
			// Reconciliation owns this attempt; calling again could only
			// guess.
			return s.ensureReconcile(ctx, open)
		case ok:
			// An intent from a run that did not finish: reuse it and its
			// idempotency key, so a call that did happen is not repeated
			// under a new key.
			attempt = open
		default:
			attempt, err = s.attempts.Create(ctx, id, s.clock.Now())
			if err != nil {
				return err
			}
		}
		call = true
		return nil
	})
	return attempt, p, call, err
}

// finishAttempt is tx2: apply the gateway's answer if the attempt is still
// open. A worker that lost its lease and finishes late finds the attempt
// closed and changes nothing; a gateway event that arrived during the call
// has already moved the payment, and a stale answer does not move it back.
func (s *Service) finishAttempt(ctx context.Context, a ports.Attempt, status payment.Status, ref string) error {
	var change *statusChange
	err := s.tx.Run(ctx, func(ctx context.Context) error {
		p, err := s.payments.Lock(ctx, a.PaymentID)
		if err != nil {
			return fmt.Errorf("lock payment: %w", err)
		}
		cur, err := s.attempts.Get(ctx, a.PaymentID, a.Number)
		if err != nil {
			return err
		}
		if !cur.State.Open() {
			s.log.InfoContext(ctx, "gateway attempt already settled", "payment", p, "attempt", cur.Number, "state", cur.State)
			return nil
		}
		if change, err = s.applyLocked(ctx, &p, status, ref); err != nil {
			return err
		}
		from := cur.State
		cur.State, cur.UpdatedAt = ports.AttemptDone, s.clock.Now()
		if ref != "" {
			cur.GatewayRef = ref
		}
		return s.attempts.Update(ctx, cur, from)
	})
	if err == nil {
		s.report(change)
	}
	return err
}

// markUnknown records an ambiguous call and schedules reconciliation. The
// payment stays pending: nothing promises progress that may not exist.
func (s *Service) markUnknown(ctx context.Context, a ports.Attempt, cause error) error {
	err := s.tx.Run(ctx, func(ctx context.Context) error {
		if _, err := s.payments.Lock(ctx, a.PaymentID); err != nil {
			return fmt.Errorf("lock payment: %w", err)
		}
		cur, err := s.attempts.Get(ctx, a.PaymentID, a.Number)
		if err != nil {
			return err
		}
		if cur.State == ports.AttemptIntent {
			cur.State, cur.UpdatedAt = ports.AttemptUnknown, s.clock.Now()
			if err := s.attempts.Update(ctx, cur, ports.AttemptIntent); err != nil {
				return err
			}
		}
		if !cur.State.Open() {
			return nil
		}
		return s.ensureReconcile(ctx, cur)
	})
	if err == nil {
		s.log.WarnContext(ctx, "gateway outcome unknown; reconciling", "payment_id", a.PaymentID,
			"attempt", a.Number, "err", cause)
	}
	return err
}

// reconcilePayload is the payload of a reconcile_payment task.
type reconcilePayload struct {
	Attempt int `json:"attempt"`
}

// ensureReconcile schedules the reconcile task of an attempt; the dedup key
// makes it idempotent.
func (s *Service) ensureReconcile(ctx context.Context, a ports.Attempt) error {
	payload, err := json.Marshal(reconcilePayload{Attempt: a.Number})
	if err != nil {
		return fmt.Errorf("encode reconcile payload: %w", err)
	}
	task := ports.TaskSpec{
		Kind:      ports.TaskReconcilePayment,
		PaymentID: a.PaymentID,
		DedupKey:  fmt.Sprintf("reconcile:%d", a.Number),
		Payload:   payload,
		RunAfter:  s.clock.Now().Add(s.cfg.ReconcileInterval),
	}
	if _, err := s.tasks.Enqueue(ctx, task); err != nil {
		return fmt.Errorf("enqueue reconcile task: %w", err)
	}
	return nil
}
