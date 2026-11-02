package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

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
// reconciliation instead of guessing; an answer saying that the request is
// still in progress keeps the attempt and its key for a later retry.
// Whatever crashes where, the stored attempt says what to check next. The
// gateway must both deduplicate by idempotency key (a reused intent is sent
// again) and support lookup by reference (ambiguous answers are settled by
// lookup); see docs/EXTENDING.md.
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
		_, err := s.finishAttempt(ctx, attempt, res.Status, res.Ref)
		return err
	case errors.Is(err, ports.ErrGatewayRejected):
		s.log.WarnContext(ctx, "gateway rejected payment", "payment", p, "err", err)
		_, err := s.finishAttempt(ctx, attempt, payment.StatusFailed, "")
		return err
	case errors.Is(err, ports.ErrGatewayInProgress):
		// The gateway knows this key and is still working on it: the attempt
		// stays an intent and the task retries later with the same key.
		return fmt.Errorf("create payment at gateway: %w", err)
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
			// Started already, or a gateway event got here first, possibly
			// after a crash that left this payment's attempt open.
			if err := s.settleOpenAttempt(ctx, p); err != nil {
				return err
			}
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
			if attempt, err = s.createAttempt(ctx, p); err != nil {
				return err
			}
		}
		call = true
		return nil
	})
	return attempt, p, call, err
}

// createAttempt records the payment's next attempt, unless the cap on
// attempts per payment is reached.
func (s *Service) createAttempt(ctx context.Context, p payment.Payment) (ports.Attempt, error) {
	last, ok, err := s.attempts.Latest(ctx, p.ID)
	if err != nil {
		return ports.Attempt{}, err
	}
	if ok && last.Number >= s.cfg.MaxGatewayAttempts {
		s.log.ErrorContext(ctx, "gateway attempts exhausted; the payment needs an operator",
			"payment", p, "attempts", last.Number)
		return ports.Attempt{}, ports.Permanent(fmt.Errorf("payment %s: %d gateway attempts used, the cap", p.ID, last.Number))
	}
	return s.attempts.Create(ctx, p.ID, s.clock.Now())
}

// finishAttempt is tx2: apply the gateway's answer if the attempt is still
// open. It reports whether the answer was applied. A worker that lost its
// lease and finishes late finds the attempt closed and changes nothing; a
// gateway event that arrived during the call has already moved the payment
// and closed the attempt.
func (s *Service) finishAttempt(ctx context.Context, a ports.Attempt, status payment.Status, ref string) (applied bool, err error) {
	var change *statusChange
	err = s.tx.Run(ctx, func(ctx context.Context) error {
		p, err := s.payments.Lock(ctx, a.PaymentID)
		if err != nil {
			return fmt.Errorf("lock payment: %w", err)
		}
		cur, err := s.attempts.Get(ctx, a.PaymentID, a.Number)
		if err != nil {
			return err
		}
		switch cur.State {
		case ports.AttemptAbandoned:
			// The gateway executed a key we gave up on. If a later attempt
			// executed too, the client was charged twice: check at once.
			s.log.ErrorContext(ctx, "gateway executed an abandoned attempt; checking for a duplicate charge",
				"payment", p, "attempt", cur.Number, "gateway_ref", ref)
			return s.ensureDuplicateCheck(ctx, cur, 0)
		case ports.AttemptDone:
			s.log.InfoContext(ctx, "gateway attempt already settled", "payment", p, "attempt", cur.Number)
			return nil
		}
		if change, err = s.applyLocked(ctx, &p, status, ref); err != nil {
			return err
		}
		applied = true
		return s.closeAttempt(ctx, cur, ref)
	})
	if err == nil {
		s.report(change)
	}
	return applied, err
}

// closeAttempt marks an open attempt done. An attempt that follows an
// abandoned one gets a delayed duplicate check: the abandoned key may still
// have executed at the gateway.
func (s *Service) closeAttempt(ctx context.Context, a ports.Attempt, ref string) error {
	from := a.State
	a.State, a.UpdatedAt = ports.AttemptDone, s.clock.Now()
	if ref != "" {
		a.GatewayRef = ref
	}
	if err := s.attempts.Update(ctx, a, from); err != nil {
		return err
	}
	if a.Number > 1 {
		return s.ensureDuplicateCheck(ctx, a, s.cfg.DuplicateCheckDelay)
	}
	return nil
}

// settleOpenAttempt closes the open attempt of a payment that has left
// pending. With a gateway reference the payment is known to the gateway, so
// the attempt is done; without one, reconciliation finds out. Without this,
// an attempt left open by a crash after the call would stay open forever
// once a gateway event moved the payment.
func (s *Service) settleOpenAttempt(ctx context.Context, p payment.Payment) error {
	if p.Status == payment.StatusPending {
		return nil
	}
	open, ok, err := s.attempts.Open(ctx, p.ID)
	if err != nil || !ok {
		return err
	}
	if p.GatewayRef != "" {
		return s.closeAttempt(ctx, open, p.GatewayRef)
	}
	if open.State == ports.AttemptIntent {
		open.State, open.UpdatedAt = ports.AttemptUnknown, s.clock.Now()
		if err := s.attempts.Update(ctx, open, ports.AttemptIntent); err != nil {
			return err
		}
	}
	return s.ensureReconcile(ctx, open)
}

// markUnknown records an ambiguous call and schedules reconciliation. The
// payment stays pending: nothing promises progress that may not exist.
func (s *Service) markUnknown(ctx context.Context, a ports.Attempt, cause error) error {
	marked := false
	err := s.tx.Run(ctx, func(ctx context.Context) error {
		if _, err := s.payments.Lock(ctx, a.PaymentID); err != nil {
			return fmt.Errorf("lock payment: %w", err)
		}
		cur, err := s.attempts.Get(ctx, a.PaymentID, a.Number)
		if err != nil {
			return err
		}
		if cur.State != ports.AttemptIntent {
			return nil // settled meanwhile by another worker or a gateway event
		}
		cur.State, cur.UpdatedAt = ports.AttemptUnknown, s.clock.Now()
		if err := s.attempts.Update(ctx, cur, ports.AttemptIntent); err != nil {
			return err
		}
		marked = true
		return s.ensureReconcile(ctx, cur)
	})
	if err == nil && marked {
		s.log.WarnContext(ctx, "gateway outcome unknown; reconciling", "payment_id", a.PaymentID,
			"attempt", a.Number, "err", cause)
	}
	return err
}

// attemptPayload is the payload of reconcile_payment and check_duplicates
// tasks.
type attemptPayload struct {
	Attempt int `json:"attempt"`
}

func decodeAttemptPayload(payload json.RawMessage) (int, error) {
	var in attemptPayload
	if err := json.Unmarshal(payload, &in); err != nil {
		return 0, ports.Permanent(fmt.Errorf("decode attempt payload: %w", err))
	}
	if in.Attempt < 1 {
		return 0, ports.Permanent(fmt.Errorf("attempt payload %s: attempt must be at least 1", payload))
	}
	return in.Attempt, nil
}

// ensureReconcile schedules the reconcile task of an attempt; the dedup key
// makes it idempotent.
func (s *Service) ensureReconcile(ctx context.Context, a ports.Attempt) error {
	return s.enqueueForAttempt(ctx, ports.TaskReconcilePayment, "reconcile", a, s.cfg.ReconcileInterval)
}

// ensureDuplicateCheck schedules the duplicate check of an attempt after
// delay; the dedup key makes it idempotent.
func (s *Service) ensureDuplicateCheck(ctx context.Context, a ports.Attempt, delay time.Duration) error {
	return s.enqueueForAttempt(ctx, ports.TaskCheckDuplicates, "duplicates", a, delay)
}

func (s *Service) enqueueForAttempt(ctx context.Context, kind ports.TaskKind, prefix string, a ports.Attempt, delay time.Duration) error {
	payload, err := json.Marshal(attemptPayload{Attempt: a.Number})
	if err != nil {
		return fmt.Errorf("encode %s payload: %w", kind, err)
	}
	task := ports.TaskSpec{
		Kind:      kind,
		PaymentID: a.PaymentID,
		DedupKey:  fmt.Sprintf("%s:%d", prefix, a.Number),
		Payload:   payload,
		RunAfter:  s.clock.Now().Add(delay),
	}
	if _, err := s.tasks.Enqueue(ctx, task); err != nil {
		return fmt.Errorf("enqueue %s task: %w", kind, err)
	}
	return nil
}
