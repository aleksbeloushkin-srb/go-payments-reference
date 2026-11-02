package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// applyStatus moves a payment to a status reported by the gateway, in its
// own short transaction under a row lock.
func (s *Service) applyStatus(ctx context.Context, id uuid.UUID, status payment.Status, ref string) error {
	return s.tx.Run(ctx, func(ctx context.Context) error {
		p, err := s.payments.Lock(ctx, id)
		if errors.Is(err, payment.ErrNotFound) {
			return ports.Permanent(err)
		}
		if err != nil {
			return fmt.Errorf("lock payment: %w", err)
		}
		err = s.transition(ctx, &p, status, ref)
		if errors.Is(err, payment.ErrInvalidTransition) {
			return s.rejectTransition(ctx, p, status, err)
		}
		return err
	})
}

// rejectTransition decides what a refused transition means. Gateway events
// and poll results can arrive late or out of order, so a non-final status
// reported for a payment that is already final is stale and ignored. Two
// different final statuses are a real disagreement between us and the
// gateway: the task goes dead so that an operator reconciles it.
func (s *Service) rejectTransition(ctx context.Context, p payment.Payment, reported payment.Status, err error) error {
	if p.Status.Final() && reported.Final() {
		s.log.ErrorContext(ctx, "gateway reports a conflicting final status",
			"payment", p, "reported", reported)
		return ports.Permanent(err)
	}
	s.log.InfoContext(ctx, "ignoring stale gateway status", "payment", p, "reported", reported)
	return nil
}

// transition applies the state machine, persists the change with an
// optimistic version check, records the event and, for a non-final status,
// makes sure a poll task exists; all in the caller's transaction. An
// unchanged status is a no-op.
func (s *Service) transition(ctx context.Context, p *payment.Payment, next payment.Status, ref string) error {
	if ref != "" && p.GatewayRef != "" && ref != p.GatewayRef {
		// The gateway reports another payment under our reference: a bug on
		// one side, never something to apply silently.
		s.log.ErrorContext(ctx, "gateway reference mismatch", "payment", *p, "reported_ref", ref)
		return ports.Permanent(fmt.Errorf("gateway reference mismatch: stored %q, reported %q", p.GatewayRef, ref))
	}
	prev := p.Version
	changed, err := p.TransitionTo(next, s.clock.Now())
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	if ref != "" {
		p.GatewayRef = ref
	}
	if err := s.payments.Update(ctx, *p, prev); err != nil {
		return fmt.Errorf("save status change: %w", err)
	}
	if err := s.events.Record(ctx, payment.NewEvent(payment.EventStatusChanged, *p)); err != nil {
		return fmt.Errorf("record event: %w", err)
	}
	s.log.InfoContext(ctx, "payment status changed", "payment", *p)
	return s.ensurePoll(ctx, *p)
}

// ensurePoll keeps the invariant "a payment that is not final has a poll
// task". Webhooks can be lost, so polling is the safety net for every
// non-final status, however the payment got there. The dedup key makes it
// idempotent: a payment has one poll task, which reschedules itself until
// the payment is final.
func (s *Service) ensurePoll(ctx context.Context, p payment.Payment) error {
	if p.Status.Final() || p.Status == payment.StatusPending {
		return nil
	}
	poll := ports.TaskSpec{
		Kind:      ports.TaskPollPayment,
		PaymentID: p.ID,
		DedupKey:  "poll",
		RunAfter:  s.clock.Now().Add(s.cfg.PollInterval),
	}
	if _, err := s.tasks.Enqueue(ctx, poll); err != nil {
		return fmt.Errorf("enqueue poll task: %w", err)
	}
	return nil
}
