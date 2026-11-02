package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// applyStatus moves a payment to a status reported by the gateway, in its
// own short transaction under a row lock.
func (s *Service) applyStatus(ctx context.Context, id uuid.UUID, status payment.Status, ref string) error {
	var change *statusChange
	err := s.tx.Run(ctx, func(ctx context.Context) error {
		p, err := s.payments.Lock(ctx, id)
		if errors.Is(err, payment.ErrNotFound) {
			return ports.Permanent(err)
		}
		if err != nil {
			return fmt.Errorf("lock payment: %w", err)
		}
		change, err = s.applyLocked(ctx, &p, status, ref)
		return err
	})
	if err == nil {
		s.report(change)
	}
	return err
}

// applyLocked applies a status reported by the gateway to a payment whose
// row the caller has locked. A stale or conflicting report is handled by
// rejectTransition instead of failing the caller.
func (s *Service) applyLocked(ctx context.Context, p *payment.Payment, status payment.Status, ref string) (*statusChange, error) {
	change, err := s.transition(ctx, p, status, ref)
	if errors.Is(err, payment.ErrInvalidTransition) {
		return nil, s.rejectTransition(ctx, *p, status, err)
	}
	return change, err
}

// statusChange is a status change made inside a transaction. It is reported
// to metrics only after that transaction has committed.
type statusChange struct {
	from, to payment.Status
	age      time.Duration
}

// report records a committed status change; nil means nothing changed.
func (s *Service) report(c *statusChange) {
	if c != nil {
		s.metrics.StatusChanged(c.from, c.to, c.age)
	}
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
// makes sure a poll task exists; all in the caller's transaction. It returns
// the change for the caller to report after the commit, or nil when the
// status did not change.
func (s *Service) transition(ctx context.Context, p *payment.Payment, next payment.Status, ref string) (*statusChange, error) {
	if ref != "" && p.GatewayRef != "" && ref != p.GatewayRef {
		// The gateway reports another payment under our reference: a bug on
		// one side, never something to apply silently.
		s.log.ErrorContext(ctx, "gateway reference mismatch", "payment", *p, "reported_ref", ref)
		return nil, ports.Permanent(fmt.Errorf("gateway reference mismatch: stored %q, reported %q", p.GatewayRef, ref))
	}
	prev, from := p.Version, p.Status
	changed, err := p.TransitionTo(next, s.clock.Now())
	if err != nil {
		return nil, err
	}
	if !changed {
		return nil, nil
	}
	if ref != "" {
		p.GatewayRef = ref
	}
	if err := s.payments.Update(ctx, *p, prev); err != nil {
		return nil, fmt.Errorf("save status change: %w", err)
	}
	if err := s.events.Record(ctx, payment.NewEvent(payment.EventStatusChanged, *p)); err != nil {
		return nil, fmt.Errorf("record event: %w", err)
	}
	s.log.InfoContext(ctx, "payment status changed", "payment", *p)
	change := &statusChange{from: from, to: p.Status, age: p.UpdatedAt.Sub(p.CreatedAt)}
	return change, s.ensurePoll(ctx, *p)
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
