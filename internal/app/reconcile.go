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
//     and, below MaxGatewayAttempts, a start task for the next attempt with
//     a new key is enqueued. At the cap the task goes dead for an operator;
//   - several: the client was charged twice. That needs a person, so the
//     task goes dead and the duplicate is counted;
//   - the lookup fails: retried with backoff, never read as "none".
//
// Abandoning is safe only if the gateway's lookup shows a payment no later
// than ReconcileInterval x ReconcileNotFoundLimit after the create call; the
// duplicate check after the next attempt catches a payment that shows up
// later.
func (s *Service) ReconcilePayment(ctx context.Context, id uuid.UUID, payload json.RawMessage) error {
	n, err := decodeAttemptPayload(payload)
	if err != nil {
		return err
	}
	a, err := s.attempts.Get(ctx, id, n)
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
		applied, err := s.finishAttempt(ctx, a, found[0].Status, found[0].Ref)
		if err != nil {
			return err
		}
		if applied {
			s.metrics.Reconciled(reconcileFound)
		}
		return nil
	default:
		s.metrics.Reconciled(reconcileDuplicate)
		return s.duplicateFound(ctx, id, a.Number, found)
	}
}

// reconcileNotFound counts a certain "not found" answer and abandons the
// attempt once the limit is reached.
func (s *Service) reconcileNotFound(ctx context.Context, a ports.Attempt) error {
	var abandoned, capped bool
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
		if cur.Number >= s.cfg.MaxGatewayAttempts {
			capped = true
			return nil
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
	switch {
	case err != nil:
		return err
	case capped:
		s.metrics.Reconciled(reconcileAbandoned)
		s.log.ErrorContext(ctx, "last gateway attempt abandoned; the payment stays pending and needs an operator",
			"payment_id", a.PaymentID, "attempt", a.Number)
		return ports.Permanent(fmt.Errorf("payment %s: %d gateway attempts abandoned, the cap", a.PaymentID, a.Number))
	case abandoned:
		s.metrics.Reconciled(reconcileAbandoned)
		s.log.WarnContext(ctx, "gateway attempt abandoned after repeated not-found answers; starting a new attempt",
			"payment_id", a.PaymentID, "attempt", a.Number)
		return nil
	}
	s.metrics.Reconciled(reconcileNotFound)
	return ports.RetryLater(s.cfg.ReconcileInterval, "gateway does not know the payment yet")
}

// CheckDuplicates runs after an attempt that followed an abandoned one has
// settled, once the gateway had time to show a payment for the abandoned
// key. It compares the charges the gateway holds for our reference (its
// payments that are neither failed nor canceled) with our own record:
//
//   - we report failed or canceled: the gateway must hold no charge; one is
//     a client charged although we said no;
//   - we report a charge (the payment has a gateway reference): the gateway
//     must hold exactly that one; another charge is a duplicate;
//   - the gateway does not show our own reference yet: its lookup lags, the
//     task retries;
//   - the payment is still pending: checked again later.
//
// Both kinds of mismatch are counted, alerted and left as a dead task with
// the gateway references in the log; the refund is an operator action.
func (s *Service) CheckDuplicates(ctx context.Context, id uuid.UUID, payload json.RawMessage) error {
	n, err := decodeAttemptPayload(payload)
	if err != nil {
		return err
	}
	p, err := s.payments.Payment(ctx, id)
	if errors.Is(err, payment.ErrNotFound) {
		return ports.Permanent(err)
	}
	if err != nil {
		return fmt.Errorf("read payment: %w", err)
	}
	if !p.Status.Final() && p.GatewayRef == "" {
		return ports.RetryLater(s.cfg.DuplicateCheckDelay, "payment "+string(p.Status)+" without a gateway reference")
	}
	found, err := s.gateway.FindByReference(ctx, id.String())
	if err != nil {
		return fmt.Errorf("look up payment at gateway: %w", err)
	}
	var charges []ports.GatewayPayment
	ours := false
	for _, f := range found {
		if f.Status == payment.StatusFailed || f.Status == payment.StatusCanceled {
			continue // a declined or canceled gateway payment charges nothing
		}
		charges = append(charges, f)
		if f.Ref == p.GatewayRef {
			ours = true
		}
	}

	reportedNoCharge := p.Status == payment.StatusFailed || p.Status == payment.StatusCanceled
	switch {
	case reportedNoCharge && len(charges) > 0:
		return s.chargeMismatch(ctx, p, n, charges)
	case reportedNoCharge:
		s.log.InfoContext(ctx, "no gateway charge for a payment we report "+string(p.Status), "payment_id", id, "attempt", n)
		return nil
	case len(charges) > 1, len(charges) == 1 && !ours:
		// Our charge and another one, or another one where ours should be.
		return s.duplicateFound(ctx, id, n, charges)
	case len(charges) == 0:
		// Our payment has a reference the lookup does not show yet: retry,
		// and go dead after the task's attempts if it never shows.
		return fmt.Errorf("gateway lookup does not show payment %s (ref %s) yet", id, p.GatewayRef)
	default:
		s.log.InfoContext(ctx, "no duplicate gateway payment", "payment_id", id, "attempt", n, "gateway_ref", p.GatewayRef)
		return nil
	}
}

// chargeMismatch records a client charged for a payment we report failed or
// canceled: counted, logged with what an operator needs, and turned into a
// dead task.
func (s *Service) chargeMismatch(ctx context.Context, p payment.Payment, attempt int, charges []ports.GatewayPayment) error {
	refs := make([]string, len(charges))
	for i, c := range charges {
		refs[i] = c.Ref + ":" + string(c.Status)
	}
	s.metrics.ChargeMismatch()
	s.log.ErrorContext(ctx, "gateway holds a charge for a payment we report "+string(p.Status)+"; refund it or correct the payment",
		"payment", p, "attempt", attempt, "gateway_charges", refs)
	return ports.Permanent(fmt.Errorf("gateway holds %d charge(s) for payment %s, which we report %s: %v",
		len(charges), p.ID, p.Status, refs))
}

// duplicateFound records a double charge: counted, logged with what an
// operator needs to refund it, and turned into a dead task.
func (s *Service) duplicateFound(ctx context.Context, id uuid.UUID, attempt int, found []ports.GatewayPayment) error {
	refs := make([]string, len(found))
	for i, f := range found {
		refs[i] = f.Ref
	}
	s.metrics.DuplicateDetected()
	s.log.ErrorContext(ctx, "gateway holds several payments for one bank payment; refund the extra ones",
		"payment_id", id, "attempt", attempt, "gateway_refs", refs, "count", len(found))
	return ports.Permanent(fmt.Errorf("gateway holds %d payments for reference %s: %v", len(found), id, refs))
}
