package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// StartPayment sends a pending payment to the gateway, in the configured
// start mode.
//
// The two modes are safe to switch between on live data. Attempt 1 of the
// two-phase start sends the v1 key (the payment id), so a v1 call that
// executed without committing is deduplicated when two_phase retries it;
// and a payment that has gateway attempts is always finished by the
// two-phase start, even in in_tx mode, so an open attempt is never bypassed.
func (s *Service) StartPayment(ctx context.Context, id uuid.UUID) error {
	switch s.cfg.StartMode {
	case StartInTx:
		err := s.startInTx(ctx, id)
		if !errors.Is(err, errHasAttempts) {
			return err
		}
		return s.startTwoPhase(ctx, id)
	case StartTwoPhase:
		return s.startTwoPhase(ctx, id)
	default:
		return ports.Permanent(fmt.Errorf("unknown start mode %q", s.cfg.StartMode))
	}
}

// errHasAttempts makes startInTx hand a payment to the two-phase start
// because it already has gateway attempts.
var errHasAttempts = errors.New("payment has gateway attempts")

// startInTx is the version 1 start, kept for comparison
// (PAYMENTS_START_MODE=in_tx).
//
// Version 1 uses this design on purpose, as a teaching example of the
// trade-off: the gateway call happens inside the database transaction,
// while the payment row is locked. It is simple to reason about (one
// executor per payment, the result and the follow-up task commit together) but it holds a pooled connection and a row
// lock for the whole network round trip, and a crash between the gateway's
// success and our commit leaves an outcome only the gateway knows. The
// retry stays correct only because the gateway deduplicates by idempotency
// key. docs/REVIEW.md explains the costs; startTwoPhase is the v2 shape
// (record the intent, call outside the transaction, apply the result in a
// second transaction).
func (s *Service) startInTx(ctx context.Context, id uuid.UUID) error {
	var change *statusChange
	err := s.tx.Run(ctx, func(ctx context.Context) error {
		p, err := s.payments.Lock(ctx, id)
		if errors.Is(err, payment.ErrNotFound) {
			return ports.Permanent(err)
		}
		if err != nil {
			return fmt.Errorf("lock payment: %w", err)
		}
		// Decided under the row lock, which the two-phase tx1 also takes: a
		// payment the two-phase start has touched is left to it.
		if _, ok, err := s.attempts.Latest(ctx, id); err != nil {
			return err
		} else if ok {
			return errHasAttempts
		}
		if p.Status != payment.StatusPending {
			// Already started: a retry after a lost commit, or a gateway
			// event that got here first. Make sure a non-final payment is
			// still being polled.
			return s.ensurePoll(ctx, p)
		}

		res, err := s.gateway.CreatePayment(ctx, ports.GatewayRequest{
			IdempotencyKey: p.ID.String(),
			PaymentID:      p.ID,
			MerchantID:     p.MerchantID,
			Amount:         p.Amount,
		})
		if errors.Is(err, ports.ErrGatewayRejected) {
			s.log.WarnContext(ctx, "gateway rejected payment", "payment", p, "err", err)
			change, err = s.transition(ctx, &p, payment.StatusFailed, "")
			return err
		}
		if err != nil {
			// Unavailable, misconfigured or ambiguous: roll back and let the
			// task retry with backoff. The retry repeats the call with the
			// same idempotency key, so the gateway returns the payment it may
			// already have executed instead of charging the client twice.
			if errors.Is(err, ports.ErrGatewayMisconfigured) {
				s.log.ErrorContext(ctx, "gateway refused our configuration; the payment stays pending and is retried",
					"payment", p, "err", err)
			}
			return fmt.Errorf("create payment at gateway: %w", err)
		}
		// transition also schedules the poll task if the status is not final.
		change, err = s.transition(ctx, &p, res.Status, res.Ref)
		return err
	})
	if err == nil {
		s.report(change)
	}
	return err
}
