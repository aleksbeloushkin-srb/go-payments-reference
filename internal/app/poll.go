package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// PollPayment asks the gateway for the status of a payment that is not
// final yet.
//
// Polling is a delayed retry of the same task, not a sleeping goroutine:
// while the gateway reports a non-final status the handler returns
// ports.RetryLater and the worker reschedules the task row. Nothing waits in
// memory, so a restart loses nothing. Unlike StartPayment, the gateway call
// happens outside any transaction; only the short status update takes a lock.
func (s *Service) PollPayment(ctx context.Context, id uuid.UUID) error {
	p, err := s.payments.Payment(ctx, id)
	if errors.Is(err, payment.ErrNotFound) {
		return ports.Permanent(err)
	}
	if err != nil {
		return fmt.Errorf("read payment: %w", err)
	}
	if p.Status.Final() {
		return nil
	}
	if p.GatewayRef == "" {
		return ports.Permanent(fmt.Errorf("payment %s has no gateway reference", id))
	}

	res, err := s.gateway.Payment(ctx, p.GatewayRef)
	if err != nil {
		if errors.Is(err, ports.ErrGatewayMisconfigured) {
			s.log.ErrorContext(ctx, "gateway refused our configuration; polling is retried", "payment", p, "err", err)
		}
		return fmt.Errorf("read payment from gateway: %w", err)
	}
	if err := s.applyStatus(ctx, id, res.Status, res.Ref); err != nil {
		return err
	}
	if res.Status.Final() {
		return nil
	}
	if age := s.clock.Now().Sub(p.CreatedAt); age > s.cfg.PollTimeout {
		return ports.Permanent(fmt.Errorf("payment %s still %s after %s: reconcile with the gateway", id, res.Status, age))
	}
	return ports.RetryLater(s.cfg.PollInterval, "gateway status is "+string(res.Status))
}
