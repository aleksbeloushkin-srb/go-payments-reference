package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

func TestPollPaymentNotFinalAsksForRetryLater(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusProcessing)
	f.gateway.readResp = ports.GatewayPayment{Ref: "gw_1", Status: payment.StatusRequiresAction}

	err := f.svc.PollPayment(context.Background(), p.ID)

	var later *ports.RetryLaterError
	if !errors.As(err, &later) || later.After != testConfig.PollInterval {
		t.Fatalf("err = %v, want RetryLater(%s)", err, testConfig.PollInterval)
	}
	if got := f.store.payments[p.ID].Status; got != payment.StatusRequiresAction {
		t.Fatalf("status = %s, want the non-final status seen by polling", got)
	}
}

func TestPollPaymentAppliesFinalStatus(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusProcessing)
	f.gateway.readResp = ports.GatewayPayment{Ref: "gw_1", Status: payment.StatusSucceeded}

	if err := f.svc.PollPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	got := f.store.payments[p.ID]
	if got.Status != payment.StatusSucceeded || got.Version != p.Version+1 {
		t.Fatalf("payment = %s v%d", got.Status, got.Version)
	}
}

func TestPollPaymentGivesUpAfterTimeout(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusProcessing)
	f.gateway.readResp = ports.GatewayPayment{Ref: "gw_1", Status: payment.StatusProcessing}
	f.clock.now = f.clock.now.Add(testConfig.PollTimeout + time.Second)

	err := f.svc.PollPayment(context.Background(), p.ID)

	var perm *ports.PermanentError
	if !errors.As(err, &perm) {
		t.Fatalf("err = %v, want a permanent error for reconciliation", err)
	}
}

func TestPollPaymentFinalPaymentDoesNotCallGateway(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusFailed)

	if err := f.svc.PollPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if f.gateway.reads != 0 {
		t.Fatal("gateway must not be polled for a final payment")
	}
}

func TestPollPaymentGatewayErrorIsRetryable(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusProcessing)
	f.gateway.readErr = ports.ErrGatewayUnavailable

	err := f.svc.PollPayment(context.Background(), p.ID)

	var perm *ports.PermanentError
	var later *ports.RetryLaterError
	if err == nil || errors.As(err, &perm) || errors.As(err, &later) {
		t.Fatalf("err = %v, want a plain retryable error", err)
	}
}
