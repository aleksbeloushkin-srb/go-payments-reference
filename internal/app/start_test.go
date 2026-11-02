package app

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// TestStartPayment covers the version 1 start (in_tx).
func TestStartPayment(t *testing.T) {
	tests := []struct {
		name        string
		resp        ports.GatewayPayment
		err         error
		wantStatus  payment.Status
		wantVersion int64
		wantPoll    bool
		wantEvents  int
		wantErr     bool
	}{
		{
			name:       "gateway processing schedules a poll",
			resp:       ports.GatewayPayment{Ref: "gw_9", Status: payment.StatusProcessing},
			wantStatus: payment.StatusProcessing, wantVersion: 2, wantPoll: true, wantEvents: 1,
		},
		{
			name:       "requires action schedules a poll",
			resp:       ports.GatewayPayment{Ref: "gw_9", Status: payment.StatusRequiresAction},
			wantStatus: payment.StatusRequiresAction, wantVersion: 2, wantPoll: true, wantEvents: 1,
		},
		{
			name:       "immediate success needs no poll",
			resp:       ports.GatewayPayment{Ref: "gw_9", Status: payment.StatusSucceeded},
			wantStatus: payment.StatusSucceeded, wantVersion: 2, wantEvents: 1,
		},
		{
			name:       "rejection fails the payment",
			err:        ports.ErrGatewayRejected,
			wantStatus: payment.StatusFailed, wantVersion: 2, wantEvents: 1,
		},
		{
			name:       "ambiguous outcome rolls back for a retry",
			err:        ports.ErrGatewayAmbiguous,
			wantStatus: payment.StatusPending, wantVersion: 1, wantErr: true,
		},
		{
			name:       "unavailable gateway rolls back for a retry",
			err:        ports.ErrGatewayUnavailable,
			wantStatus: payment.StatusPending, wantVersion: 1, wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(v1Config())
			p := f.seed(payment.StatusPending)
			f.gateway.createResp, f.gateway.createErr = tt.resp, tt.err

			err := f.svc.StartPayment(context.Background(), p.ID)

			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			got := f.store.payments[p.ID]
			if got.Status != tt.wantStatus || got.Version != tt.wantVersion {
				t.Fatalf("payment = %s v%d, want %s v%d", got.Status, got.Version, tt.wantStatus, tt.wantVersion)
			}
			if req := f.gateway.creates[0]; req.IdempotencyKey != p.ID.String() {
				t.Fatalf("idempotency key = %q, want the payment id", req.IdempotencyKey)
			}
			polls := f.store.tasksOf(ports.TaskPollPayment)
			if (len(polls) == 1) != tt.wantPoll {
				t.Fatalf("poll tasks = %+v, want poll %v", polls, tt.wantPoll)
			}
			if tt.wantPoll && !polls[0].RunAfter.Equal(f.clock.now.Add(testConfig.PollInterval)) {
				t.Fatalf("poll run_after = %v", polls[0].RunAfter)
			}
			if n := len(f.store.events); n != tt.wantEvents {
				t.Fatalf("events = %d, want %d", n, tt.wantEvents)
			}
		})
	}
}

func TestStartPaymentSkipsAlreadyStartedPayment(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusProcessing)

	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if len(f.gateway.creates) != 0 {
		t.Fatal("a started payment must not be sent to the gateway again")
	}
}

func TestStartPaymentUnknownIDIsPermanent(t *testing.T) {
	f := newFixture(testConfig)
	var perm *ports.PermanentError

	err := f.svc.StartPayment(context.Background(), [16]byte{1})

	if !errors.As(err, &perm) || !errors.Is(err, payment.ErrNotFound) {
		t.Fatalf("err = %v, want permanent not-found", err)
	}
}

// The sequence from the review, in the version 1 start: the first start is
// ambiguous and rolls back, a non-final gateway event arrives before the
// retry, and the retry finds the payment already started. A poll task must
// exist throughout, or a lost final webhook would leave the payment
// "processing" forever.
func TestNonFinalEventBeforeStartCommitStillPolls(t *testing.T) {
	f := newFixture(v1Config())
	p := f.seed(payment.StatusPending)
	f.gateway.createErr = ports.ErrGatewayAmbiguous
	if err := f.svc.StartPayment(context.Background(), p.ID); err == nil {
		t.Fatal("want an error for the ambiguous start")
	}

	payload := []byte(`{"id":"evt_1","payment_id":"` + p.ID.String() + `","gateway_ref":"gw_1","status":"processing"}`)
	if err := f.svc.ApplyGatewayEvent(context.Background(), payload); err != nil {
		t.Fatal(err)
	}
	if n := len(f.store.tasksOf(ports.TaskPollPayment)); n != 1 {
		t.Fatalf("poll tasks after the early event = %d, want 1", n)
	}

	f.gateway.createErr = nil
	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if len(f.gateway.creates) != 1 {
		t.Fatalf("gateway creates = %d, want only the first, ambiguous one", len(f.gateway.creates))
	}
	if got := f.store.payments[p.ID].Status; got != payment.StatusProcessing {
		t.Fatalf("status = %s", got)
	}
	if n := len(f.store.tasksOf(ports.TaskPollPayment)); n != 1 {
		t.Fatalf("poll tasks = %d, want exactly 1", n)
	}
}

// A non-final payment found by a start retry gets its poll task back.
func TestStartPaymentEnsuresPollForStartedPayment(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusRequiresAction)

	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if n := len(f.store.tasksOf(ports.TaskPollPayment)); n != 1 {
		t.Fatalf("poll tasks = %d, want 1", n)
	}
}

// A configuration error at the gateway never fails the payment.
func TestStartPaymentMisconfiguredGatewayIsRetried(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusPending)
	f.gateway.createErr = fmt.Errorf("%w: %w: status 401", ports.ErrGatewayUnavailable, ports.ErrGatewayMisconfigured)

	err := f.svc.StartPayment(context.Background(), p.ID)

	var perm *ports.PermanentError
	if err == nil || errors.As(err, &perm) {
		t.Fatalf("err = %v, want a retryable error", err)
	}
	if got := f.store.payments[p.ID].Status; got != payment.StatusPending {
		t.Fatalf("status = %s, want pending", got)
	}
}
