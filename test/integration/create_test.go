//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/app"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

func TestCreateIsIdempotent(t *testing.T) {
	e := newEnv(t, defaultConfig())

	first := createPayment(t, e, "key-1", 1500)
	replay := createPayment(t, e, "key-1", 1500)

	if !first.Created || replay.Created || replay.Payment.ID != first.Payment.ID {
		t.Fatalf("first = %+v, replay = %+v", first, replay)
	}
	if n := count(t, `SELECT count(*) FROM payments`); n != 1 {
		t.Fatalf("payments = %d, want 1", n)
	}
	if n := count(t, `SELECT count(*) FROM tasks WHERE kind = 'start_payment'`); n != 1 {
		t.Fatalf("start tasks = %d, want 1", n)
	}
	if n := count(t, `SELECT count(*) FROM outbox WHERE type = 'payment.created'`); n != 1 {
		t.Fatalf("created events = %d, want 1", n)
	}

	_, err := e.svc.CreatePayment(context.Background(), app.CreatePaymentRequest{
		IdempotencyKey: "key-1", MerchantID: "merchant-1", AmountMinor: 9900, Currency: "EUR",
	})
	if !errors.Is(err, payment.ErrIdempotencyConflict) {
		t.Fatalf("different body: err = %v, want ErrIdempotencyConflict", err)
	}
}

// Concurrent requests with one key race on the unique index. Exactly one
// inserts; the rest wait for it and return the same payment.
func TestConcurrentCreatesWithOneKey(t *testing.T) {
	e := newEnv(t, defaultConfig())
	const n = 16
	results := make([]app.CreatePaymentResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = e.svc.CreatePayment(context.Background(), app.CreatePaymentRequest{
				IdempotencyKey: "same-key", MerchantID: "merchant-1", AmountMinor: 1500, Currency: "EUR",
			})
		}()
	}
	wg.Wait()

	created := 0
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("request %d: %v", i, errs[i])
		}
		if results[i].Payment.ID != results[0].Payment.ID {
			t.Fatalf("request %d got payment %s, want %s", i, results[i].Payment.ID, results[0].Payment.ID)
		}
		if results[i].Created {
			created++
		}
	}
	if created != 1 || count(t, `SELECT count(*) FROM tasks`) != 1 || count(t, `SELECT count(*) FROM outbox`) != 1 {
		t.Fatalf("created = %d, want exactly one payment with one task and one event", created)
	}
}

// The status change, its outbox event and the follow-up task commit
// together. When the gateway outcome is ambiguous, none of them is written.
// This is the v1 start, whose single transaction rolls everything back.
func TestOutboxIsWrittenInTheSameTransaction(t *testing.T) {
	cfg := defaultConfig()
	cfg.StartMode = app.StartInTx
	e := newEnv(t, cfg)
	p := createPayment(t, e, "key-1", 1500).Payment

	e.gateway.set(ports.GatewayPayment{}, ports.ErrGatewayAmbiguous)
	if err := e.svc.StartPayment(context.Background(), p.ID); err == nil {
		t.Fatal("want an error for an ambiguous outcome")
	}
	if n := count(t, `SELECT count(*) FROM outbox WHERE type = 'payment.status_changed'`); n != 0 {
		t.Fatalf("status events after rollback = %d, want 0", n)
	}
	if n := count(t, `SELECT count(*) FROM tasks WHERE kind = 'poll_payment'`); n != 0 {
		t.Fatalf("poll tasks after rollback = %d, want 0", n)
	}

	e.gateway.set(ports.GatewayPayment{Ref: "gw_1", Status: payment.StatusProcessing}, nil)
	if err := e.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if n := count(t, `SELECT count(*) FROM outbox WHERE type = 'payment.status_changed' AND payload->>'status' = 'processing'`); n != 1 {
		t.Fatalf("status events = %d, want 1", n)
	}
	if n := count(t, `SELECT count(*) FROM tasks WHERE kind = 'poll_payment'`); n != 1 {
		t.Fatalf("poll tasks = %d, want 1", n)
	}
}

// A late or duplicate gateway event never moves a final payment.
func TestStateMachineDoesNotRegress(t *testing.T) {
	e := newEnv(t, defaultConfig())
	ctx := context.Background()
	p := createPayment(t, e, "key-1", 1500).Payment
	e.gateway.set(ports.GatewayPayment{Ref: "gw_1", Status: payment.StatusProcessing}, nil)
	if err := e.svc.StartPayment(ctx, p.ID); err != nil {
		t.Fatal(err)
	}

	apply := func(status payment.Status) error {
		payload := []byte(`{"id":"evt","payment_id":"` + p.ID.String() + `","gateway_ref":"gw_1","status":"` + string(status) + `"}`)
		return e.svc.ApplyGatewayEvent(ctx, payload)
	}
	if err := apply(payment.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if err := apply(payment.StatusProcessing); err != nil {
		t.Fatalf("stale event must be ignored, got %v", err)
	}
	var perm *ports.PermanentError
	if err := apply(payment.StatusFailed); !errors.As(err, &perm) {
		t.Fatalf("conflicting final status: err = %v, want permanent", err)
	}

	got, err := e.payments.Payment(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != payment.StatusSucceeded || got.Version != 3 {
		t.Fatalf("payment = %s v%d, want succeeded v3", got.Status, got.Version)
	}
}

// Update refuses a stale version even when the caller forgot the lock.
func TestUpdateChecksVersion(t *testing.T) {
	e := newEnv(t, defaultConfig())
	p := createPayment(t, e, "key-1", 1500).Payment
	p.Status, p.Version = payment.StatusProcessing, 2

	if err := e.payments.Update(context.Background(), p, 7); !errors.Is(err, payment.ErrVersionConflict) {
		t.Fatalf("err = %v, want ErrVersionConflict", err)
	}
	if _, err := e.payments.Lock(context.Background(), p.ID); err == nil {
		t.Fatal("Lock outside a transaction must fail")
	}
}
