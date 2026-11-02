//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/app"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

func attemptPayload(n int) json.RawMessage {
	b, _ := json.Marshal(map[string]int{"attempt": n})
	return b
}

func attemptStates(t *testing.T, id string) string {
	t.Helper()
	var states string
	if err := pool.QueryRow(context.Background(), `
		SELECT coalesce(string_agg(state, ',' ORDER BY attempt), '') FROM gateway_attempts WHERE payment_id = $1`,
		id).Scan(&states); err != nil {
		t.Fatal(err)
	}
	return states
}

// ambiguous starts p with an ambiguous gateway answer: attempt 1 unknown.
func ambiguous(t *testing.T, e *env) payment.Payment {
	t.Helper()
	p := createPayment(t, e, "key-1", 1500).Payment
	e.gateway.set(ports.GatewayPayment{}, ports.ErrGatewayAmbiguous)
	if err := e.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if got := attemptStates(t, p.ID.String()); got != "unknown" {
		t.Fatalf("attempts = %q, want unknown", got)
	}
	return p
}

func TestReconcileFoundOnPostgres(t *testing.T) {
	e := newEnv(t, defaultConfig())
	p := ambiguous(t, e)
	e.gateway.lookup = func() ([]ports.GatewayPayment, error) {
		return []ports.GatewayPayment{{Ref: "gw_9", Status: payment.StatusSucceeded}}, nil
	}

	if err := e.svc.ReconcilePayment(context.Background(), p.ID, attemptPayload(1)); err != nil {
		t.Fatal(err)
	}
	got, _ := e.payments.Payment(context.Background(), p.ID)
	if got.Status != payment.StatusSucceeded || got.GatewayRef != "gw_9" || attemptStates(t, p.ID.String()) != "done" {
		t.Fatalf("payment = %s %s, attempts %s", got.Status, got.GatewayRef, attemptStates(t, p.ID.String()))
	}
}

// Certain "not found" answers are counted under the row lock; the last one
// abandons the attempt and enqueues start:2, whose run uses a new key and,
// once it settles, schedules the duplicate check.
func TestReconcileAbandonOnPostgres(t *testing.T) {
	e := newEnv(t, defaultConfig())
	p := ambiguous(t, e)
	e.gateway.lookup = func() ([]ports.GatewayPayment, error) { return []ports.GatewayPayment{}, nil }

	for i := 1; i < 3; i++ {
		var later *ports.RetryLaterError
		if err := e.svc.ReconcilePayment(context.Background(), p.ID, attemptPayload(1)); !errors.As(err, &later) {
			t.Fatalf("lookup %d: err = %v, want RetryLater", i, err)
		}
	}
	if err := e.svc.ReconcilePayment(context.Background(), p.ID, attemptPayload(1)); err != nil {
		t.Fatal(err)
	}
	if got := attemptStates(t, p.ID.String()); got != "abandoned" {
		t.Fatalf("attempts = %q", got)
	}
	if n := count(t, `SELECT not_found FROM gateway_attempts WHERE payment_id = $1 AND attempt = 1`, p.ID); n != 3 {
		t.Fatalf("not_found = %d, want 3", n)
	}
	if n := count(t, `SELECT count(*) FROM tasks WHERE kind = 'start_payment' AND dedup_key = 'start:2' AND state = 'ready'`); n != 1 {
		t.Fatalf("start:2 tasks = %d, want 1", n)
	}

	e.gateway.set(ports.GatewayPayment{Ref: "gw_2", Status: payment.StatusProcessing}, nil)
	if err := e.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	var key string
	if err := pool.QueryRow(context.Background(),
		`SELECT idempotency_key FROM gateway_attempts WHERE payment_id = $1 AND attempt = 2`, p.ID).Scan(&key); err != nil {
		t.Fatal(err)
	}
	if key != p.ID.String()+":2" || attemptStates(t, p.ID.String()) != "abandoned,done" {
		t.Fatalf("attempt 2 key %q, attempts %q", key, attemptStates(t, p.ID.String()))
	}
	if n := count(t, `SELECT count(*) FROM tasks WHERE kind = 'check_duplicates' AND dedup_key = 'duplicates:2'`); n != 1 {
		t.Fatalf("duplicate checks = %d, want 1", n)
	}
}

func TestReconcileDuplicateOnPostgres(t *testing.T) {
	e := newEnv(t, defaultConfig())
	p := ambiguous(t, e)
	e.gateway.lookup = func() ([]ports.GatewayPayment, error) {
		return []ports.GatewayPayment{{Ref: "gw_1", Status: payment.StatusSucceeded}, {Ref: "gw_2", Status: payment.StatusSucceeded}}, nil
	}

	var perm *ports.PermanentError
	if err := e.svc.ReconcilePayment(context.Background(), p.ID, attemptPayload(1)); !errors.As(err, &perm) {
		t.Fatalf("err = %v, want permanent", err)
	}
	if err := e.svc.CheckDuplicates(context.Background(), p.ID, attemptPayload(1)); !errors.As(err, &perm) {
		t.Fatalf("duplicate check err = %v, want permanent", err)
	}
}

// A crash after the call leaves attempt 1 an intent; the gateway's event
// then makes the payment final. The event closes the attempt, so the
// stuck-attempt gauge goes back to zero.
func TestEventClosesAnAttemptLeftOpenByACrash(t *testing.T) {
	e := newEnv(t, defaultConfig())
	p := createPayment(t, e, "key-o", 1500).Payment
	if err := e.tx.Run(context.Background(), func(ctx context.Context) error {
		if _, err := e.payments.Lock(ctx, p.ID); err != nil {
			return err
		}
		_, err := e.attempts.Create(ctx, p.ID, time.Now().Add(-time.Hour))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	event, _ := json.Marshal(app.GatewayEvent{ID: "evt", PaymentID: p.ID, GatewayRef: "gw_1", Status: payment.StatusSucceeded})
	if err := e.svc.ApplyGatewayEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if got := attemptStates(t, p.ID.String()); got != "done" {
		t.Fatalf("attempts = %q, want done", got)
	}
	if n := count(t, `SELECT count(*) FROM gateway_attempts WHERE state IN ('intent', 'unknown')`); n != 0 {
		t.Fatalf("open attempts = %d, want 0", n)
	}
}

// Concurrent starts of one payment (duplicate deliveries, expired leases)
// and gateway events: one attempt, one key, one execution, never more than
// one open attempt, and none left open at the end.
func TestConcurrentStartsAndEventsOneAttempt(t *testing.T) {
	for round := range 20 {
		e := newEnv(t, defaultConfig())
		p := createPayment(t, e, "key-c", 1500).Payment
		e.gateway.set(ports.GatewayPayment{Ref: "gw_1", Status: payment.StatusProcessing}, nil)
		var maxOpen atomic.Int32
		e.gateway.during = func(context.Context) {
			n := int32(count(t, `SELECT count(*) FROM gateway_attempts WHERE state IN ('intent', 'unknown')`))
			for {
				cur := maxOpen.Load()
				if n <= cur || maxOpen.CompareAndSwap(cur, n) {
					break
				}
			}
		}
		var wg sync.WaitGroup
		errs := make(chan error, 12)
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := e.svc.StartPayment(context.Background(), p.ID); err != nil {
					errs <- err
				}
			}()
		}
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				event, _ := json.Marshal(app.GatewayEvent{ID: "evt", PaymentID: p.ID, GatewayRef: "gw_1", Status: payment.StatusSucceeded})
				if err := e.svc.ApplyGatewayEvent(context.Background(), event); err != nil {
					errs <- err
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Errorf("round %d: %v", round, err)
		}
		if n := e.gateway.executions(); n > 1 {
			t.Fatalf("round %d: executions = %d", round, n)
		}
		if n := count(t, `SELECT count(*) FROM gateway_attempts`); n > 1 {
			t.Fatalf("round %d: attempts = %d", round, n)
		}
		if maxOpen.Load() > 1 {
			t.Fatalf("round %d: %d open attempts seen", round, maxOpen.Load())
		}
		if n := count(t, `SELECT count(*) FROM gateway_attempts WHERE state IN ('intent', 'unknown')`); n != 0 {
			t.Fatalf("round %d: %d attempts left open", round, n)
		}
	}
}

// The partial unique index is the last line of defence: transactions that
// skip the payment's row lock still cannot open two attempts.
func TestOneOpenAttemptWithoutTheRowLock(t *testing.T) {
	e := newEnv(t, defaultConfig())
	p := createPayment(t, e, "key-u", 1500).Payment
	var wg sync.WaitGroup
	var created atomic.Int32
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := e.tx.Run(context.Background(), func(ctx context.Context) error {
				_, err := e.attempts.Create(ctx, p.ID, time.Now())
				return err
			})
			if err == nil {
				created.Add(1)
			}
		}()
	}
	wg.Wait()
	if created.Load() != 1 {
		t.Fatalf("%d concurrent creates succeeded, want 1", created.Load())
	}
}

// Switching modes on PostgreSQL: a two-phase intent left by a crash is
// finished by the in_tx service through the two-phase path, with the same
// key, so the gateway executes once.
func TestInTxDefersToTwoPhaseOnPostgres(t *testing.T) {
	e := newEnv(t, defaultConfig())
	p := createPayment(t, e, "key-m", 1500).Payment
	if err := e.tx.Run(context.Background(), func(ctx context.Context) error {
		if _, err := e.payments.Lock(ctx, p.ID); err != nil {
			return err
		}
		_, err := e.attempts.Create(ctx, p.ID, time.Now())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	cfg := defaultConfig()
	cfg.StartMode = app.StartInTx
	inTx := newEnvService(e, cfg)
	e.gateway.set(ports.GatewayPayment{Ref: "gw_1", Status: payment.StatusProcessing}, nil)

	if err := inTx.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if got := attemptStates(t, p.ID.String()); got != "done" {
		t.Fatalf("attempts = %q, want the open intent finished", got)
	}
	if n := e.gateway.executions(); n != 1 {
		t.Fatalf("executions = %d", n)
	}
}
