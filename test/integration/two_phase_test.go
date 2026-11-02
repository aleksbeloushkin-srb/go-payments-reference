//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/adapters/in/queue"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/app"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

func attemptState(t *testing.T, id string, n int) string {
	t.Helper()
	var state string
	if err := pool.QueryRow(context.Background(),
		`SELECT state FROM gateway_attempts WHERE payment_id = $1 AND attempt = $2`, id, n).Scan(&state); err != nil {
		t.Fatalf("attempt %d of %s: %v", n, id, err)
	}
	return state
}

// A webhook that lands while the gateway call is in flight is applied at
// once, because no row lock is held during the call; tx2 then finds the
// payment ahead of its own answer and only closes the attempt.
func TestTwoPhaseWebhookDuringTheCall(t *testing.T) {
	e := newEnv(t, defaultConfig())
	p := createPayment(t, e, "key-1", 1500).Payment
	e.gateway.set(ports.GatewayPayment{Ref: "gw_1", Status: payment.StatusProcessing}, nil)
	e.gateway.during = func(context.Context) {
		event, _ := json.Marshal(app.GatewayEvent{ID: "evt_1", PaymentID: p.ID, GatewayRef: "gw_1", Status: payment.StatusSucceeded})
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := e.svc.ApplyGatewayEvent(ctx, event); err != nil {
			t.Errorf("webhook during the call: %v", err)
		}
	}

	if err := e.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	got, err := e.payments.Payment(context.Background(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != payment.StatusSucceeded || got.Version != 2 {
		t.Fatalf("payment = %s v%d, want succeeded v2", got.Status, got.Version)
	}
	if state := attemptState(t, p.ID.String(), 1); state != "done" {
		t.Fatalf("attempt state = %s, want done", state)
	}
}

// Two workers race on one start task: the first is slow at the gateway, its
// lease expires, the second claims the task, reuses the attempt's key and
// finishes. The gateway executes once; the first worker's late answer
// changes nothing and its result is discarded by the lease fence.
func TestTwoPhaseExpiredLeaseTwoWorkers(t *testing.T) {
	e := newEnv(t, defaultConfig())
	p := createPayment(t, e, "key-1", 1500).Payment
	e.gateway.set(ports.GatewayPayment{Ref: "gw_1", Status: payment.StatusProcessing}, nil)

	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	e.gateway.during = func(context.Context) {
		if calls.Add(1) == 1 {
			close(started)
			<-release // the first worker is stuck at the gateway
		}
	}
	slowMetrics := queue.NewMetrics(prometheus.NewRegistry())
	slow := queue.NewWorker(e.tasks, e.svc, e.clock, nil, slowMetrics, discard(), workerConfig())
	fast := queue.NewWorker(e.tasks, e.svc, e.clock, nil, queue.NewMetrics(prometheus.NewRegistry()), discard(), workerConfig())

	done := make(chan error)
	go func() {
		_, err := slow.RunOnce(context.Background())
		done <- err
	}()
	<-started
	e.clock.Advance(2 * workerConfig().Lease)
	if n, err := fast.RunOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("second worker claimed %d, %v; want the expired task", n, err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if n := e.gateway.executions(); n != 1 || calls.Load() != 2 {
		t.Fatalf("executions = %d over %d calls, want 1 over 2: the same key both times", n, calls.Load())
	}
	got, _ := e.payments.Payment(context.Background(), p.ID)
	if got.Status != payment.StatusProcessing || got.Version != 2 {
		t.Fatalf("payment = %s v%d, want processing v2", got.Status, got.Version)
	}
	if state := attemptState(t, p.ID.String(), 1); state != "done" {
		t.Fatalf("attempt state = %s", state)
	}
	if n := count(t, `SELECT count(*) FROM gateway_attempts`); n != 1 {
		t.Fatalf("attempts = %d, want 1", n)
	}
}

// While the gateway answers, the two-phase start holds no database
// connection and no open transaction; the v1 start holds both.
func TestNoConnectionHeldDuringTheCall(t *testing.T) {
	tests := []struct {
		mode               app.StartMode
		acquired, idleInTx int
	}{
		{mode: app.StartTwoPhase, acquired: 0, idleInTx: 0},
		{mode: app.StartInTx, acquired: 1, idleInTx: 1},
	}
	for _, tt := range tests {
		t.Run(string(tt.mode), func(t *testing.T) {
			cfg := defaultConfig()
			cfg.StartMode = tt.mode
			e := newEnv(t, cfg)
			p := createPayment(t, e, "key-1", 1500).Payment
			e.gateway.set(ports.GatewayPayment{Ref: "gw_1", Status: payment.StatusProcessing}, nil)

			var acquired, idleInTx int
			e.gateway.during = func(context.Context) {
				acquired = int(pool.Stat().AcquiredConns())
				// This probe takes a connection of its own; it counts the
				// sessions sitting inside an open transaction.
				err := pool.QueryRow(context.Background(), `
					SELECT count(*) FROM pg_stat_activity
					WHERE datname = current_database() AND state = 'idle in transaction'`).Scan(&idleInTx)
				if err != nil {
					t.Error(err)
				}
			}
			if err := e.svc.StartPayment(context.Background(), p.ID); err != nil {
				t.Fatal(err)
			}
			if acquired != tt.acquired || idleInTx != tt.idleInTx {
				t.Fatalf("during the call: %d connections acquired, %d idle in transaction; want %d and %d",
					acquired, idleInTx, tt.acquired, tt.idleInTx)
			}
		})
	}
}
