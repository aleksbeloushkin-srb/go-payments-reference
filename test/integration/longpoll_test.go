//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/adapters/in/httpapi"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/adapters/out/metrics"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/app"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/longpoll"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// startAPI serves the real handler over the service and runs the batcher.
func startAPI(t *testing.T, e *env) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(httpapi.NewHandler(httpapi.Deps{
		Creator: e.svc, Reader: e.svc, Events: e.svc,
		WebhookSecret: []byte("0123456789abcdef"),
		Static:        fstest.MapFS{},
		Clock:         e.clock,
		Metrics:       httpapi.NewMetrics(prometheus.NewRegistry()),
		Logger:        discard(),
	}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	b := longpoll.NewBatcher(e.waiters, e.payments, longpoll.NewMetrics(prometheus.NewRegistry()), longpoll.BatcherConfig{Interval: 20 * time.Millisecond, QueryTimeout: time.Second}, discard())
	go func() { done <- b.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
		srv.Close()
	})
	return srv
}

type waitAnswer struct {
	Status   string `json:"status"`
	Version  int64  `json:"version"`
	Modified bool   `json:"modified"`
}

// longPoll performs one GET /wait. It reports errors instead of failing the
// test, because it also runs in goroutines, where t.Fatal must not be called.
func longPoll(srv *httptest.Server, id fmt.Stringer, version int64) (*http.Response, waitAnswer, error) {
	resp, err := http.Get(fmt.Sprintf("%s/v1/payments/%s/wait?version=%d", srv.URL, id, version))
	if err != nil {
		return nil, waitAnswer{}, err
	}
	defer resp.Body.Close()
	var a waitAnswer
	if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
		return resp, waitAnswer{}, err
	}
	return resp, a, nil
}

func wait(t *testing.T, srv *httptest.Server, id fmt.Stringer, version int64) (*http.Response, waitAnswer) {
	t.Helper()
	resp, a, err := longPoll(srv, id, version)
	if err != nil && resp == nil {
		t.Fatal(err)
	}
	return resp, a
}

type pollResult struct {
	answer waitAnswer
	err    error
}

// A waiter is woken by the batcher's single query once the version moves.
// Whether the request registers before or after the change, the answer is
// the same: modified, with the new version.
func TestLongPollWakesOnChange(t *testing.T) {
	e := newEnv(t, app.Config{PollInterval: time.Second, PollTimeout: time.Hour, WaitTimeout: 10 * time.Second, WaitReadTimeout: 2 * time.Second})
	srv := startAPI(t, e)
	p := createPayment(t, e, "key-1", 1500).Payment

	answer := make(chan pollResult)
	go func() {
		_, a, err := longPoll(srv, p.ID, p.Version)
		answer <- pollResult{a, err}
	}()
	e.gateway.set(ports.GatewayPayment{Ref: "gw_1", Status: payment.StatusProcessing}, nil)
	if err := e.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}

	r := <-answer
	if r.err != nil {
		t.Fatal(r.err)
	}
	if a := r.answer; !a.Modified || a.Status != "processing" || a.Version != 2 {
		t.Fatalf("answer = %+v, want modified processing v2", a)
	}
}

func TestLongPollSoftTimeout(t *testing.T) {
	e := newEnv(t, app.Config{PollInterval: time.Second, PollTimeout: time.Hour, WaitTimeout: 150 * time.Millisecond, WaitReadTimeout: 2 * time.Second})
	srv := startAPI(t, e)
	p := createPayment(t, e, "key-1", 1500).Payment

	resp, a := wait(t, srv, p.ID, p.Version)

	if resp.StatusCode != http.StatusOK || a.Modified || a.Status != "pending" || a.Version != 1 {
		t.Fatalf("answer = %d %+v, want 200 not modified", resp.StatusCode, a)
	}
}

func TestLongPollRegistryFull(t *testing.T) {
	e := newEnv(t, app.Config{})
	e.waiters = longpoll.NewRegistry(1, longpoll.NewMetrics(prometheus.NewRegistry()))
	e.svc = app.NewService(app.Deps{
		Tx: e.tx, Payments: e.payments, Tasks: e.tasks, Events: e.outbox, Gateway: e.gateway,
		Watcher: e.waiters, Clock: e.clock, Metrics: metrics.NewBusiness(prometheus.NewRegistry()), NewID: newV7, Logger: discard(),
	}, app.Config{PollInterval: time.Second, PollTimeout: time.Hour, WaitTimeout: 10 * time.Second, WaitReadTimeout: 2 * time.Second})
	srv := startAPI(t, e)
	p := createPayment(t, e, "key-1", 1500).Payment

	_, cancel, err := e.waiters.Watch(p.ID, 99) // occupy the only slot
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	resp, _ := wait(t, srv, p.ID, p.Version)
	if resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("status = %d, Retry-After %q; want 429 with Retry-After", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
}
