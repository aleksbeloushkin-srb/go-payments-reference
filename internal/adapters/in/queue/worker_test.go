package queue

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

type settled struct {
	how      string
	taskID   int64
	runAfter time.Time
	cause    string
}

// fakeStore hands out queued tasks once and records how they were settled.
type fakeStore struct {
	mu        sync.Mutex
	queue     []ports.Task
	settled   []settled
	settleErr error
	claims    chan struct{} // receives a value on every Claim when set
}

func (s *fakeStore) Claim(_ context.Context, _ time.Time, limit int, _ time.Duration) ([]ports.Task, error) {
	if s.claims != nil {
		s.claims <- struct{}{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := min(limit, len(s.queue))
	out := s.queue[:n]
	s.queue = s.queue[n:]
	return out, nil
}

func (s *fakeStore) record(st settled) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settled = append(s.settled, st)
	return s.settleErr
}

func (s *fakeStore) Complete(_ context.Context, t ports.Task) error {
	return s.record(settled{how: "done", taskID: t.ID})
}

func (s *fakeStore) Retry(_ context.Context, t ports.Task, runAfter time.Time, cause string) error {
	return s.record(settled{how: "retried", taskID: t.ID, runAfter: runAfter, cause: cause})
}

func (s *fakeStore) Postpone(_ context.Context, t ports.Task, runAfter time.Time) error {
	return s.record(settled{how: "postponed", taskID: t.ID, runAfter: runAfter})
}

func (s *fakeStore) Bury(_ context.Context, t ports.Task, cause string) error {
	return s.record(settled{how: "dead", taskID: t.ID, cause: cause})
}

type handlerFunc func(ctx context.Context, t ports.Task) error

func (f handlerFunc) HandleTask(ctx context.Context, t ports.Task) error { return f(ctx, t) }

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

var (
	t0      = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	testCfg = Config{
		Concurrency:  4,
		IdleInterval: time.Hour,
		Lease:        time.Minute,
		TaskTimeout:  time.Second,
		MaxAttempts:  3,
		Backoff:      Backoff{Base: time.Second, Max: time.Minute, Rand: func(int64) int64 { return 0 }},
	}
)

func newWorker(store *fakeStore, h Handler) *Worker {
	return NewWorker(store, h, fixedClock{t0}, nil, NewMetrics(prometheus.NewRegistry()), slog.New(slog.NewTextHandler(io.Discard, nil)), testCfg)
}

func task(id int64, attempts int) ports.Task {
	return ports.Task{ID: id, Kind: ports.TaskStartPayment, PaymentID: uuid.New(), Attempts: attempts}
}

// wantOutcome is the task_attempts_total outcome of each case below.
var wantOutcome = map[string]string{
	"success":                                   outcomeOK,
	"first failure backs off":                   outcomeRetry,
	"second failure backs off longer":           outcomeRetry,
	"last attempt goes dead":                    outcomeDead,
	"permanent goes dead at once":               outcomePermanent,
	"retry later postpones":                     outcomeOK,
	"retry-after hint longer than backoff wins": outcomeRetry,
	"backoff longer than hint wins":             outcomeRetry,
}

func TestWorkerSettlesEachOutcome(t *testing.T) {
	boom := errors.New("gateway down")
	tests := []struct {
		name     string
		attempts int
		err      error
		want     settled
	}{
		{name: "success", err: nil, want: settled{how: "done"}},
		{name: "first failure backs off", err: boom,
			want: settled{how: "retried", runAfter: t0.Add(500 * time.Millisecond), cause: boom.Error()}},
		{name: "second failure backs off longer", attempts: 1, err: boom,
			want: settled{how: "retried", runAfter: t0.Add(time.Second), cause: boom.Error()}},
		{name: "last attempt goes dead", attempts: 2, err: boom, want: settled{how: "dead", cause: boom.Error()}},
		{name: "permanent goes dead at once", err: ports.Permanent(boom), want: settled{how: "dead", cause: "permanent: " + boom.Error()}},
		{name: "retry later postpones", err: ports.RetryLater(5*time.Second, "processing"),
			want: settled{how: "postponed", runAfter: t0.Add(5 * time.Second)}},
		{name: "retry-after hint longer than backoff wins", err: &ports.RetryAfterError{After: time.Minute, Err: boom},
			want: settled{how: "retried", runAfter: t0.Add(time.Minute), cause: boom.Error()}},
		{name: "backoff longer than hint wins", attempts: 1, err: &ports.RetryAfterError{After: time.Millisecond, Err: boom},
			want: settled{how: "retried", runAfter: t0.Add(time.Second), cause: boom.Error()}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeStore{queue: []ports.Task{task(7, tt.attempts)}}
			w := newWorker(store, handlerFunc(func(context.Context, ports.Task) error { return tt.err }))

			n, err := w.RunOnce(context.Background())

			if err != nil || n != 1 {
				t.Fatalf("RunOnce = %d, %v", n, err)
			}
			tt.want.taskID = 7
			if len(store.settled) != 1 || store.settled[0] != tt.want {
				t.Fatalf("settled = %+v, want %+v", store.settled, tt.want)
			}
			outcome := wantOutcome[tt.name]
			if got := testutil.ToFloat64(w.metrics.attempts.WithLabelValues("start_payment", outcome)); got != 1 {
				t.Fatalf("task_attempts_total{outcome=%q} = %v, want 1", outcome, got)
			}
			if n := testutil.CollectAndCount(w.metrics.attempts); n != 1 {
				t.Fatalf("attempt series = %d, want exactly one outcome recorded", n)
			}
			if got := testutil.ToFloat64(w.metrics.claimed.WithLabelValues("start_payment")); got != 1 {
				t.Fatalf("tasks_claimed_total = %v, want 1", got)
			}
		})
	}
}

func TestWorkerRecoversHandlerPanic(t *testing.T) {
	store := &fakeStore{queue: []ports.Task{task(1, 0)}}
	w := newWorker(store, handlerFunc(func(context.Context, ports.Task) error { panic("nil map") }))

	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := store.settled[0]; got.how != "retried" || got.cause != "handler panic: nil map" {
		t.Fatalf("settled = %+v, want a retry", got)
	}
	if got := testutil.ToFloat64(w.metrics.panics); got != 1 {
		t.Fatalf("task_panics_total = %v, want 1", got)
	}
}

func TestWorkerSurvivesLostLease(t *testing.T) {
	store := &fakeStore{queue: []ports.Task{task(1, 0)}, settleErr: ports.ErrLeaseLost}
	w := newWorker(store, handlerFunc(func(context.Context, ports.Task) error { return nil }))

	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatalf("a lost lease is not a worker error: %v", err)
	}
	if got := testutil.ToFloat64(w.metrics.leaseLost); got != 1 {
		t.Fatalf("lease_lost_total = %v, want 1", got)
	}
	if n := testutil.CollectAndCount(w.metrics.attempts); n != 0 {
		t.Fatal("a discarded result must not be counted as an attempt")
	}
}

// task_lag_seconds measures how long a due task waited for its claim.
func TestWorkerObservesTaskLag(t *testing.T) {
	late := task(1, 0)
	late.RunAfter = t0.Add(-5 * time.Second)
	store := &fakeStore{queue: []ports.Task{late}}
	w := newWorker(store, handlerFunc(func(context.Context, ports.Task) error { return nil }))

	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	var m dto.Metric
	if err := w.metrics.lag.WithLabelValues("start_payment").(prometheus.Metric).Write(&m); err != nil {
		t.Fatal(err)
	}
	if h := m.GetHistogram(); h.GetSampleCount() != 1 || h.GetSampleSum() != 5 {
		t.Fatalf("lag histogram count=%d sum=%v, want one sample of 5s", h.GetSampleCount(), h.GetSampleSum())
	}
}

func TestWorkerRunsBatchConcurrently(t *testing.T) {
	store := &fakeStore{queue: []ports.Task{task(1, 0), task(2, 0), task(3, 0)}}
	started := make(chan struct{})
	release := make(chan struct{})
	w := newWorker(store, handlerFunc(func(context.Context, ports.Task) error {
		started <- struct{}{}
		<-release
		return nil
	}))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = w.RunOnce(context.Background())
	}()
	for range 3 {
		<-started // all three are running at the same time
	}
	close(release)
	<-done
	if len(store.settled) != 3 {
		t.Fatalf("settled %d tasks, want 3", len(store.settled))
	}
}

// Shutdown must not cancel a running handler: the task finishes and is
// settled before Run returns.
func TestWorkerShutdownLetsRunningTaskFinish(t *testing.T) {
	store := &fakeStore{queue: []ports.Task{task(1, 0)}}
	started := make(chan struct{})
	release := make(chan struct{})
	var handlerErr error
	w := newWorker(store, handlerFunc(func(ctx context.Context, _ ports.Task) error {
		close(started)
		<-release
		handlerErr = ctx.Err()
		return nil
	}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- w.Run(ctx) }()

	<-started
	cancel()
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
	if handlerErr != nil {
		t.Fatalf("handler ctx was cancelled: %v", handlerErr)
	}
	if len(store.settled) != 1 || store.settled[0].how != "done" {
		t.Fatalf("settled = %+v, want the task completed", store.settled)
	}
}

func TestWorkerWakesOnSignal(t *testing.T) {
	store := &fakeStore{claims: make(chan struct{}, 4)}
	ran := make(chan struct{})
	wake := make(chan struct{}, 1)
	w := NewWorker(store, handlerFunc(func(context.Context, ports.Task) error {
		close(ran)
		return nil
	}), fixedClock{t0}, wake, NewMetrics(prometheus.NewRegistry()), slog.New(slog.NewTextHandler(io.Discard, nil)), testCfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error)
	go func() { done <- w.Run(ctx) }()

	<-store.claims // the first, empty claim has happened
	store.mu.Lock()
	store.queue = append(store.queue, task(1, 0))
	store.mu.Unlock()
	wake <- struct{}{} // IdleInterval is an hour: only the signal can start it

	<-ran
	cancel()
	<-done
}

func TestBackoffDelay(t *testing.T) {
	lowest := Backoff{Base: time.Second, Max: time.Minute, Rand: func(int64) int64 { return 0 }}
	highest := Backoff{Base: time.Second, Max: time.Minute, Rand: func(n int64) int64 { return n - 1 }}
	tests := []struct {
		attempt  int
		min, max time.Duration
	}{
		{attempt: 1, min: 500 * time.Millisecond, max: time.Second},
		{attempt: 2, min: time.Second, max: 2 * time.Second},
		{attempt: 4, min: 4 * time.Second, max: 8 * time.Second},
		{attempt: 7, min: 30 * time.Second, max: time.Minute},
		{attempt: 500, min: 30 * time.Second, max: time.Minute},
		{attempt: 0, min: 500 * time.Millisecond, max: time.Second},
	}
	for _, tt := range tests {
		if got := lowest.Delay(tt.attempt); got != tt.min {
			t.Errorf("attempt %d lowest = %s, want %s", tt.attempt, got, tt.min)
		}
		if got := highest.Delay(tt.attempt); got != tt.max {
			t.Errorf("attempt %d highest = %s, want %s", tt.attempt, got, tt.max)
		}
	}
}

func TestBackoffDefaultRandStaysInRange(t *testing.T) {
	b := Backoff{Base: time.Second, Max: time.Minute}
	for range 1000 {
		if d := b.Delay(3); d < 2*time.Second || d > 4*time.Second {
			t.Fatalf("Delay(3) = %s, want within [2s, 4s]", d)
		}
	}
}

// After StopClaiming the worker takes nothing new, even when woken, and Run
// returns once its context is cancelled.
func TestWorkerStopClaiming(t *testing.T) {
	store := &fakeStore{queue: []ports.Task{task(1, 0)}}
	wake := make(chan struct{}, 1)
	w := NewWorker(store, handlerFunc(func(context.Context, ports.Task) error { return nil }),
		fixedClock{t0}, wake, NewMetrics(prometheus.NewRegistry()), slog.New(slog.NewTextHandler(io.Discard, nil)), testCfg)
	w.StopClaiming()
	w.StopClaiming() // idempotent

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- w.Run(ctx) }()
	wake <- struct{}{}
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
	if len(store.settled) != 0 || len(store.queue) != 1 {
		t.Fatalf("settled %v, queue %d: no task may be claimed after StopClaiming", store.settled, len(store.queue))
	}
}
