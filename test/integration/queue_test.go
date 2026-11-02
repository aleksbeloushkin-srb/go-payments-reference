//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/adapters/in/queue"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

type handlerFunc func(ctx context.Context, t ports.Task) error

func (f handlerFunc) HandleTask(ctx context.Context, t ports.Task) error { return f(ctx, t) }

func workerConfig() queue.Config {
	return queue.Config{
		Concurrency:  5,
		IdleInterval: time.Hour,
		Lease:        time.Minute,
		TaskTimeout:  10 * time.Second,
		MaxAttempts:  3,
		Backoff:      queue.Backoff{Base: time.Second, Max: 10 * time.Second},
	}
}

func enqueue(t *testing.T, e *env, n int) {
	t.Helper()
	for range n {
		_, err := e.tasks.Enqueue(context.Background(), ports.TaskSpec{
			Kind: ports.TaskStartPayment, PaymentID: uuid.New(), DedupKey: "start", RunAfter: e.clock.Now(),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Eight workers drain the same table concurrently. SKIP LOCKED and the lease
// guarantee that each task is handled exactly once.
func TestConcurrentWorkersNeverDoubleProcess(t *testing.T) {
	e := newEnv(t, defaultConfig())
	const tasks, workers = 200, 8
	enqueue(t, e, tasks)

	var mu sync.Mutex
	seen := map[int64]int{}
	h := handlerFunc(func(_ context.Context, task ports.Task) error {
		mu.Lock()
		seen[task.ID]++
		mu.Unlock()
		return nil
	})

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := queue.NewWorker(e.tasks, h, e.clock, nil, queue.NewMetrics(prometheus.NewRegistry()), discard(), workerConfig())
			for {
				n, err := w.RunOnce(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	if len(seen) != tasks {
		t.Fatalf("handled %d distinct tasks, want %d", len(seen), tasks)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("task %d handled %d times", id, n)
		}
	}
	if n := count(t, `SELECT count(*) FROM tasks WHERE state = 'done'`); n != tasks {
		t.Fatalf("done = %d, want %d", n, tasks)
	}
}

// A worker that dies keeps its lease until it expires; then another worker
// re-claims the task, and the first worker can no longer settle it.
func TestLeaseExpiryAllowsReclaim(t *testing.T) {
	e := newEnv(t, defaultConfig())
	ctx := context.Background()
	enqueue(t, e, 1)
	t0 := e.clock.Now()

	first, err := e.tasks.Claim(ctx, t0, 10, 45*time.Second)
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim = %v, %v", first, err)
	}
	if again, _ := e.tasks.Claim(ctx, t0.Add(10*time.Second), 10, 45*time.Second); len(again) != 0 {
		t.Fatalf("claimed %d tasks while the lease is held", len(again))
	}
	second, err := e.tasks.Claim(ctx, t0.Add(46*time.Second), 10, 45*time.Second)
	if err != nil || len(second) != 1 || second[0].ID != first[0].ID {
		t.Fatalf("re-claim after expiry = %v, %v", second, err)
	}

	if err := e.tasks.Complete(ctx, first[0]); !errors.Is(err, ports.ErrLeaseLost) {
		t.Fatalf("stale owner Complete = %v, want ErrLeaseLost", err)
	}
	if err := e.tasks.Complete(ctx, second[0]); err != nil {
		t.Fatalf("current owner Complete = %v", err)
	}
}

// A task that keeps failing is retried with backoff and moves to the dead
// state after MaxAttempts.
func TestTaskGoesDeadAfterMaxAttempts(t *testing.T) {
	e := newEnv(t, defaultConfig())
	enqueue(t, e, 1)
	var runs atomic.Int32
	w := queue.NewWorker(e.tasks, handlerFunc(func(context.Context, ports.Task) error {
		runs.Add(1)
		return errors.New("gateway down")
	}), e.clock, nil, queue.NewMetrics(prometheus.NewRegistry()), discard(), workerConfig())

	for range workerConfig().MaxAttempts {
		if n, err := w.RunOnce(context.Background()); err != nil || n != 1 {
			t.Fatalf("RunOnce = %d, %v", n, err)
		}
		if n, _ := w.RunOnce(context.Background()); n != 0 {
			t.Fatal("a failed task must wait for its backoff")
		}
		e.clock.Advance(time.Minute) // past the longest backoff
	}
	if n, _ := w.RunOnce(context.Background()); n != 0 {
		t.Fatal("a dead task must never be claimed again")
	}

	var state, lastError string
	var attempts int
	err := pool.QueryRow(context.Background(), `SELECT state, attempts, last_error FROM tasks`).Scan(&state, &attempts, &lastError)
	if err != nil {
		t.Fatal(err)
	}
	if state != "dead" || attempts != 3 || lastError != "gateway down" || runs.Load() != 3 {
		t.Fatalf("state=%s attempts=%d last_error=%q runs=%d", state, attempts, lastError, runs.Load())
	}
}

// Polling postpones the same task without spending attempts.
func TestRetryLaterKeepsAttempts(t *testing.T) {
	e := newEnv(t, defaultConfig())
	enqueue(t, e, 1)
	w := queue.NewWorker(e.tasks, handlerFunc(func(context.Context, ports.Task) error {
		return ports.RetryLater(5*time.Second, "processing")
	}), e.clock, nil, queue.NewMetrics(prometheus.NewRegistry()), discard(), workerConfig())

	for range 5 {
		if n, err := w.RunOnce(context.Background()); err != nil || n != 1 {
			t.Fatalf("RunOnce = %d, %v", n, err)
		}
		e.clock.Advance(5 * time.Second)
	}
	if n := count(t, `SELECT attempts FROM tasks`); n != 0 {
		t.Fatalf("attempts = %d, want 0", n)
	}
}
