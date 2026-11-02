package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// settleTimeout bounds the statement that records a task's outcome.
const settleTimeout = 5 * time.Second

// Claimer leases due tasks.
type Claimer interface {
	Claim(ctx context.Context, now time.Time, limit int, lease time.Duration) ([]ports.Task, error)
}

// Settler records the outcome of a claimed task. Every method returns
// ports.ErrLeaseLost if the lease has passed to another worker.
type Settler interface {
	Complete(ctx context.Context, t ports.Task) error
	Retry(ctx context.Context, t ports.Task, runAfter time.Time, cause string) error
	Postpone(ctx context.Context, t ports.Task, runAfter time.Time) error
	Bury(ctx context.Context, t ports.Task, cause string) error
}

// Store is what the Worker needs from the task table.
type Store interface {
	Claimer
	Settler
}

// Handler executes one task.
type Handler interface {
	HandleTask(ctx context.Context, t ports.Task) error
}

// Config tunes a Worker.
type Config struct {
	// Concurrency is the batch size and the number of tasks run at once.
	Concurrency int
	// IdleInterval is how often an idle worker looks for due tasks.
	IdleInterval time.Duration
	// Lease is how long a claimed task stays invisible to other workers.
	Lease time.Duration
	// TaskTimeout bounds one handler run. It must be shorter than Lease, so
	// a live worker settles its task before the lease can pass to another.
	TaskTimeout time.Duration
	// MaxAttempts is the number of failed runs after which a task is dead.
	MaxAttempts int
	Backoff     Backoff
}

// Worker claims and runs tasks.
type Worker struct {
	store     Store
	handler   Handler
	clock     ports.Clock
	wake      <-chan struct{}
	metrics   *Metrics
	log       *slog.Logger
	cfg       Config
	stopClaim chan struct{}
	stopOnce  sync.Once
}

// NewWorker returns a Worker. wake may be nil; when set, a value on it
// starts a claim immediately instead of at the next idle tick.
func NewWorker(store Store, handler Handler, clock ports.Clock, wake <-chan struct{}, m *Metrics, log *slog.Logger, cfg Config) *Worker {
	return &Worker{
		store:     store,
		handler:   handler,
		clock:     clock,
		wake:      wake,
		metrics:   m,
		log:       log,
		cfg:       cfg,
		stopClaim: make(chan struct{}),
	}
}

// StopClaiming makes Run take no new tasks. Tasks already running finish;
// Run itself returns when its context is cancelled. Shutdown calls it first,
// so the worker starts no new gateway calls while HTTP drains.
func (w *Worker) StopClaiming() {
	w.stopOnce.Do(func() { close(w.stopClaim) })
}

// Run claims and processes tasks until ctx is cancelled or StopClaiming is
// called. Tasks already running are allowed to finish (see process), so Run
// returns only after the current batch is settled.
func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.cfg.IdleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopClaim:
			<-ctx.Done()
			return nil
		default:
		}
		n, err := w.RunOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			w.log.ErrorContext(ctx, "claim tasks failed", "err", err)
		}
		if err == nil && n == w.cfg.Concurrency {
			continue // a full batch: more work is probably waiting
		}
		select {
		case <-ctx.Done():
			return nil
		case <-w.stopClaim:
		case <-ticker.C:
		case <-w.wake:
		}
	}
}

// RunOnce claims one batch, runs it and waits until every task is settled.
// It returns the number of tasks claimed.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	now := w.clock.Now()
	tasks, err := w.store.Claim(ctx, now, w.cfg.Concurrency, w.cfg.Lease)
	if err != nil {
		return 0, err
	}
	var wg sync.WaitGroup
	for _, t := range tasks {
		kind := string(t.Kind)
		w.metrics.claimed.WithLabelValues(kind).Inc()
		w.metrics.lag.WithLabelValues(kind).Observe(max(now.Sub(t.RunAfter), 0).Seconds())
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.process(ctx, t)
		}()
	}
	wg.Wait()
	return len(tasks), nil
}

// process runs one task and records the outcome.
//
// The handler gets a context detached from the worker's cancellation and
// bounded by TaskTimeout. On shutdown we stop claiming but let a running
// task finish: cancelling it mid-way through a gateway call would turn a
// clean result into an ambiguous one.
func (w *Worker) process(ctx context.Context, t ports.Task) {
	hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.TaskTimeout)
	defer cancel()
	err := w.handle(hctx, t)
	w.settle(ctx, t, err)
}

func (w *Worker) handle(ctx context.Context, t ports.Task) (err error) {
	defer func() {
		if p := recover(); p != nil {
			w.metrics.panics.Inc()
			w.log.ErrorContext(ctx, "task handler panicked",
				"task_id", t.ID, "kind", t.Kind, "panic", p, "stack", string(debug.Stack()))
			err = fmt.Errorf("handler panic: %v", p)
		}
	}()
	return w.handler.HandleTask(ctx, t)
}

func (w *Worker) settle(ctx context.Context, t ports.Task, handleErr error) {
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()
	now := w.clock.Now()
	attrs := []any{"task_id", t.ID, "kind", t.Kind, "payment_id", t.PaymentID, "attempt", t.Attempts + 1}

	var (
		later   *ports.RetryLaterError
		perm    *ports.PermanentError
		err     error
		outcome string
	)
	switch {
	case handleErr == nil:
		err, outcome = w.store.Complete(sctx, t), outcomeOK
	case errors.As(handleErr, &later):
		// The run worked; the task simply runs again later (polling).
		err, outcome = w.store.Postpone(sctx, t, now.Add(later.After)), outcomeOK
	case errors.As(handleErr, &perm), t.Attempts+1 >= w.cfg.MaxAttempts:
		outcome = outcomeDead
		if perm != nil {
			outcome = outcomePermanent
		}
		err = w.store.Bury(sctx, t, handleErr.Error())
		if err == nil {
			w.log.ErrorContext(ctx, "task moved to dead state", append(attrs, "err", handleErr)...)
		}
	default:
		delay := w.cfg.Backoff.Delay(t.Attempts + 1)
		var hint *ports.RetryAfterError
		if errors.As(handleErr, &hint) {
			// The downstream system asked us to wait; never come back sooner.
			delay = max(delay, hint.After)
		}
		err, outcome = w.store.Retry(sctx, t, now.Add(delay), handleErr.Error()), outcomeRetry
		if err == nil {
			w.log.WarnContext(ctx, "task failed, will retry", append(attrs, "err", handleErr, "retry_in", delay)...)
		}
	}

	// An outcome is counted only once it is recorded: a lost lease or a
	// failed settle means the task will run again and be counted then.
	switch {
	case errors.Is(err, ports.ErrLeaseLost):
		w.metrics.leaseLost.Inc()
		w.log.WarnContext(ctx, "task lease lost; result discarded", attrs...)
	case err != nil:
		w.log.ErrorContext(ctx, "settle task failed; it runs again when the lease expires", append(attrs, "err", err)...)
	default:
		w.metrics.attempts.WithLabelValues(string(t.Kind), outcome).Inc()
	}
}
