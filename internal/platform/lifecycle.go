package platform

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

// Readiness reports whether the instance should receive new traffic. It
// starts as not ready.
type Readiness struct {
	ready atomic.Bool
}

// Set changes the readiness state.
func (r *Readiness) Set(ready bool) { r.ready.Store(ready) }

// Ready reports the current state.
func (r *Readiness) Ready() bool { return r.ready.Load() }

// SystemClock is the real clock.
type SystemClock struct{}

// Now returns the current time.
func (SystemClock) Now() time.Time { return time.Now() }

// Group owns long-running background components (worker, relay, batcher).
//
// Its context is not derived from the signal context: on SIGTERM the HTTP
// server must drain first, and only then are background components
// stopped. A component that fails, panics or returns early cancels the
// group, which the caller sees on Done and treats as a reason to shut down.
type Group struct {
	ctx    context.Context
	cancel context.CancelFunc
	eg     *errgroup.Group
	log    *slog.Logger
}

// NewGroup returns an empty Group.
func NewGroup(log *slog.Logger) *Group {
	ctx, cancel := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(ctx)
	return &Group{ctx: ctx, cancel: cancel, eg: eg, log: log}
}

// Go starts run in its own goroutine. run must return when its ctx is
// cancelled.
func (g *Group) Go(name string, run func(ctx context.Context) error) {
	g.eg.Go(func() (err error) {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("%s: panic: %v", name, p)
			}
		}()
		g.log.Info("component started", "component", name)
		err = run(g.ctx)
		g.log.Info("component stopped", "component", name)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		if g.ctx.Err() == nil {
			return fmt.Errorf("%s: stopped unexpectedly", name)
		}
		return nil
	})
}

// Done is closed when the group is stopping, either because Stop was called
// or because a component failed.
func (g *Group) Done() <-chan struct{} { return g.ctx.Done() }

// Stop cancels every component and waits for all of them to return. It
// returns the first component error, if any.
func (g *Group) Stop() error {
	g.cancel()
	return g.eg.Wait()
}
