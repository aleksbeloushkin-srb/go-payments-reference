// Package outbox publishes integration events recorded in the outbox table.
//
// The Relay claims a batch of unpublished events with a lease (one UPDATE
// that commits at once), publishes them with no transaction open, and then
// marks the published ones. Delivery is at least once: a crash between
// publishing and marking publishes the event again after the lease expires,
// so consumers must deduplicate by event id and use the payment version to
// ignore stale events.
package outbox

import (
	"context"
	"expvar"
	"fmt"
	"log/slog"
	"time"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

var stats = expvar.NewMap("outbox")

// settleTimeout bounds the statements that record a batch's outcome.
const settleTimeout = 5 * time.Second

// Store claims and marks outbox events.
type Store interface {
	Claim(ctx context.Context, now time.Time, limit int, lease time.Duration) ([]ports.OutboxRecord, error)
	MarkPublished(ctx context.Context, ids []int64, at time.Time) error
	Release(ctx context.Context, ids []int64) error
}

// Publisher delivers one event to the outside world.
type Publisher interface {
	Publish(ctx context.Context, r ports.OutboxRecord) error
}

// Fanout publishes to every publisher in order and stops at the first
// error. Publishers that succeeded before the error see the event again on
// the retry, which at-least-once delivery allows.
type Fanout []Publisher

// Publish delivers r to each publisher.
func (f Fanout) Publish(ctx context.Context, r ports.OutboxRecord) error {
	for _, p := range f {
		if err := p.Publish(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

// Config tunes a Relay.
type Config struct {
	// Interval is how often an idle relay looks for new events.
	Interval time.Duration
	// Batch is the most events claimed at once.
	Batch int
	// Lease is how long claimed events stay invisible to other relays.
	Lease time.Duration
	// PublishTimeout bounds one delivery. A delivery starts only if it can
	// finish before the lease expires.
	PublishTimeout time.Duration
}

// Relay moves events from the outbox to a Publisher.
type Relay struct {
	store Store
	pub   Publisher
	clock ports.Clock
	wake  <-chan struct{}
	cfg   Config
	log   *slog.Logger
}

// NewRelay returns a Relay. wake may be nil; a value on it starts a batch
// without waiting for the next tick.
func NewRelay(store Store, pub Publisher, clock ports.Clock, wake <-chan struct{}, cfg Config, log *slog.Logger) *Relay {
	return &Relay{store: store, pub: pub, clock: clock, wake: wake, cfg: cfg, log: log}
}

// Run publishes until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	for {
		n, err := r.RunOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			r.log.WarnContext(ctx, "outbox relay batch failed", "err", err)
		}
		if err == nil && n == r.cfg.Batch {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-r.wake:
		}
	}
}

// RunOnce claims one batch, publishes it in id order and records the
// outcome. It returns the number of events published.
//
// Publishing stops at the first failure so that a later event of the same
// payment never overtakes an earlier one. Events published before the
// failure are marked; the rest are released for the next batch.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	claimedAt := r.clock.Now()
	records, err := r.store.Claim(ctx, claimedAt, r.cfg.Batch, r.cfg.Lease)
	if err != nil {
		return 0, err
	}
	leaseEnd := claimedAt.Add(r.cfg.Lease)

	var (
		published []int64
		pubErr    error
	)
	for _, rec := range records {
		// Starting a delivery that could outlive the lease would let another
		// relay claim and publish the same event at the same time.
		if r.clock.Now().Add(r.cfg.PublishTimeout).After(leaseEnd) {
			break
		}
		if err := r.publish(ctx, rec); err != nil {
			pubErr = fmt.Errorf("publish outbox event %d: %w", rec.ID, err)
			break
		}
		published = append(published, rec.ID)
	}

	// The deliveries have happened: record them even if ctx was cancelled
	// meanwhile, otherwise they would be published again.
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()
	if len(published) > 0 {
		if err := r.store.MarkPublished(sctx, published, r.clock.Now()); err != nil {
			return 0, fmt.Errorf("mark %d published events: %w", len(published), err)
		}
	}
	if rest := idsOf(records[len(published):]); len(rest) > 0 {
		if err := r.store.Release(sctx, rest); err != nil {
			// Not fatal: the events come back when their lease expires.
			r.log.WarnContext(ctx, "release outbox events failed", "err", err)
		}
	}

	stats.Add("published", int64(len(published)))
	if pubErr != nil {
		stats.Add("failed", 1)
	}
	return len(published), pubErr
}

func (r *Relay) publish(ctx context.Context, rec ports.OutboxRecord) error {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.PublishTimeout)
	defer cancel()
	return r.pub.Publish(ctx, rec)
}

func idsOf(records []ports.OutboxRecord) []int64 {
	ids := make([]int64, len(records))
	for i, r := range records {
		ids[i] = r.ID
	}
	return ids
}
