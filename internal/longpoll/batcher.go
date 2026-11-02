package longpoll

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// VersionReader returns current versions for a set of payment ids. Missing
// ids are simply absent from the map.
type VersionReader interface {
	Versions(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]int64, error)
}

// BatcherConfig tunes a Batcher.
type BatcherConfig struct {
	// Interval is the pause between two batches.
	Interval time.Duration
	// QueryTimeout bounds one batch query.
	QueryTimeout time.Duration
}

// Batcher periodically checks all awaited payments with one query.
type Batcher struct {
	registry *Registry
	versions VersionReader
	cfg      BatcherConfig
	log      *slog.Logger
}

// NewBatcher returns a Batcher.
func NewBatcher(registry *Registry, versions VersionReader, cfg BatcherConfig, log *slog.Logger) *Batcher {
	return &Batcher{registry: registry, versions: versions, cfg: cfg, log: log}
}

// Run ticks until ctx is cancelled. A failed query is logged and retried on
// the next tick; waiters simply wait a little longer.
func (b *Batcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(b.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := b.Poll(ctx); err != nil && ctx.Err() == nil {
				b.log.WarnContext(ctx, "long-poll batch failed", "err", err)
			}
		}
	}
}

// Poll runs one batch: read versions for every awaited id and wake waiters
// whose payment has moved on.
func (b *Batcher) Poll(ctx context.Context) error {
	ids := b.registry.IDs()
	if len(ids) == 0 {
		return nil
	}
	qctx, cancel := context.WithTimeout(ctx, b.cfg.QueryTimeout)
	defer cancel()
	versions, err := b.versions.Versions(qctx, ids)
	if err != nil {
		return err
	}
	for id, v := range versions {
		b.registry.Notify(id, v)
	}
	return nil
}
