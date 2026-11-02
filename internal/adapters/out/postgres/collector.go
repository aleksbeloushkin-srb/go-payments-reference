package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
	"github.com/aleksbeloushkin-srb/go-payments-reference/pkg/txctx"
)

// QueueCollector exports gauges about the task queue and the outbox. It runs
// one SQL query per scrape, bounded by a short timeout.
//
// The values describe the whole database, so every instance exports the
// same numbers: aggregate them with max(), not sum().
//
// When the query fails, the collector increments
// metrics_collect_errors_total and emits no gauges for that scrape. Stale
// values would be worse than none: a frozen tasks_dead of 0 would look
// healthy while the database is unreachable, whereas missing series and
// the error counter both show up in alerts.
type QueueCollector struct {
	db      txctx.Querier
	timeout time.Duration
	log     *slog.Logger

	ready             *prometheus.Desc
	dead              *prometheus.Desc
	oldestReady       *prometheus.Desc
	oldestUnpublished *prometheus.Desc
	errors            prometheus.Counter
}

// NewQueueCollector returns a collector that queries db, each scrape bounded
// by timeout. Register it on a prometheus.Registerer.
func NewQueueCollector(db txctx.Querier, timeout time.Duration, log *slog.Logger) *QueueCollector {
	return &QueueCollector{
		db:      db,
		timeout: timeout,
		log:     log,
		ready: prometheus.NewDesc("tasks_ready",
			"Tasks in the ready state by kind, including those scheduled for later.", []string{"kind"}, nil),
		dead: prometheus.NewDesc("tasks_dead",
			"Tasks in the dead state by kind, waiting for an operator.", []string{"kind"}, nil),
		oldestReady: prometheus.NewDesc("task_oldest_ready_seconds",
			"Age of the oldest task that is due but not yet done; 0 when none.", nil, nil),
		oldestUnpublished: prometheus.NewDesc("outbox_oldest_unpublished_seconds",
			"Age of the oldest unpublished outbox event; 0 when none.", nil, nil),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "metrics_collect_errors_total",
			Help: "Scrapes in which the database gauges could not be read.",
		}),
	}
}

// Describe sends the descriptors of every metric the collector can emit.
func (c *QueueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.ready
	ch <- c.dead
	ch <- c.oldestReady
	ch <- c.oldestUnpublished
	c.errors.Describe(ch)
}

// queueStatsSQL returns one row per value: counts by kind for ready and dead
// tasks, and the two ages.
const queueStatsSQL = `
	SELECT 'ready' AS name, kind, count(*)::float8 AS value
	FROM tasks WHERE state = 'ready' GROUP BY kind
	UNION ALL
	SELECT 'dead', kind, count(*)::float8
	FROM tasks WHERE state = 'dead' GROUP BY kind
	UNION ALL
	SELECT 'oldest_ready', '', coalesce(extract(epoch FROM now() - min(run_after))::float8, 0)
	FROM tasks WHERE state = 'ready' AND run_after <= now()
	UNION ALL
	SELECT 'oldest_unpublished', '', greatest(coalesce(extract(epoch FROM now() - min(created_at))::float8, 0), 0)
	FROM outbox WHERE published_at IS NULL`

// queueStats is one scrape's worth of values.
type queueStats struct {
	ready, dead                    map[string]float64
	oldestReady, oldestUnpublished float64
}

// Collect queries the database and sends the gauges. Prometheus gives
// Collect no context, so the query gets its own timeout.
func (c *QueueCollector) Collect(ch chan<- prometheus.Metric) {
	defer c.errors.Collect(ch)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	stats, err := c.query(ctx)
	if err != nil {
		c.errors.Inc()
		c.log.Warn("collect queue metrics failed", "err", err)
		return
	}
	// Known kinds are always exported, at 0 if absent, so series do not
	// appear and disappear with the workload.
	for _, kind := range ports.TaskKinds {
		for _, m := range []map[string]float64{stats.ready, stats.dead} {
			if _, ok := m[string(kind)]; !ok {
				m[string(kind)] = 0
			}
		}
	}
	for kind, n := range stats.ready {
		ch <- prometheus.MustNewConstMetric(c.ready, prometheus.GaugeValue, n, kind)
	}
	for kind, n := range stats.dead {
		ch <- prometheus.MustNewConstMetric(c.dead, prometheus.GaugeValue, n, kind)
	}
	ch <- prometheus.MustNewConstMetric(c.oldestReady, prometheus.GaugeValue, stats.oldestReady)
	ch <- prometheus.MustNewConstMetric(c.oldestUnpublished, prometheus.GaugeValue, stats.oldestUnpublished)
}

func (c *QueueCollector) query(ctx context.Context) (queueStats, error) {
	rows, err := c.db.Query(ctx, queueStatsSQL)
	if err != nil {
		return queueStats{}, fmt.Errorf("query queue stats: %w", err)
	}
	defer rows.Close()

	stats := queueStats{ready: map[string]float64{}, dead: map[string]float64{}}
	for rows.Next() {
		var (
			name, kind string
			value      float64
		)
		if err := rows.Scan(&name, &kind, &value); err != nil {
			return queueStats{}, fmt.Errorf("scan queue stats: %w", err)
		}
		switch name {
		case "ready":
			stats.ready[kind] = value
		case "dead":
			stats.dead[kind] = value
		case "oldest_ready":
			stats.oldestReady = value
		case "oldest_unpublished":
			stats.oldestUnpublished = value
		}
	}
	if err := rows.Err(); err != nil {
		return queueStats{}, fmt.Errorf("read queue stats: %w", err)
	}
	return stats, nil
}

// PoolCollector exports the connection pool's statistics. Reading them takes
// no query, so it cannot fail.
type PoolCollector struct {
	pool *pgxpool.Pool

	acquired, idle, total, max *prometheus.Desc
	acquires, emptyAcquires    *prometheus.Desc
	acquireWait                *prometheus.Desc
}

// NewPoolCollector returns a collector over pool.
func NewPoolCollector(pool *pgxpool.Pool) *PoolCollector {
	desc := func(name, help string) *prometheus.Desc { return prometheus.NewDesc(name, help, nil, nil) }
	return &PoolCollector{
		pool:          pool,
		acquired:      desc("db_pool_acquired_conns", "Connections currently in use."),
		idle:          desc("db_pool_idle_conns", "Idle connections in the pool."),
		total:         desc("db_pool_total_conns", "Open connections, in use or idle."),
		max:           desc("db_pool_max_conns", "Maximum size of the pool."),
		acquires:      desc("db_pool_acquires_total", "Successful connection acquisitions."),
		emptyAcquires: desc("db_pool_empty_acquires_total", "Acquisitions that had to wait because the pool was empty."),
		acquireWait:   desc("db_pool_acquire_wait_seconds_total", "Total time spent acquiring connections."),
	}
}

// Describe sends the descriptors of every pool metric.
func (c *PoolCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.acquired, c.idle, c.total, c.max, c.acquires, c.emptyAcquires, c.acquireWait} {
		ch <- d
	}
}

// Collect reads pgxpool.Stat and sends the values.
func (c *PoolCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.pool.Stat()
	ch <- prometheus.MustNewConstMetric(c.acquired, prometheus.GaugeValue, float64(s.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(c.idle, prometheus.GaugeValue, float64(s.IdleConns()))
	ch <- prometheus.MustNewConstMetric(c.total, prometheus.GaugeValue, float64(s.TotalConns()))
	ch <- prometheus.MustNewConstMetric(c.max, prometheus.GaugeValue, float64(s.MaxConns()))
	ch <- prometheus.MustNewConstMetric(c.acquires, prometheus.CounterValue, float64(s.AcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.emptyAcquires, prometheus.CounterValue, float64(s.EmptyAcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.acquireWait, prometheus.CounterValue, s.AcquireDuration().Seconds())
}
