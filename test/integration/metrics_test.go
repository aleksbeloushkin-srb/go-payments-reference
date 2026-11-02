//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/adapters/out/postgres"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/app"
)

// gather collects reg into "name{label=value}" -> value.
func gather(t *testing.T, reg *prometheus.Registry) map[string]float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			key := mf.GetName()
			for _, l := range m.GetLabel() {
				key += "{" + l.GetName() + "=" + l.GetValue() + "}"
			}
			out[key] = value(m)
		}
	}
	return out
}

func value(m *dto.Metric) float64 {
	switch {
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue()
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue()
	}
	return 0
}

// The queue collector's SQL against a real schema: counts by state and kind,
// and the ages of the oldest due task and the oldest unpublished event.
func TestQueueCollector(t *testing.T) {
	newEnv(t, app.Config{}) // empties the tables
	ctx := context.Background()
	exec := func(sql string) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	exec(`INSERT INTO tasks (kind, payment_id, dedup_key, run_after) VALUES
		('start_payment', gen_random_uuid(), 'start', now() - interval '30 seconds'),
		('start_payment', gen_random_uuid(), 'start', now() + interval '1 hour'),
		('poll_payment',  gen_random_uuid(), 'poll',  now() - interval '5 seconds')`)
	exec(`INSERT INTO tasks (kind, payment_id, dedup_key, run_after, state) VALUES
		('apply_gateway_event', gen_random_uuid(), 'evt_1', now(), 'dead')`)
	exec(`INSERT INTO outbox (payment_id, type, payload, created_at) VALUES
		(gen_random_uuid(), 'payment.created', '{}', now() - interval '90 seconds')`)

	reg := prometheus.NewRegistry()
	reg.MustRegister(postgres.NewQueueCollector(pool, 2*time.Second, discard()))
	got := gather(t, reg)

	for key, want := range map[string]float64{
		"tasks_ready{kind=start_payment}":       2,
		"tasks_ready{kind=poll_payment}":        1,
		"tasks_ready{kind=apply_gateway_event}": 0,
		"tasks_dead{kind=apply_gateway_event}":  1,
		"tasks_dead{kind=start_payment}":        0,
		"metrics_collect_errors_total":          0,
	} {
		if got[key] != want {
			t.Errorf("%s = %v, want %v", key, got[key], want)
		}
	}
	if age := got["task_oldest_ready_seconds"]; age < 30 || age > 60 {
		t.Errorf("task_oldest_ready_seconds = %v, want about 30 (the future task does not count)", age)
	}
	if age := got["outbox_oldest_unpublished_seconds"]; age < 90 || age > 120 {
		t.Errorf("outbox_oldest_unpublished_seconds = %v, want about 90", age)
	}
}

func TestPoolCollector(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(postgres.NewPoolCollector(pool))
	got := gather(t, reg)

	if got["db_pool_max_conns"] != 20 || got["db_pool_total_conns"] < 1 || got["db_pool_acquires_total"] < 1 {
		t.Fatalf("pool metrics = %v", got)
	}
	if _, ok := got["db_pool_acquire_wait_seconds_total"]; !ok {
		t.Fatal("db_pool_acquire_wait_seconds_total is missing")
	}
}
