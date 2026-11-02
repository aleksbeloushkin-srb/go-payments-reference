package queue

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// Attempt outcomes recorded in task_attempts_total.
const (
	outcomeOK        = "ok"
	outcomeRetry     = "retry"
	outcomePermanent = "permanent"
	outcomeDead      = "dead"
)

// Metrics are the task queue's Prometheus metrics. Labels are the task kind
// and the outcome, both from small fixed sets; task and payment ids never
// become labels.
type Metrics struct {
	claimed   *prometheus.CounterVec
	attempts  *prometheus.CounterVec
	lag       *prometheus.HistogramVec
	leaseLost prometheus.Counter
	panics    prometheus.Counter
}

// NewMetrics creates the queue metrics and registers them on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		claimed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tasks_claimed_total",
			Help: "Tasks claimed by this worker.",
		}, []string{"kind"}),
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "task_attempts_total",
			Help: "Settled task runs by outcome: ok (done or postponed), retry, permanent (dead at once), dead (out of attempts).",
		}, []string{"kind", "outcome"}),
		lag: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "task_lag_seconds",
			Help:    "Time between a task becoming due (run_after) and its claim.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300},
		}, []string{"kind"}),
		leaseLost: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lease_lost_total",
			Help: "Task results discarded because the lease had passed to another worker.",
		}),
		panics: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_panics_total",
			Help: "Task handler panics recovered by the worker.",
		}),
	}
	reg.MustRegister(m.claimed, m.attempts, m.lag, m.leaseLost, m.panics)
	// Create the series of every known kind at zero, so increase() and
	// rate() see the first event instead of starting from it.
	for _, kind := range ports.TaskKinds {
		m.claimed.WithLabelValues(string(kind))
		for _, o := range []string{outcomeOK, outcomeRetry, outcomePermanent, outcomeDead} {
			m.attempts.WithLabelValues(string(kind), o)
		}
	}
	return m
}
