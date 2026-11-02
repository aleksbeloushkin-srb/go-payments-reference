package longpoll

import "github.com/prometheus/client_golang/prometheus"

// Metrics are the long-poll Prometheus metrics.
type Metrics struct {
	waiters    prometheus.Gauge
	rejected   prometheus.Counter
	batchQuery prometheus.Histogram
}

// NewMetrics creates the long-poll metrics and registers them on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		waiters: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "longpoll_waiters",
			Help: "Long polls currently waiting on this instance.",
		}),
		rejected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "longpoll_rejected_total",
			Help: "Long polls answered 429 because the per-instance cap was reached.",
		}),
		batchQuery: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "longpoll_batch_query_duration_seconds",
			Help:    "Duration of the batcher's version query.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		}),
	}
	reg.MustRegister(m.waiters, m.rejected, m.batchQuery)
	return m
}
