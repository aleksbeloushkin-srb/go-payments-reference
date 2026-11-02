// Package metrics implements ports.Metrics, the business measurements of
// the payment service, with Prometheus.
//
// Metrics are registered on a registry passed in by the composition root,
// never on the global default registry, so tests can create as many
// instances as they like.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
)

// Business records how many payments are created, how they move between
// statuses and how long they take to finish.
type Business struct {
	created     prometheus.Counter
	transitions *prometheus.CounterVec
	timeToFinal *prometheus.HistogramVec
}

// NewBusiness creates the business metrics and registers them on reg.
func NewBusiness(reg prometheus.Registerer) *Business {
	b := &Business{
		created: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "payments_created_total",
			Help: "Payments accepted. A replay of an existing Idempotency-Key is not counted.",
		}),
		transitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "payment_transitions_total",
			Help: "Committed payment status changes.",
		}, []string{"from", "to"}),
		timeToFinal: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "payment_time_to_final_seconds",
			Help: "Time from creation to a final status.",
			// From an instant decline to the end of polling (two hours).
			Buckets: []float64{0.5, 1, 2, 5, 10, 30, 60, 300, 900, 3600, 7200},
		}, []string{"status"}),
	}
	reg.MustRegister(b.created, b.transitions, b.timeToFinal)
	return b
}

// PaymentCreated counts a new payment.
func (b *Business) PaymentCreated() {
	b.created.Inc()
}

// StatusChanged counts a transition and, for a final status, observes how
// long the payment took.
func (b *Business) StatusChanged(from, to payment.Status, age time.Duration) {
	b.transitions.WithLabelValues(string(from), string(to)).Inc()
	if to.Final() {
		b.timeToFinal.WithLabelValues(string(to)).Observe(age.Seconds())
	}
}
