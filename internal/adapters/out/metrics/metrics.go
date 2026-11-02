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
	reconciled  *prometheus.CounterVec
	duplicates  prometheus.Counter
}

// reconcileOutcomes lists every value of the outcome label of
// reconcile_total.
var reconcileOutcomes = []string{"found", "not_found", "abandoned", "duplicate", "error"}

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
			// From an instant decline to the end of polling (two hours), fine
			// in the first seconds, where interactive payments end.
			Buckets: []float64{0.25, 0.5, 0.75, 1, 1.5, 2, 2.5, 3, 4, 5, 7.5, 10, 30, 60, 300, 900, 3600, 7200},
		}, []string{"status"}),
		reconciled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reconcile_total",
			Help: "Reconciliations of ambiguous gateway attempts by outcome: found, not_found, abandoned, duplicate, error.",
		}, []string{"outcome"}),
	}
	b.duplicates = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gateway_duplicates_detected_total",
		Help: "Times the gateway was found holding more than one payment for one bank payment: a double charge to refund.",
	})
	reg.MustRegister(b.created, b.transitions, b.timeToFinal, b.reconciled, b.duplicates)
	// Created at zero so increase() sees the first "duplicate" or "error".
	for _, o := range reconcileOutcomes {
		b.reconciled.WithLabelValues(o)
	}
	return b
}

// DuplicateDetected counts a detected double charge.
func (b *Business) DuplicateDetected() {
	b.duplicates.Inc()
}

// Reconciled counts one reconciliation outcome.
func (b *Business) Reconciled(outcome string) {
	b.reconciled.WithLabelValues(outcome).Inc()
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
