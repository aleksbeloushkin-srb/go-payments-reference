package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
)

func TestBusinessMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	b := NewBusiness(reg)

	b.PaymentCreated()
	b.StatusChanged(payment.StatusPending, payment.StatusProcessing, time.Second)
	b.StatusChanged(payment.StatusProcessing, payment.StatusSucceeded, 3*time.Second)

	if got := testutil.ToFloat64(b.created); got != 1 {
		t.Fatalf("payments_created_total = %v, want 1", got)
	}
	want := `
# HELP payment_transitions_total Committed payment status changes.
# TYPE payment_transitions_total counter
payment_transitions_total{from="pending",to="processing"} 1
payment_transitions_total{from="processing",to="succeeded"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "payment_transitions_total"); err != nil {
		t.Fatal(err)
	}
	// Only the final status is observed in the time-to-final histogram.
	if n := testutil.CollectAndCount(b.timeToFinal); n != 1 {
		t.Fatalf("time-to-final series = %d, want 1 (succeeded only)", n)
	}
}
