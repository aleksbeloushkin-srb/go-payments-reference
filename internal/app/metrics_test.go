package app

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// Business metrics count only committed work: a replay creates nothing, and
// a start that rolls back changes no status.
func TestMetricsCountOnlyCommittedWork(t *testing.T) {
	f := newFixture(testConfig)
	ctx := context.Background()

	res, err := f.svc.CreatePayment(ctx, validRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.CreatePayment(ctx, validRequest()); err != nil {
		t.Fatal(err)
	}
	if f.metrics.created != 1 {
		t.Fatalf("created = %d, want 1: a replay is not a new payment", f.metrics.created)
	}

	f.gateway.createErr = ports.ErrGatewayAmbiguous
	_ = f.svc.StartPayment(ctx, res.Payment.ID)
	if len(f.metrics.changes) != 0 {
		t.Fatalf("changes after a rolled-back start = %v, want none", f.metrics.changes)
	}

	f.gateway.createErr = nil
	f.gateway.createResp = ports.GatewayPayment{Ref: "gw_1", Status: payment.StatusProcessing}
	if err := f.svc.StartPayment(ctx, res.Payment.ID); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(GatewayEvent{ID: "evt", PaymentID: res.Payment.ID, GatewayRef: "gw_1", Status: payment.StatusSucceeded})
	if err := f.svc.ApplyGatewayEvent(ctx, payload); err != nil {
		t.Fatal(err)
	}
	// A duplicate event changes nothing and reports nothing.
	if err := f.svc.ApplyGatewayEvent(ctx, payload); err != nil {
		t.Fatal(err)
	}

	want := []string{"pending->processing", "processing->succeeded"}
	if diff := cmp.Diff(want, f.metrics.changes); diff != "" {
		t.Fatalf("reported changes (-want +got):\n%s", diff)
	}
}
