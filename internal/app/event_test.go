package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

func TestAcceptGatewayEventDeduplicatesByEventID(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusProcessing)
	e := GatewayEvent{ID: "evt_1", PaymentID: p.ID, GatewayRef: "gw_1", Status: payment.StatusSucceeded}

	for range 2 {
		if err := f.svc.AcceptGatewayEvent(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}

	tasks := f.store.tasksOf(ports.TaskApplyGatewayEvent)
	if len(tasks) != 1 || tasks[0].DedupKey != "evt_1" {
		t.Fatalf("tasks = %+v, want one task keyed by the event id", tasks)
	}
}

func TestAcceptGatewayEventRejectsUnknownPayment(t *testing.T) {
	f := newFixture(testConfig)
	e := GatewayEvent{ID: "evt_1", PaymentID: uuid.New(), Status: payment.StatusSucceeded}

	if err := f.svc.AcceptGatewayEvent(context.Background(), e); !errors.Is(err, payment.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestApplyGatewayEvent(t *testing.T) {
	tests := []struct {
		name        string
		current     payment.Status
		reported    payment.Status
		wantStatus  payment.Status
		wantPerm    bool
		wantVersion int64
	}{
		{name: "forward", current: payment.StatusProcessing, reported: payment.StatusSucceeded, wantStatus: payment.StatusSucceeded, wantVersion: 2},
		{name: "duplicate is a no-op", current: payment.StatusSucceeded, reported: payment.StatusSucceeded, wantStatus: payment.StatusSucceeded, wantVersion: 1},
		{name: "stale non-final is ignored", current: payment.StatusSucceeded, reported: payment.StatusProcessing, wantStatus: payment.StatusSucceeded, wantVersion: 1},
		{name: "conflicting final goes dead", current: payment.StatusSucceeded, reported: payment.StatusFailed, wantStatus: payment.StatusSucceeded, wantPerm: true, wantVersion: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(testConfig)
			p := f.seed(tt.current)
			payload, _ := json.Marshal(GatewayEvent{ID: "evt", PaymentID: p.ID, GatewayRef: "gw_1", Status: tt.reported})

			err := f.svc.ApplyGatewayEvent(context.Background(), payload)

			var perm *ports.PermanentError
			if errors.As(err, &perm) != tt.wantPerm || (!tt.wantPerm && err != nil) {
				t.Fatalf("err = %v, want permanent %v", err, tt.wantPerm)
			}
			got := f.store.payments[p.ID]
			if got.Status != tt.wantStatus || got.Version != tt.wantVersion {
				t.Fatalf("payment = %s v%d, want %s v%d", got.Status, got.Version, tt.wantStatus, tt.wantVersion)
			}
		})
	}
}

func TestApplyGatewayEventBadPayloadIsPermanent(t *testing.T) {
	f := newFixture(testConfig)
	var perm *ports.PermanentError
	if err := f.svc.ApplyGatewayEvent(context.Background(), []byte("{")); !errors.As(err, &perm) {
		t.Fatalf("err = %v, want permanent", err)
	}
}

func TestApplyGatewayEventReferenceMismatchGoesDead(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusProcessing) // stored reference gw_1
	payload, _ := json.Marshal(GatewayEvent{ID: "evt", PaymentID: p.ID, GatewayRef: "gw_other", Status: payment.StatusSucceeded})

	err := f.svc.ApplyGatewayEvent(context.Background(), payload)

	var perm *ports.PermanentError
	if !errors.As(err, &perm) {
		t.Fatalf("err = %v, want permanent", err)
	}
	if got := f.store.payments[p.ID]; got.Status != payment.StatusProcessing || got.GatewayRef != "gw_1" {
		t.Fatalf("payment = %s %s, want it untouched", got.Status, got.GatewayRef)
	}
}

func TestNonFinalTransitionSchedulesOnePoll(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusProcessing)
	for i, status := range []payment.Status{payment.StatusRequiresAction, payment.StatusProcessing, payment.StatusSucceeded} {
		payload, _ := json.Marshal(GatewayEvent{ID: fmt.Sprint("evt", i), PaymentID: p.ID, GatewayRef: "gw_1", Status: status})
		if err := f.svc.ApplyGatewayEvent(context.Background(), payload); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(f.store.tasksOf(ports.TaskPollPayment)); n != 1 {
		t.Fatalf("poll tasks = %d, want 1", n)
	}
}
