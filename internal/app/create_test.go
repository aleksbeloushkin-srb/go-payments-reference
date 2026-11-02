package app

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

func validRequest() CreatePaymentRequest {
	return CreatePaymentRequest{IdempotencyKey: "key-1", MerchantID: "merchant-1", AmountMinor: 1500, Currency: "EUR"}
}

func TestCreatePaymentWritesPaymentTaskAndEventTogether(t *testing.T) {
	f := newFixture(testConfig)

	res, err := f.svc.CreatePayment(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}

	if !res.Created || res.Payment.Status != payment.StatusPending || res.Payment.Version != 1 {
		t.Fatalf("got %+v", res)
	}
	start := f.store.tasksOf(ports.TaskStartPayment)
	if len(start) != 1 || start[0].PaymentID != res.Payment.ID || !start[0].RunAfter.Equal(f.clock.now) {
		t.Fatalf("start tasks = %+v", start)
	}
	if diff := cmp.Diff([]payment.EventType{payment.EventCreated}, f.store.eventTypes()); diff != "" {
		t.Fatalf("events (-want +got):\n%s", diff)
	}
}

func TestCreatePaymentReplayReturnsStoredPayment(t *testing.T) {
	f := newFixture(testConfig)
	first, err := f.svc.CreatePayment(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}

	again, err := f.svc.CreatePayment(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if again.Created || again.Payment.ID != first.Payment.ID {
		t.Fatalf("replay = %+v, want the stored payment %s", again, first.Payment.ID)
	}
	if n := len(f.store.tasksOf(ports.TaskStartPayment)); n != 1 {
		t.Fatalf("start tasks = %d, want 1", n)
	}
	if n := len(f.store.eventTypes()); n != 1 {
		t.Fatalf("events = %d, want 1", n)
	}
}

func TestCreatePaymentSameKeyDifferentBodyConflicts(t *testing.T) {
	f := newFixture(testConfig)
	if _, err := f.svc.CreatePayment(context.Background(), validRequest()); err != nil {
		t.Fatal(err)
	}
	other := validRequest()
	other.AmountMinor = 9900

	_, err := f.svc.CreatePayment(context.Background(), other)

	if !errors.Is(err, payment.ErrIdempotencyConflict) {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}
}

func TestCreatePaymentValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*CreatePaymentRequest)
		field  string
	}{
		{"missing key", func(r *CreatePaymentRequest) { r.IdempotencyKey = "" }, "Idempotency-Key"},
		{"non-ASCII key", func(r *CreatePaymentRequest) { r.IdempotencyKey = "clé-1" }, "Idempotency-Key"},
		{"zero amount", func(r *CreatePaymentRequest) { r.AmountMinor = 0 }, "amount_minor"},
		{"bad currency", func(r *CreatePaymentRequest) { r.Currency = "euro" }, "currency"},
		{"missing merchant", func(r *CreatePaymentRequest) { r.MerchantID = "" }, "merchant_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(testConfig)
			req := validRequest()
			tt.mutate(&req)

			_, err := f.svc.CreatePayment(context.Background(), req)

			var verr *payment.ValidationError
			if !errors.As(err, &verr) || verr.Field != tt.field {
				t.Fatalf("err = %v, want validation error on %s", err, tt.field)
			}
			if len(f.store.payments) != 0 {
				t.Fatal("nothing must be stored for an invalid request")
			}
		})
	}
}
