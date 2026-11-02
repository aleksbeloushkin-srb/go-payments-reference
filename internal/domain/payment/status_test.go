package payment

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

var allStatuses = []Status{
	StatusPending, StatusProcessing, StatusRequiresAction,
	StatusSucceeded, StatusFailed, StatusCanceled,
}

func TestStatusTransitions(t *testing.T) {
	want := map[[2]Status]bool{
		{StatusPending, StatusProcessing}:        true,
		{StatusPending, StatusRequiresAction}:    true,
		{StatusPending, StatusSucceeded}:         true,
		{StatusPending, StatusFailed}:            true,
		{StatusPending, StatusCanceled}:          true,
		{StatusProcessing, StatusRequiresAction}: true,
		{StatusProcessing, StatusSucceeded}:      true,
		{StatusProcessing, StatusFailed}:         true,
		{StatusProcessing, StatusCanceled}:       true,
		{StatusRequiresAction, StatusProcessing}: true,
		{StatusRequiresAction, StatusSucceeded}:  true,
		{StatusRequiresAction, StatusFailed}:     true,
		{StatusRequiresAction, StatusCanceled}:   true,
	}
	for _, from := range allStatuses {
		for _, to := range allStatuses {
			t.Run(string(from)+"->"+string(to), func(t *testing.T) {
				wantOK := want[[2]Status{from, to}]
				if got := from.CanTransitionTo(to); got != wantOK {
					t.Fatalf("CanTransitionTo = %v, want %v", got, wantOK)
				}
			})
		}
	}
}

func TestFinalStatusesHaveNoExit(t *testing.T) {
	for _, s := range allStatuses {
		if s.Final() != (len(transitions[s]) == 0) {
			t.Fatalf("%s: Final()=%v but has %d outgoing transitions", s, s.Final(), len(transitions[s]))
		}
	}
}

// Every status in the table must be a known status, so a typo there cannot
// create an unreachable state.
func TestTransitionTableUsesKnownStatuses(t *testing.T) {
	for from, tos := range transitions {
		if !from.Valid() {
			t.Fatalf("unknown status %q in table", from)
		}
		for _, to := range tos {
			if !to.Valid() {
				t.Fatalf("unknown status %q in table", to)
			}
		}
	}
}

func TestPaymentTransitionTo(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	later := now.Add(time.Minute)

	tests := []struct {
		name        string
		from        Status
		to          Status
		wantChanged bool
		wantErr     error
		wantVersion int64
	}{
		{name: "forward", from: StatusPending, to: StatusProcessing, wantChanged: true, wantVersion: 2},
		{name: "straight to final", from: StatusPending, to: StatusSucceeded, wantChanged: true, wantVersion: 2},
		{name: "action completed", from: StatusRequiresAction, to: StatusProcessing, wantChanged: true, wantVersion: 2},
		{name: "same status is a no-op", from: StatusProcessing, to: StatusProcessing, wantVersion: 1},
		{name: "no regress from final", from: StatusSucceeded, to: StatusProcessing, wantErr: ErrInvalidTransition, wantVersion: 1},
		{name: "no flip between finals", from: StatusSucceeded, to: StatusFailed, wantErr: ErrInvalidTransition, wantVersion: 1},
		{name: "no return to pending", from: StatusProcessing, to: StatusPending, wantErr: ErrInvalidTransition, wantVersion: 1},
		{name: "unknown status", from: StatusProcessing, to: Status("refunded"), wantErr: ErrInvalidTransition, wantVersion: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := New(uuid.New(), "m-1", Money{AmountMinor: 100, Currency: "EUR"}, "k", "h", now)
			if err != nil {
				t.Fatal(err)
			}
			p.Status = tt.from

			changed, err := p.TransitionTo(tt.to, later)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if changed != tt.wantChanged {
				t.Fatalf("changed = %v, want %v", changed, tt.wantChanged)
			}
			if p.Version != tt.wantVersion {
				t.Fatalf("version = %d, want %d", p.Version, tt.wantVersion)
			}
			if changed && !p.UpdatedAt.Equal(later) {
				t.Fatalf("updated_at = %v, want %v", p.UpdatedAt, later)
			}
			if !changed && p.Status != tt.from {
				t.Fatalf("status changed to %s on a rejected transition", p.Status)
			}
		})
	}
}

func TestNewValidatesMerchantID(t *testing.T) {
	m := Money{AmountMinor: 1, Currency: "EUR"}
	for _, id := range []string{"", string(make([]byte, MaxMerchantIDLen+1)), "café", "a\tb"} {
		_, err := New(uuid.New(), id, m, "k", "h", time.Now())
		var verr *ValidationError
		if !errors.As(err, &verr) || verr.Field != "merchant_id" {
			t.Fatalf("merchant id %q: err = %v, want merchant_id validation error", id, err)
		}
	}
}
