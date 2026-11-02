//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/app"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// The attempts store against the real schema: numbering, keys, the
// one-open-attempt rule and the state check on update.
func TestAttemptsStore(t *testing.T) {
	e := newEnv(t, app.Config{})
	ctx := context.Background()
	p := createPayment(t, e, "key-1", 1500).Payment
	now := e.clock.Now()

	err := e.tx.Run(ctx, func(ctx context.Context) error {
		if _, ok, err := e.attempts.Open(ctx, p.ID); err != nil || ok {
			t.Fatalf("Open on a fresh payment = %v, %v; want none", ok, err)
		}
		first, err := e.attempts.Create(ctx, p.ID, now)
		if err != nil {
			return err
		}
		if first.Number != 1 || first.IdempotencyKey != p.ID.String()+":1" || first.State != ports.AttemptIntent {
			t.Fatalf("first attempt = %+v", first)
		}
		open, ok, err := e.attempts.Open(ctx, p.ID)
		if err != nil || !ok || open.Number != 1 {
			t.Fatalf("Open = %+v, %v, %v", open, ok, err)
		}

		first.State, first.UpdatedAt = ports.AttemptUnknown, now
		if err := e.attempts.Update(ctx, first, ports.AttemptIntent); err != nil {
			return err
		}
		stale := first
		stale.State = ports.AttemptDone
		if err := e.attempts.Update(ctx, stale, ports.AttemptIntent); !errors.Is(err, ports.ErrAttemptChanged) {
			t.Fatalf("update from a stale state = %v, want ErrAttemptChanged", err)
		}

		first.State, first.NotFound = ports.AttemptAbandoned, 3
		if err := e.attempts.Update(ctx, first, ports.AttemptUnknown); err != nil {
			return err
		}
		second, err := e.attempts.Create(ctx, p.ID, now)
		if err != nil {
			return err
		}
		if second.Number != 2 || second.IdempotencyKey != p.ID.String()+":2" {
			t.Fatalf("second attempt = %+v", second)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// A second open attempt is refused by the partial unique index.
	err = e.tx.Run(ctx, func(ctx context.Context) error {
		_, err := e.attempts.Create(ctx, p.ID, now)
		return err
	})
	if err == nil {
		t.Fatal("a second open attempt must be refused")
	}
	if _, err := e.attempts.Create(ctx, p.ID, now); err == nil {
		t.Fatal("Create outside a transaction must fail")
	}
	got, err := e.attempts.Get(ctx, p.ID, 1)
	if err != nil || got.State != ports.AttemptAbandoned || got.NotFound != 3 {
		t.Fatalf("attempt 1 = %+v, %v", got, err)
	}
	if _, err := e.attempts.Get(ctx, p.ID, 9); !errors.Is(err, ports.ErrAttemptNotFound) {
		t.Fatalf("missing attempt = %v", err)
	}
}
