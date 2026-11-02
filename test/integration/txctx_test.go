//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/app"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/pkg/txctx"
)

func newPayment(t *testing.T, key string) payment.Payment {
	t.Helper()
	p, err := payment.New(uuid.New(), "merchant-1", payment.Money{AmountMinor: 100, Currency: "EUR"}, key, "hash", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// A nested Run joins the outer transaction: an error anywhere rolls back
// every write, and AfterCommit hooks run only after a real commit.
func TestTransactionJoinRollbackAndHooks(t *testing.T) {
	e := newEnv(t, defaultConfig())
	ctx := context.Background()
	hooks := 0

	err := e.tx.Run(ctx, func(ctx context.Context) error {
		if _, _, err := e.payments.Insert(ctx, newPayment(t, "a")); err != nil {
			return err
		}
		return e.tx.Run(ctx, func(ctx context.Context) error {
			if err := e.outbox.Record(ctx, payment.NewEvent(payment.EventCreated, newPayment(t, "b"))); err != nil {
				return err
			}
			txctx.AfterCommit(ctx, func(context.Context) { hooks++ })
			return errors.New("inner failure")
		})
	})
	if err == nil {
		t.Fatal("want the inner error")
	}
	if count(t, `SELECT count(*) FROM payments`) != 0 || count(t, `SELECT count(*) FROM outbox`) != 0 || hooks != 0 {
		t.Fatal("a failed unit of work must leave nothing behind and run no hooks")
	}

	err = e.tx.Run(ctx, func(ctx context.Context) error {
		if _, _, err := e.payments.Insert(ctx, newPayment(t, "c")); err != nil {
			return err
		}
		txctx.AfterCommit(ctx, func(context.Context) {
			// Runs after COMMIT: a new connection already sees the row.
			hooks += count(t, `SELECT count(*) FROM payments`)
		})
		return nil
	})
	if err != nil || hooks != 1 {
		t.Fatalf("err = %v, hooks = %d", err, hooks)
	}
}

// The commit hook of Enqueue wakes the local worker only after commit.
func TestEnqueueSignalsAfterCommit(t *testing.T) {
	e := newEnv(t, defaultConfig())
	_ = e.tx.Run(context.Background(), func(ctx context.Context) error {
		createPaymentIn(t, ctx, e)
		select {
		case <-e.tasks.Ready():
			t.Fatal("signal before commit")
		default:
		}
		return nil
	})
	select {
	case <-e.tasks.Ready():
	default:
		t.Fatal("no signal after commit")
	}
}

func createPaymentIn(t *testing.T, ctx context.Context, e *env) {
	t.Helper()
	_, err := e.svc.CreatePayment(ctx, app.CreatePaymentRequest{
		IdempotencyKey: "k", MerchantID: "merchant-1", AmountMinor: 100, Currency: "EUR",
	})
	if err != nil {
		t.Fatal(err)
	}
}
