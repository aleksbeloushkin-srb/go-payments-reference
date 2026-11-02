package txctx

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
)

// fakeTx records Commit and Rollback. The embedded interface is nil: any
// other method panics, which is fine because Do never calls them.
type fakeTx struct {
	pgx.Tx
	commits, rollbacks int
	commitErr          error
}

func (f *fakeTx) Commit(context.Context) error {
	f.commits++
	return f.commitErr
}

func (f *fakeTx) Rollback(context.Context) error {
	f.rollbacks++
	return nil
}

type fakePool struct {
	Querier
	begins int
	opts   pgx.TxOptions
	tx     *fakeTx
}

func (f *fakePool) BeginTx(_ context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	f.begins++
	f.opts = opts
	return f.tx, nil
}

func newFake() (*Manager, *fakePool) {
	db := &fakePool{tx: &fakeTx{}}
	return New(db), db
}

func TestRunCommitsAndRunsHooksAfterCommit(t *testing.T) {
	tr, db := newFake()
	var hookSawCommit bool

	err := tr.Run(context.Background(), func(ctx context.Context) error {
		if !InTx(ctx) {
			t.Fatal("ctx must carry the transaction")
		}
		if tr.DB(ctx) != Querier(db.tx) {
			t.Fatal("DB must return the transaction inside Run")
		}
		AfterCommit(ctx, func(ctx context.Context) {
			hookSawCommit = db.tx.commits == 1
			if InTx(ctx) {
				t.Error("hook ctx must not carry the finished transaction")
			}
		})
		return nil
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if db.tx.commits != 1 || db.tx.rollbacks != 0 {
		t.Fatalf("commits=%d rollbacks=%d", db.tx.commits, db.tx.rollbacks)
	}
	if !hookSawCommit {
		t.Fatal("hook must run after commit")
	}
	if db.opts.IsoLevel != pgx.ReadCommitted {
		t.Fatalf("isolation = %q, want read committed", db.opts.IsoLevel)
	}
}

func TestRunRollsBackOnErrorAndDropsHooks(t *testing.T) {
	tr, db := newFake()
	boom := errors.New("boom")
	hookRan := false

	err := tr.Run(context.Background(), func(ctx context.Context) error {
		AfterCommit(ctx, func(context.Context) { hookRan = true })
		return boom
	})

	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if db.tx.commits != 0 || db.tx.rollbacks != 1 {
		t.Fatalf("commits=%d rollbacks=%d", db.tx.commits, db.tx.rollbacks)
	}
	if hookRan {
		t.Fatal("hook must not run after rollback")
	}
}

func TestNestedRunJoinsOuterTransaction(t *testing.T) {
	tr, db := newFake()
	hooks := 0

	err := tr.Run(context.Background(), func(ctx context.Context) error {
		return tr.Run(ctx, func(ctx context.Context) error {
			AfterCommit(ctx, func(context.Context) { hooks++ })
			return nil
		})
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if db.begins != 1 || db.tx.commits != 1 {
		t.Fatalf("begins=%d commits=%d, want 1 and 1", db.begins, db.tx.commits)
	}
	if hooks != 1 {
		t.Fatalf("inner hook ran %d times, want once after the outer commit", hooks)
	}
}

func TestRunRollsBackOnPanic(t *testing.T) {
	tr, db := newFake()
	defer func() {
		if recover() == nil {
			t.Fatal("panic must propagate")
		}
		if db.tx.rollbacks != 1 {
			t.Fatalf("rollbacks=%d, want 1", db.tx.rollbacks)
		}
	}()
	_ = tr.Run(context.Background(), func(context.Context) error { panic("handler bug") })
}

func TestCommitErrorIsReturnedAndHooksSkipped(t *testing.T) {
	tr, db := newFake()
	db.tx.commitErr = errors.New("connection reset")
	hookRan := false

	err := tr.Run(context.Background(), func(ctx context.Context) error {
		AfterCommit(ctx, func(context.Context) { hookRan = true })
		return nil
	})
	if !errors.Is(err, db.tx.commitErr) {
		t.Fatalf("err = %v, want wrapped commit error", err)
	}
	if hookRan {
		t.Fatal("hook must not run when commit failed")
	}
}

func TestAfterCommitOutsideTxRunsImmediately(t *testing.T) {
	ran := false
	AfterCommit(context.Background(), func(context.Context) { ran = true })
	if !ran {
		t.Fatal("hook must run immediately outside a transaction")
	}
}
