// Package txctx carries a database transaction in a context.Context.
//
// Use cases call Manager.Run and never see SQL types; repositories call
// Manager.DB(ctx) and transparently run inside the caller's transaction, or
// on the pool when there is none.
//
// Trade-offs, stated plainly:
//
//   - The dependency is invisible in signatures. A repository method called
//     outside Run silently autocommits. Methods that are only correct inside
//     a transaction (SELECT ... FOR UPDATE) must check InTx and fail loudly.
//   - A nested Run joins the outer transaction instead of opening a
//     savepoint. An inner error therefore aborts the whole unit of work: the
//     outer fn cannot swallow it and carry on, because PostgreSQL has already
//     marked the transaction as failed. Savepoints would allow partial
//     rollback but make AfterCommit semantics subtle (a hook must wait for
//     the outermost COMMIT, not for RELEASE SAVEPOINT); this package keeps
//     the simpler rule.
//   - Goroutines started inside fn inherit the transaction through ctx.
//     Start them from an AfterCommit hook instead.
//   - A failed Commit is ambiguous: the server may have committed before the
//     connection broke. Callers that need certainty must re-read.
package txctx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// rollbackTimeout bounds a rollback that runs after the caller's context was
// cancelled.
const rollbackTimeout = 5 * time.Second

// Querier is the subset of pgx shared by a pool, a connection and a tx.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Pool is what the Manager needs from a connection pool. *pgxpool.Pool
// implements it.
type Pool interface {
	Querier
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}

// Manager opens transactions and resolves the right Querier for a ctx.
type Manager struct {
	pool Pool
}

// New returns a Manager over pool.
func New(pool Pool) *Manager {
	return &Manager{pool: pool}
}

type txKey struct{}

type txState struct {
	tx          pgx.Tx
	afterCommit []func(context.Context)
}

func stateFrom(ctx context.Context) *txState {
	st, _ := ctx.Value(txKey{}).(*txState)
	return st
}

// Run executes fn in a transaction. If ctx already carries one, fn joins it
// and the outermost Run decides commit or rollback. fn returning an error or
// panicking rolls back; otherwise Run commits and then runs AfterCommit
// hooks.
//
// The isolation level is READ COMMITTED, set explicitly rather than taken
// from the server default: the idempotent insert relies on each statement
// seeing rows committed by concurrent transactions.
func (m *Manager) Run(ctx context.Context, fn func(ctx context.Context) error) error {
	if stateFrom(ctx) != nil {
		return fn(ctx)
	}

	tx, err := m.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("txctx: begin: %w", err)
	}
	st := &txState{tx: tx}

	defer func() {
		if p := recover(); p != nil {
			_ = rollback(ctx, tx)
			panic(p)
		}
	}()

	if err := fn(context.WithValue(ctx, txKey{}, st)); err != nil {
		if rbErr := rollback(ctx, tx); rbErr != nil {
			return errors.Join(err, rbErr)
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("txctx: commit: %w", err)
	}

	// Hooks get the caller's values without its cancellation: the commit has
	// happened, so follow-up work should not be cut short by a client that
	// has gone away.
	hookCtx := context.WithoutCancel(ctx)
	for _, hook := range st.afterCommit {
		hook(hookCtx)
	}
	return nil
}

func rollback(ctx context.Context, tx pgx.Tx) error {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
	defer cancel()
	if err := tx.Rollback(rctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return fmt.Errorf("txctx: rollback: %w", err)
	}
	return nil
}

// DB returns the transaction carried by ctx, or the pool if there is none.
func (m *Manager) DB(ctx context.Context) Querier {
	if st := stateFrom(ctx); st != nil {
		return st.tx
	}
	return m.pool
}

// InTx reports whether ctx carries a transaction.
func InTx(ctx context.Context) bool {
	return stateFrom(ctx) != nil
}

// AfterCommit registers hook to run after the transaction in ctx commits. It
// is dropped on rollback. Without a transaction the hook runs immediately,
// because the preceding write has already been autocommitted.
//
// Hooks run synchronously, in registration order, on the committing
// goroutine. Keep them short and non-blocking (wake a worker, signal a
// channel); anything slow belongs in a task.
func AfterCommit(ctx context.Context, hook func(context.Context)) {
	if st := stateFrom(ctx); st != nil {
		st.afterCommit = append(st.afterCommit, hook)
		return
	}
	hook(ctx)
}
