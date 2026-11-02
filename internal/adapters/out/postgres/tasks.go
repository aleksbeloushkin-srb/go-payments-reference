package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
	"github.com/aleksbeloushkin-srb/go-payments-reference/pkg/txctx"
)

// maxErrorLen truncates last_error so a huge error message cannot bloat the
// task table.
const maxErrorLen = 1000

// Tasks is the PostgreSQL task queue.
//
// Claiming takes a short row lock (FOR UPDATE SKIP LOCKED) only for the
// duration of one UPDATE statement and leaves a lease (lease_until) behind.
// The lease, not a held lock or an open transaction, keeps other workers
// away while the handler runs; a crashed worker's tasks become claimable
// when the lease expires. lease_until doubles as a fencing token: every
// settle statement matches on it, so a worker whose lease was taken over
// cannot overwrite the new owner's result.
type Tasks struct {
	tx    *txctx.Manager
	ready chan struct{}
}

// NewTasks returns a task queue.
func NewTasks(tx *txctx.Manager) *Tasks {
	return &Tasks{tx: tx, ready: make(chan struct{}, 1)}
}

// Ready receives a value after a transaction that enqueued a task commits.
// The local worker listens to it to start the task without waiting for its
// next poll. Workers in other processes still find the task by polling.
func (q *Tasks) Ready() <-chan struct{} {
	return q.ready
}

func (q *Tasks) signal(context.Context) {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

// Enqueue inserts t unless a task with the same kind, payment id and dedup
// key exists.
func (q *Tasks) Enqueue(ctx context.Context, t ports.TaskSpec) (bool, error) {
	payload := t.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	tag, err := q.tx.DB(ctx).Exec(ctx, `
		INSERT INTO tasks (kind, payment_id, dedup_key, payload, run_after)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (kind, payment_id, dedup_key) DO NOTHING`,
		string(t.Kind), t.PaymentID, t.DedupKey, payload, t.RunAfter)
	if err != nil {
		return false, fmt.Errorf("insert task: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	txctx.AfterCommit(ctx, q.signal)
	return true, nil
}

// Claim leases up to limit ready tasks whose run_after has come and whose
// previous lease, if any, has expired. Concurrent claimers skip each other's
// locked rows instead of waiting, so N workers never receive the same task.
//
// now comes from the caller's clock rather than the database's now(): it
// makes lease expiry testable without sleeping. The price is that instance
// clocks must agree to well within a lease, which NTP gives in practice.
func (q *Tasks) Claim(ctx context.Context, now time.Time, limit int, lease time.Duration) ([]ports.Task, error) {
	rows, err := q.tx.DB(ctx).Query(ctx, `
		WITH batch AS MATERIALIZED (
			SELECT id FROM tasks
			WHERE state = 'ready'
			  AND run_after <= $1
			  AND (lease_until IS NULL OR lease_until <= $1)
			ORDER BY run_after, id
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)
		UPDATE tasks t
		SET lease_until = $2, updated_at = now()
		FROM batch
		WHERE t.id = batch.id
		RETURNING t.id, t.kind, t.payment_id, t.dedup_key, t.payload, t.attempts, t.run_after, t.lease_until`,
		now, now.Add(lease), limit)
	if err != nil {
		return nil, fmt.Errorf("claim tasks: %w", err)
	}
	defer rows.Close()

	var tasks []ports.Task
	for rows.Next() {
		var (
			t    ports.Task
			kind string
		)
		if err := rows.Scan(&t.ID, &kind, &t.PaymentID, &t.DedupKey, &t.Payload, &t.Attempts, &t.RunAfter, &t.LeaseUntil); err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
		}
		t.Kind = ports.TaskKind(kind)
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim tasks: %w", err)
	}
	return tasks, nil
}

// Complete marks the task done.
func (q *Tasks) Complete(ctx context.Context, t ports.Task) error {
	return q.settle(ctx, t, `
		UPDATE tasks SET state = 'done', lease_until = NULL, updated_at = now()
		WHERE id = $1 AND lease_until = $2 AND state = 'ready'`)
}

// Retry records a failed attempt and schedules the next one at runAfter.
func (q *Tasks) Retry(ctx context.Context, t ports.Task, runAfter time.Time, cause string) error {
	return q.settle(ctx, t, `
		UPDATE tasks
		SET attempts = attempts + 1, run_after = $3, last_error = $4, lease_until = NULL, updated_at = now()
		WHERE id = $1 AND lease_until = $2 AND state = 'ready'`,
		runAfter, truncate(cause))
}

// Postpone reschedules the task at runAfter without counting an attempt.
func (q *Tasks) Postpone(ctx context.Context, t ports.Task, runAfter time.Time) error {
	return q.settle(ctx, t, `
		UPDATE tasks SET run_after = $3, lease_until = NULL, updated_at = now()
		WHERE id = $1 AND lease_until = $2 AND state = 'ready'`,
		runAfter)
}

// Bury moves the task to the dead state. Dead tasks are never claimed again;
// they wait for an operator.
func (q *Tasks) Bury(ctx context.Context, t ports.Task, cause string) error {
	return q.settle(ctx, t, `
		UPDATE tasks
		SET state = 'dead', attempts = attempts + 1, last_error = $3, lease_until = NULL, updated_at = now()
		WHERE id = $1 AND lease_until = $2 AND state = 'ready'`,
		truncate(cause))
}

func (q *Tasks) settle(ctx context.Context, t ports.Task, sql string, args ...any) error {
	tag, err := q.tx.DB(ctx).Exec(ctx, sql, append([]any{t.ID, t.LeaseUntil}, args...)...)
	if err != nil {
		return fmt.Errorf("settle task %d: %w", t.ID, err)
	}
	return leaseHeld(tag)
}

func leaseHeld(tag pgconn.CommandTag) error {
	if tag.RowsAffected() == 0 {
		return ports.ErrLeaseLost
	}
	return nil
}

func truncate(s string) string {
	if len(s) <= maxErrorLen {
		return s
	}
	return s[:maxErrorLen]
}
