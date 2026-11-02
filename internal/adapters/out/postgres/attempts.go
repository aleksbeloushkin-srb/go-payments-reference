package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
	"github.com/aleksbeloushkin-srb/go-payments-reference/pkg/txctx"
)

// Attempts stores gateway attempts of the two-phase start.
type Attempts struct {
	tx *txctx.Manager
}

// NewAttempts returns an attempts store.
func NewAttempts(tx *txctx.Manager) *Attempts {
	return &Attempts{tx: tx}
}

const attemptColumns = `payment_id, attempt, idempotency_key, state, coalesce(gateway_ref, ''), not_found, created_at, updated_at`

func scanAttempt(row pgx.Row) (ports.Attempt, error) {
	var (
		a     ports.Attempt
		state string
	)
	err := row.Scan(&a.PaymentID, &a.Number, &a.IdempotencyKey, &state, &a.GatewayRef, &a.NotFound, &a.CreatedAt, &a.UpdatedAt)
	a.State = ports.AttemptState(state)
	return a, err
}

// requireTx refuses writes outside a transaction: they must happen under
// the payment's row lock.
func requireTx(ctx context.Context, op string) error {
	if !txctx.InTx(ctx) {
		return fmt.Errorf("postgres: %s requires a transaction", op)
	}
	return nil
}

// Open returns the payment's open attempt, if any. A partial unique index
// guarantees there is at most one.
func (s *Attempts) Open(ctx context.Context, paymentID uuid.UUID) (ports.Attempt, bool, error) {
	if err := requireTx(ctx, "Attempts.Open"); err != nil {
		return ports.Attempt{}, false, err
	}
	row := s.tx.DB(ctx).QueryRow(ctx, `
		SELECT `+attemptColumns+` FROM gateway_attempts
		WHERE payment_id = $1 AND state IN ('intent', 'unknown')`, paymentID)
	a, err := scanAttempt(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return ports.Attempt{}, false, nil
	}
	if err != nil {
		return ports.Attempt{}, false, fmt.Errorf("select open attempt: %w", err)
	}
	return a, true, nil
}

// Get returns attempt n of the payment.
func (s *Attempts) Get(ctx context.Context, paymentID uuid.UUID, n int) (ports.Attempt, error) {
	row := s.tx.DB(ctx).QueryRow(ctx, `
		SELECT `+attemptColumns+` FROM gateway_attempts
		WHERE payment_id = $1 AND attempt = $2`, paymentID, n)
	a, err := scanAttempt(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return ports.Attempt{}, ports.ErrAttemptNotFound
	}
	if err != nil {
		return ports.Attempt{}, fmt.Errorf("select attempt: %w", err)
	}
	return a, nil
}

// Create inserts the next attempt as intent, numbered after the last one.
func (s *Attempts) Create(ctx context.Context, paymentID uuid.UUID, now time.Time) (ports.Attempt, error) {
	if err := requireTx(ctx, "Attempts.Create"); err != nil {
		return ports.Attempt{}, err
	}
	db := s.tx.DB(ctx)
	var last int
	if err := db.QueryRow(ctx, `SELECT coalesce(max(attempt), 0) FROM gateway_attempts WHERE payment_id = $1`,
		paymentID).Scan(&last); err != nil {
		return ports.Attempt{}, fmt.Errorf("select last attempt: %w", err)
	}
	n := last + 1
	row := db.QueryRow(ctx, `
		INSERT INTO gateway_attempts (payment_id, attempt, idempotency_key, state, created_at, updated_at)
		VALUES ($1, $2, $3, 'intent', $4, $4)
		RETURNING `+attemptColumns,
		paymentID, n, ports.AttemptKey(paymentID, n), now)
	a, err := scanAttempt(row)
	if err != nil {
		return ports.Attempt{}, fmt.Errorf("insert attempt: %w", err)
	}
	return a, nil
}

// Update writes a's state, reference, not-found count and update time if
// the stored state is still from.
func (s *Attempts) Update(ctx context.Context, a ports.Attempt, from ports.AttemptState) error {
	if err := requireTx(ctx, "Attempts.Update"); err != nil {
		return err
	}
	tag, err := s.tx.DB(ctx).Exec(ctx, `
		UPDATE gateway_attempts
		SET state = $3, gateway_ref = $4, not_found = $5, updated_at = $6
		WHERE payment_id = $1 AND attempt = $2 AND state = $7`,
		a.PaymentID, a.Number, string(a.State), nullable(a.GatewayRef), a.NotFound, a.UpdatedAt, string(from))
	if err != nil {
		return fmt.Errorf("update attempt: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: attempt %d of %s is no longer %s", ports.ErrAttemptChanged, a.Number, a.PaymentID, from)
	}
	return nil
}

// Latest returns the payment's attempt with the highest number, if any.
func (s *Attempts) Latest(ctx context.Context, paymentID uuid.UUID) (ports.Attempt, bool, error) {
	row := s.tx.DB(ctx).QueryRow(ctx, `
		SELECT `+attemptColumns+` FROM gateway_attempts
		WHERE payment_id = $1 ORDER BY attempt DESC LIMIT 1`, paymentID)
	a, err := scanAttempt(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return ports.Attempt{}, false, nil
	}
	if err != nil {
		return ports.Attempt{}, false, fmt.Errorf("select latest attempt: %w", err)
	}
	return a, true, nil
}
