package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/pkg/txctx"
)

// Payments stores payments.
type Payments struct {
	tx *txctx.Manager
}

// NewPayments returns a Payments store.
func NewPayments(tx *txctx.Manager) *Payments {
	return &Payments{tx: tx}
}

const paymentColumns = `id, merchant_id, amount_minor, currency, status, version,
	gateway_ref, idempotency_key, request_hash, created_at, updated_at`

func scanPayment(row pgx.Row) (payment.Payment, error) {
	var (
		p          payment.Payment
		currency   string
		status     string
		gatewayRef *string
	)
	err := row.Scan(&p.ID, &p.MerchantID, &p.Amount.AmountMinor, &currency, &status, &p.Version,
		&gatewayRef, &p.IdempotencyKey, &p.RequestHash, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return payment.Payment{}, err
	}
	p.Amount.Currency = payment.Currency(currency)
	p.Status = payment.Status(status)
	if gatewayRef != nil {
		p.GatewayRef = *gatewayRef
	}
	return p, nil
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// Insert stores p unless its idempotency key is taken, and returns the
// stored row either way.
//
// ON CONFLICT DO NOTHING waits for a concurrent insert of the same key to
// finish, so under READ COMMITTED the follow-up SELECT, which takes a fresh
// snapshot, sees the winner. Catching a unique violation instead would abort
// the surrounding transaction.
func (s *Payments) Insert(ctx context.Context, p payment.Payment) (payment.Payment, bool, error) {
	db := s.tx.DB(ctx)
	row := db.QueryRow(ctx, `
		INSERT INTO payments (`+paymentColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+paymentColumns,
		p.ID, p.MerchantID, p.Amount.AmountMinor, string(p.Amount.Currency), string(p.Status), p.Version,
		nullable(p.GatewayRef), p.IdempotencyKey, p.RequestHash, p.CreatedAt, p.UpdatedAt)
	stored, err := scanPayment(row)
	if err == nil {
		return stored, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return payment.Payment{}, false, fmt.Errorf("insert payment: %w", err)
	}

	row = db.QueryRow(ctx, `SELECT `+paymentColumns+` FROM payments WHERE idempotency_key = $1`, p.IdempotencyKey)
	stored, err = scanPayment(row)
	if err != nil {
		return payment.Payment{}, false, fmt.Errorf("read payment by idempotency key: %w", err)
	}
	return stored, false, nil
}

// Payment returns the payment with the given id.
func (s *Payments) Payment(ctx context.Context, id uuid.UUID) (payment.Payment, error) {
	row := s.tx.DB(ctx).QueryRow(ctx, `SELECT `+paymentColumns+` FROM payments WHERE id = $1`, id)
	return notFound(scanPayment(row))
}

// Lock reads the payment with SELECT ... FOR UPDATE. The row stays locked
// until the transaction in ctx ends, so concurrent handlers of the same
// payment run one after another.
func (s *Payments) Lock(ctx context.Context, id uuid.UUID) (payment.Payment, error) {
	if !txctx.InTx(ctx) {
		return payment.Payment{}, errors.New("postgres: Lock requires a transaction")
	}
	row := s.tx.DB(ctx).QueryRow(ctx, `SELECT `+paymentColumns+` FROM payments WHERE id = $1 FOR UPDATE`, id)
	return notFound(scanPayment(row))
}

func notFound(p payment.Payment, err error) (payment.Payment, error) {
	if errors.Is(err, pgx.ErrNoRows) {
		return payment.Payment{}, payment.ErrNotFound
	}
	if err != nil {
		return payment.Payment{}, fmt.Errorf("select payment: %w", err)
	}
	return p, nil
}

// Update writes status, version and gateway reference if the stored version
// still equals expectedVersion. The row lock already gives exclusivity; the
// version check turns a missing lock (a bug) into an error instead of a lost
// update.
func (s *Payments) Update(ctx context.Context, p payment.Payment, expectedVersion int64) error {
	tag, err := s.tx.DB(ctx).Exec(ctx, `
		UPDATE payments
		SET status = $2, version = $3, gateway_ref = $4, updated_at = $5
		WHERE id = $1 AND version = $6`,
		p.ID, string(p.Status), p.Version, nullable(p.GatewayRef), p.UpdatedAt, expectedVersion)
	if err != nil {
		return fmt.Errorf("update payment: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return payment.ErrVersionConflict
	}
	return nil
}

// Versions returns the current version of each existing id in one query.
// The long-poll batcher calls it for all awaited payments at once.
func (s *Payments) Versions(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]int64, error) {
	rows, err := s.tx.DB(ctx).Query(ctx, `SELECT id, version FROM payments WHERE id = ANY($1)`, ids)
	if err != nil {
		return nil, fmt.Errorf("read versions: %w", err)
	}
	defer rows.Close()
	out := make(map[uuid.UUID]int64, len(ids))
	for rows.Next() {
		var (
			id      uuid.UUID
			version int64
		)
		if err := rows.Scan(&id, &version); err != nil {
			return nil, fmt.Errorf("scan version: %w", err)
		}
		out[id] = version
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read versions: %w", err)
	}
	return out, nil
}
