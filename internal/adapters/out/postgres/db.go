package postgres

import (
	"context"
	"fmt"
	"io/fs"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// Config tunes the connection pool and the server-side safety limits.
type Config struct {
	URL      string
	MaxConns int32
	// StatementTimeout aborts any single statement that runs longer.
	StatementTimeout time.Duration
	// LockTimeout aborts a statement that waits longer for a row lock.
	LockTimeout time.Duration
	// IdleInTxTimeout closes a session that sits idle inside an open
	// transaction, so a stuck client cannot hold locks forever.
	IdleInTxTimeout time.Duration
}

// pingTimeout bounds the start-up connectivity check.
const pingTimeout = 5 * time.Second

// Open creates the pool and checks connectivity. pgxpool connects lazily, so
// without the ping a wrong URL would only surface on the first request.
func Open(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse url: %w", err)
	}
	pc.MaxConns = cfg.MaxConns
	params := pc.ConnConfig.RuntimeParams
	params["application_name"] = "payments"
	params["statement_timeout"] = millis(cfg.StatementTimeout)
	params["lock_timeout"] = millis(cfg.LockTimeout)
	params["idle_in_transaction_session_timeout"] = millis(cfg.IdleInTxTimeout)

	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("postgres: create pool: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := pool.Ping(pctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return pool, nil
}

func millis(d time.Duration) string {
	return strconv.FormatInt(d.Milliseconds(), 10)
}

// Migrate applies pending migrations from fsys. A session-level advisory
// lock serializes concurrent starts of several instances.
func Migrate(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS) ([]*goose.MigrationResult, error) {
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()

	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return nil, fmt.Errorf("postgres: migration lock: %w", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, fsys, goose.WithSessionLocker(locker))
	if err != nil {
		return nil, fmt.Errorf("postgres: migrations: %w", err)
	}
	results, err := provider.Up(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: migrate up: %w", err)
	}
	return results, nil
}
