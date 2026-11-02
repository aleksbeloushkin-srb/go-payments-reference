//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/adapters/out/metrics"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/adapters/out/postgres"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/app"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/longpoll"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
	"github.com/aleksbeloushkin-srb/go-payments-reference/migrations"
	"github.com/aleksbeloushkin-srb/go-payments-reference/pkg/txctx"
)

// pool is shared by all tests; each test truncates the tables first, so the
// tests must not run in parallel.
var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("payments"),
		tcpostgres.WithUsername("payments"),
		tcpostgres.WithPassword("payments"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres:", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(ctr) }()

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintln(os.Stderr, "connection string:", err)
		return 1
	}
	pool, err = postgres.Open(ctx, postgres.Config{
		URL:              dsn,
		MaxConns:         20,
		StatementTimeout: 10 * time.Second,
		LockTimeout:      5 * time.Second,
		IdleInTxTimeout:  30 * time.Second,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "open pool:", err)
		return 1
	}
	defer pool.Close()
	if _, err := postgres.Migrate(ctx, pool, migrations.FS); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		return 1
	}
	return m.Run()
}

// env is a fully wired service on the shared database with a fake gateway.
type env struct {
	tx       *txctx.Manager
	payments *postgres.Payments
	tasks    *postgres.Tasks
	outbox   *postgres.Outbox
	gateway  *fakeGateway
	waiters  *longpoll.Registry
	clock    *clock
	svc      *app.Service
}

func newEnv(t *testing.T, cfg app.Config) *env {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE payments, tasks, outbox RESTART IDENTITY`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	tx := txctx.New(pool)
	e := &env{
		tx:       tx,
		payments: postgres.NewPayments(tx),
		tasks:    postgres.NewTasks(tx),
		outbox:   postgres.NewOutbox(tx),
		gateway:  &fakeGateway{},
		waiters:  longpoll.NewRegistry(100, longpoll.NewMetrics(prometheus.NewRegistry())),
		clock:    &clock{now: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)},
	}
	if cfg == (app.Config{}) {
		cfg = app.Config{PollInterval: 3 * time.Second, PollTimeout: time.Hour, WaitTimeout: 5 * time.Second, WaitReadTimeout: 2 * time.Second}
	}
	e.svc = app.NewService(app.Deps{
		Tx:       tx,
		Payments: e.payments,
		Tasks:    e.tasks,
		Events:   e.outbox,
		Gateway:  e.gateway,
		Watcher:  e.waiters,
		Clock:    e.clock,
		Metrics:  metrics.NewBusiness(prometheus.NewRegistry()),
		NewID:    uuid.NewV7,
		Logger:   discard(),
	}, cfg)
	return e
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// clock is a settable clock shared by the service and the worker.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// fakeGateway answers every create with a configurable result.
type fakeGateway struct {
	mu      sync.Mutex
	create  ports.GatewayPayment
	err     error
	creates int
}

func (g *fakeGateway) set(res ports.GatewayPayment, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.create, g.err = res, err
}

func (g *fakeGateway) CreatePayment(context.Context, ports.GatewayRequest) (ports.GatewayPayment, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.creates++
	return g.create, g.err
}

func (g *fakeGateway) Payment(context.Context, string) (ports.GatewayPayment, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.create, g.err
}

func count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func createPayment(t *testing.T, e *env, key string, amount int64) app.CreatePaymentResult {
	t.Helper()
	res, err := e.svc.CreatePayment(context.Background(), app.CreatePaymentRequest{
		IdempotencyKey: key, MerchantID: "merchant-1", AmountMinor: amount, Currency: "EUR",
	})
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	return res
}

func newV7() (uuid.UUID, error) { return uuid.NewV7() }
