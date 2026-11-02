// Command payments is the Pinkponies Bank payment service: HTTP API, long
// polling, task worker and outbox relay in one binary. PAYMENTS_ROLES picks
// which of them run in this process.
//
// main is the composition root. It is the only place that knows every
// concrete type: it reads the configuration, builds the adapters, injects
// them into the use cases, starts the components and stops them in the
// right order. Read it top to bottom.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/adapters/in/httpapi"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/adapters/in/outbox"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/adapters/in/queue"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/adapters/out/gateway"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/adapters/out/metrics"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/adapters/out/postgres"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/adapters/out/publisher"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/app"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/longpoll"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/platform"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
	"github.com/aleksbeloushkin-srb/go-payments-reference/migrations"
	"github.com/aleksbeloushkin-srb/go-payments-reference/pkg/txctx"
	"github.com/aleksbeloushkin-srb/go-payments-reference/web"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}

	// Catch signals before anything else, so a SIGTERM during start-up
	// still goes through the orderly path.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := platform.LoadConfig(os.Environ())
	if err != nil {
		fmt.Fprintln(os.Stderr, "payments: config:", err)
		os.Exit(1)
	}
	log := platform.NewLogger(os.Stdout, cfg.LogLevel)

	if err := run(ctx, stop, cfg, log); err != nil {
		log.Error("payments stopped with an error", "err", err)
		os.Exit(1)
	}
	log.Info("payments stopped")
}

// run builds the service, serves until ctx is cancelled (or a component
// fails), and then shuts down in order:
//
//  1. readiness turns false, so the load balancer stops sending traffic, and
//     the worker stops claiming new tasks (running ones continue);
//  2. a short pause lets the readiness change propagate;
//  3. the HTTP server drains in-flight requests (long polls are released
//     at once by closing the waiter registry);
//  4. background components stop: the worker finishes its running tasks;
//  5. the database pool closes (deferred, after everything that uses it).
//
// After the first signal the default handler is restored, so a second
// signal kills the process immediately.
func run(ctx context.Context, stopSignals context.CancelFunc, cfg platform.Config, log *slog.Logger) error {
	pool, err := postgres.Open(ctx, postgres.Config{
		URL:              cfg.DatabaseURL,
		MaxConns:         int32(cfg.DBMaxConns),
		StatementTimeout: cfg.DBStatementTimeout,
		LockTimeout:      cfg.DBLockTimeout,
		IdleInTxTimeout:  cfg.DBIdleInTxTimeout,
	})
	if err != nil {
		return err
	}
	defer pool.Close()

	results, err := postgres.Migrate(ctx, pool, migrations.FS)
	if err != nil {
		return err
	}
	for _, r := range results {
		log.Info("migration applied", "version", r.Source.Version, "path", r.Source.Path)
	}

	// Metrics live on a registry owned by this function and injected into
	// every component that records them; nothing uses the global default.
	reg := prometheus.NewRegistry()

	// Adapters.
	clock := platform.SystemClock{}
	tx := txctx.New(pool)
	payments := postgres.NewPayments(tx)
	tasks := postgres.NewTasks(tx)
	events := postgres.NewOutbox(tx)
	gw, err := newGateway(cfg)
	if err != nil {
		return err
	}
	waiters := longpoll.NewRegistry(cfg.WaitMaxWaiters)

	// Use cases.
	svc := app.NewService(app.Deps{
		Tx:       tx,
		Payments: payments,
		Tasks:    tasks,
		Events:   events,
		Gateway:  gw,
		Watcher:  waiters,
		Clock:    clock,
		Metrics:  metrics.NewBusiness(reg),
		NewID:    uuid.NewV7,
		Logger:   log,
	}, app.Config{
		PollInterval:    cfg.PollInterval,
		PollTimeout:     cfg.PollTimeout,
		WaitTimeout:     cfg.WaitTimeout,
		WaitReadTimeout: cfg.WaitReadTimeout,
	})

	// Background components. Their context is independent of the signal
	// context: they must keep running while HTTP drains.
	bg := platform.NewGroup(log)
	if cfg.Roles.API {
		batcher := longpoll.NewBatcher(waiters, payments, longpoll.BatcherConfig{
			Interval:     cfg.WaitBatchInterval,
			QueryTimeout: cfg.WaitReadTimeout,
		}, log)
		bg.Go("longpoll-batcher", batcher.Run)
	}
	stopClaiming := func() {}
	if cfg.Roles.Worker {
		w := queue.NewWorker(tasks, svc, clock, tasks.Ready(), log, queue.Config{
			Concurrency:  cfg.WorkerConcurrency,
			IdleInterval: cfg.WorkerIdleInterval,
			Lease:        cfg.TaskLease,
			TaskTimeout:  cfg.TaskTimeout,
			MaxAttempts:  cfg.TaskMaxAttempts,
			Backoff:      queue.Backoff{Base: cfg.TaskRetryBase, Max: cfg.TaskRetryMax},
		})
		bg.Go("worker", w.Run)
		stopClaiming = w.StopClaiming
	}
	if cfg.Roles.Relay {
		relay := outbox.NewRelay(events, newPublisher(cfg, log), clock, events.Ready(), outbox.Config{
			Interval:       cfg.RelayInterval,
			Batch:          cfg.RelayBatch,
			Lease:          cfg.RelayLease,
			PublishTimeout: cfg.EventsWebhookTimeout,
		}, log)
		bg.Go("relay", relay.Run)
	}

	// HTTP.
	var ready platform.Readiness
	var srv *http.Server
	serveErr := make(chan error, 1)
	if cfg.Roles.API {
		handler := httpapi.NewHandler(httpapi.Deps{
			Creator:       svc,
			Reader:        svc,
			Events:        svc,
			Ready:         ready.Ready,
			WebhookSecret: []byte(cfg.WebhookSecret),
			Static:        web.FS,
			Clock:         clock,
			Logger:        log,
		})
		srv = httpapi.NewServer(httpapi.ServerConfig{
			Addr:              cfg.HTTPAddr,
			ReadHeaderTimeout: cfg.HTTPReadHeaderTimeout,
			ReadTimeout:       cfg.HTTPReadTimeout,
			WriteTimeout:      cfg.HTTPWriteTimeout,
			IdleTimeout:       cfg.HTTPIdleTimeout,
		}, handler, log)
		srv.RegisterOnShutdown(waiters.Close)

		// Listen synchronously so a busy port fails the start instead of a
		// goroutine later.
		ln, err := net.Listen("tcp", cfg.HTTPAddr)
		if err != nil {
			return errors.Join(fmt.Errorf("listen: %w", err), bg.Stop())
		}
		go func() { serveErr <- srv.Serve(ln) }()
	}
	ready.Set(true)
	log.Info("payments started", "roles", cfg.Roles, "addr", cfg.HTTPAddr)

	var runErr error
	select {
	case <-ctx.Done():
		log.Info("shutdown requested", "cause", context.Cause(ctx))
	case err := <-serveErr:
		runErr = fmt.Errorf("http server: %w", err)
	case <-bg.Done():
		runErr = errors.New("a background component stopped")
	}
	stopSignals()

	return errors.Join(runErr, shutdown(ctx, cfg, log, &ready, srv, serveErr, bg, stopClaiming))
}

func shutdown(ctx context.Context, cfg platform.Config, log *slog.Logger, ready *platform.Readiness,
	srv *http.Server, serveErr <-chan error, bg *platform.Group, stopClaiming func()) error {
	var errs []error
	ready.Set(false)
	// No new tasks from here on; running ones finish while HTTP drains.
	stopClaiming()
	if srv != nil {
		log.Info("draining: readiness is off", "pause", cfg.ShutdownDrainDelay)
		pause := time.NewTimer(cfg.ShutdownDrainDelay)
		<-pause.C

		// A fresh deadline: ctx is already cancelled, and Shutdown with a
		// cancelled context would return at once without draining.
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(sctx); err != nil {
			errs = append(errs, fmt.Errorf("http shutdown: %w", err))
			_ = srv.Close()
		}
		if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs = append(errs, fmt.Errorf("http server: %w", err))
		}
		log.Info("http server stopped")
	}
	if err := bg.Stop(); err != nil {
		errs = append(errs, err)
	}
	log.Info("background components stopped")
	// The deferred pool.Close in run comes next. slog writes synchronously
	// to stdout, so there is no log buffer to flush; with an asynchronous
	// handler, flushing it would be the very last step.
	return errors.Join(errs...)
}

// newGateway builds the gateway client. Only the worker role calls the
// gateway, so a process without it may run without a gateway URL.
func newGateway(cfg platform.Config) (ports.Gateway, error) {
	if !cfg.Roles.Worker && cfg.GatewayURL == "" {
		return noGateway{}, nil
	}
	c, err := gateway.NewClient(gateway.Config{
		BaseURL:       cfg.GatewayURL,
		CallTimeout:   cfg.GatewayTimeout,
		MaxRetries:    cfg.GatewayMaxRetries,
		MaxRetryAfter: cfg.GatewayMaxRetryAfter,
	}, platform.NewHTTPClient())
	if err != nil {
		// Return a nil interface, not an interface holding a nil *Client.
		return nil, err
	}
	return c, nil
}

// noGateway stands in for the gateway in processes without the worker role.
// Task handlers, the only callers, never run there.
type noGateway struct{}

var errNoGateway = fmt.Errorf("%w: this process has no worker role", ports.ErrGatewayUnavailable)

func (noGateway) CreatePayment(context.Context, ports.GatewayRequest) (ports.GatewayPayment, error) {
	return ports.GatewayPayment{}, errNoGateway
}

func (noGateway) Payment(context.Context, string) (ports.GatewayPayment, error) {
	return ports.GatewayPayment{}, errNoGateway
}

func newPublisher(cfg platform.Config, log *slog.Logger) outbox.Publisher {
	logPub := publisher.NewLog(log)
	if cfg.EventsWebhookURL == "" {
		return logPub
	}
	return outbox.Fanout{logPub, publisher.NewWebhook(cfg.EventsWebhookURL, platform.NewHTTPClient())}
}

// healthcheck probes /readyz. Distroless images have no curl, so the
// container health check runs the binary itself.
func healthcheck() int {
	addr := os.Getenv("PAYMENTS_HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	port := addr[strings.LastIndexByte(addr, ':')+1:]
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/readyz")
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
