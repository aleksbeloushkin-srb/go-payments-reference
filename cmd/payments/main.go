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
	"github.com/prometheus/client_golang/prometheus/collectors"

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

// metricsQueryTimeout bounds the database query behind the queue gauges,
// well below the default Prometheus scrape timeout of 10 s.
const metricsQueryTimeout = 2 * time.Second

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
//  3. the API server drains in-flight requests (long polls are released
//     at once by closing the waiter registry);
//  4. background components stop: the worker finishes its running tasks;
//  5. the admin listener (metrics, probes) stops;
//  6. the database pool closes (deferred, after everything that uses it).
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

	// Process-wide collectors: Go runtime, process, the pool, and the queue
	// gauges read from the database on each scrape.
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		postgres.NewPoolCollector(pool),
		postgres.NewQueueCollector(pool, metricsQueryTimeout, log),
	)

	// Adapters.
	clock := platform.SystemClock{}
	tx := txctx.New(pool)
	payments := postgres.NewPayments(tx)
	tasks := postgres.NewTasks(tx)
	events := postgres.NewOutbox(tx)
	gw, err := newGateway(cfg, gateway.NewMetrics(reg))
	if err != nil {
		return err
	}
	longpollMetrics := longpoll.NewMetrics(reg)
	waiters := longpoll.NewRegistry(cfg.WaitMaxWaiters, longpollMetrics)

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
		batcher := longpoll.NewBatcher(waiters, payments, longpollMetrics, longpoll.BatcherConfig{
			Interval:     cfg.WaitBatchInterval,
			QueryTimeout: cfg.WaitReadTimeout,
		}, log)
		bg.Go("longpoll-batcher", batcher.Run)
	}
	stopClaiming := func() {}
	if cfg.Roles.Worker {
		w := queue.NewWorker(tasks, svc, clock, tasks.Ready(), queue.NewMetrics(reg), log, queue.Config{
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
		relay := outbox.NewRelay(events, newPublisher(cfg, log), clock, events.Ready(), outbox.NewMetrics(reg), outbox.Config{
			Interval:       cfg.RelayInterval,
			Batch:          cfg.RelayBatch,
			Lease:          cfg.RelayLease,
			PublishTimeout: cfg.EventsWebhookTimeout,
		}, log)
		bg.Go("relay", relay.Run)
	}

	// HTTP. The admin listener (metrics and probes) runs in every role; the
	// API only with the api role. Both listen synchronously, so a busy port
	// fails the start instead of a goroutine later.
	var ready platform.Readiness
	admin, err := startServer("admin", httpapi.NewServer(adminServerConfig(cfg.AdminAddr),
		httpapi.NewAdminHandler(reg, ready.Ready, log), log))
	if err != nil {
		return errors.Join(err, bg.Stop())
	}
	var api *server
	if cfg.Roles.API {
		handler := httpapi.NewHandler(httpapi.Deps{
			Creator:       svc,
			Reader:        svc,
			Events:        svc,
			WebhookSecret: []byte(cfg.WebhookSecret),
			Static:        web.FS,
			Clock:         clock,
			Metrics:       httpapi.NewMetrics(reg),
			Logger:        log,
		})
		srv := httpapi.NewServer(httpapi.ServerConfig{
			Addr:              cfg.HTTPAddr,
			ReadHeaderTimeout: cfg.HTTPReadHeaderTimeout,
			ReadTimeout:       cfg.HTTPReadTimeout,
			WriteTimeout:      cfg.HTTPWriteTimeout,
			IdleTimeout:       cfg.HTTPIdleTimeout,
		}, handler, log)
		srv.RegisterOnShutdown(waiters.Close)
		if api, err = startServer("api", srv); err != nil {
			return errors.Join(err, admin.stop(ctx, adminShutdownTimeout), bg.Stop())
		}
	}
	ready.Set(true)
	log.Info("payments started", "roles", cfg.Roles, "addr", cfg.HTTPAddr, "admin_addr", cfg.AdminAddr)

	var apiDone <-chan struct{} // nil without the api role: never ready
	if api != nil {
		apiDone = api.done
	}
	var runErr error
	select {
	case <-ctx.Done():
		log.Info("shutdown requested", "cause", context.Cause(ctx))
	case <-apiDone:
		runErr = fmt.Errorf("api server stopped: %w", api.err)
	case <-admin.done:
		runErr = fmt.Errorf("admin server stopped: %w", admin.err)
	case <-bg.Done():
		runErr = errors.New("a background component stopped")
	}
	stopSignals()

	return errors.Join(runErr, shutdown(ctx, cfg, log, &ready, api, admin, bg, stopClaiming))
}

func shutdown(ctx context.Context, cfg platform.Config, log *slog.Logger, ready *platform.Readiness,
	api, admin *server, bg *platform.Group, stopClaiming func()) error {
	var errs []error
	ready.Set(false)
	// No new tasks from here on; running ones finish while HTTP drains.
	stopClaiming()
	if api != nil {
		log.Info("draining: readiness is off", "pause", cfg.ShutdownDrainDelay)
		pause := time.NewTimer(cfg.ShutdownDrainDelay)
		<-pause.C
		if err := api.stop(ctx, cfg.ShutdownTimeout); err != nil {
			errs = append(errs, err)
		}
		log.Info("api server stopped")
	}
	if err := bg.Stop(); err != nil {
		errs = append(errs, err)
	}
	log.Info("background components stopped")
	// The admin listener goes last: probes and metrics stay available while
	// everything else drains.
	if err := admin.stop(ctx, adminShutdownTimeout); err != nil {
		errs = append(errs, err)
	}
	// The deferred pool.Close in run comes next. slog writes synchronously
	// to stdout, so there is no log buffer to flush; with an asynchronous
	// handler, flushing it would be the very last step.
	return errors.Join(errs...)
}

// adminShutdownTimeout bounds the admin listener's drain: a scrape or a
// probe takes milliseconds.
const adminShutdownTimeout = 5 * time.Second

func adminServerConfig(addr string) httpapi.ServerConfig {
	return httpapi.ServerConfig{
		Addr:              addr,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// server is an http.Server whose Serve runs in its own goroutine.
type server struct {
	srv  *http.Server
	name string
	done chan struct{} // closed when Serve returns
	err  error         // Serve's result; read it only after done is closed
}

// startServer listens on srv.Addr and serves in a new goroutine owned by the
// returned server; stop waits for it.
func startServer(name string, srv *http.Server) (*server, error) {
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return nil, fmt.Errorf("%s listen: %w", name, err)
	}
	s := &server{srv: srv, name: name, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		s.err = s.srv.Serve(ln)
	}()
	return s, nil
}

// stop drains the server within timeout, closes it if the drain does not
// finish, and waits until Serve has returned. The deadline is fresh: the
// signal context is already cancelled, and Shutdown with a cancelled context
// would return at once without draining.
func (s *server) stop(ctx context.Context, timeout time.Duration) error {
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	var errs []error
	if err := s.srv.Shutdown(sctx); err != nil {
		errs = append(errs, fmt.Errorf("%s shutdown: %w", s.name, err))
		_ = s.srv.Close()
	}
	<-s.done
	if s.err != nil && !errors.Is(s.err, http.ErrServerClosed) {
		errs = append(errs, fmt.Errorf("%s server: %w", s.name, s.err))
	}
	return errors.Join(errs...)
}

// newGateway builds the gateway client. Only the worker role calls the
// gateway, so a process without it may run without a gateway URL.
func newGateway(cfg platform.Config, m *gateway.Metrics) (ports.Gateway, error) {
	if !cfg.Roles.Worker && cfg.GatewayURL == "" {
		return noGateway{}, nil
	}
	c, err := gateway.NewClient(gateway.Config{
		BaseURL:       cfg.GatewayURL,
		CallTimeout:   cfg.GatewayTimeout,
		MaxRetries:    cfg.GatewayMaxRetries,
		MaxRetryAfter: cfg.GatewayMaxRetryAfter,
	}, platform.NewHTTPClient(), m)
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

func (noGateway) FindByReference(context.Context, string) ([]ports.GatewayPayment, error) {
	return nil, errNoGateway
}

func newPublisher(cfg platform.Config, log *slog.Logger) outbox.Publisher {
	logPub := publisher.NewLog(log)
	if cfg.EventsWebhookURL == "" {
		return logPub
	}
	return outbox.Fanout{logPub, publisher.NewWebhook(cfg.EventsWebhookURL, platform.NewHTTPClient())}
}

// healthcheck probes /readyz on the admin listener. Distroless images have
// no curl, so the container health check runs the binary itself.
func healthcheck() int {
	addr := os.Getenv("PAYMENTS_ADMIN_ADDR")
	if addr == "" {
		addr = ":9100"
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
