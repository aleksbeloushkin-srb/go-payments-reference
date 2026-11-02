package platform

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// envPrefix scopes the variables this service owns. Any unknown variable
// with the prefix is a start-up error, so a typo cannot silently fall back
// to a default.
const envPrefix = "PAYMENTS_"

// Roles selects which parts of the binary run in this process.
type Roles struct {
	API    bool // HTTP API, long polling, web UI
	Worker bool // task queue worker
	Relay  bool // outbox relay
}

// Config is the full configuration of cmd/payments.
//
// Every default below is a value chosen for this reference service, with
// the reason next to it. They are starting points to tune with metrics, not
// recommendations for any particular system.
type Config struct {
	LogLevel slog.Level
	Roles    Roles

	HTTPAddr              string
	AdminAddr             string
	HTTPReadHeaderTimeout time.Duration
	HTTPReadTimeout       time.Duration
	HTTPWriteTimeout      time.Duration
	HTTPIdleTimeout       time.Duration
	ShutdownDrainDelay    time.Duration
	ShutdownTimeout       time.Duration

	DatabaseURL        string
	DBMaxConns         int
	DBStatementTimeout time.Duration
	DBLockTimeout      time.Duration
	DBIdleInTxTimeout  time.Duration

	GatewayURL           string
	GatewayTimeout       time.Duration
	GatewayMaxRetries    int
	GatewayMaxRetryAfter time.Duration
	WebhookSecret        string

	WorkerConcurrency  int
	WorkerIdleInterval time.Duration
	TaskLease          time.Duration
	TaskTimeout        time.Duration
	TaskMaxAttempts    int
	TaskRetryBase      time.Duration
	TaskRetryMax       time.Duration

	PollInterval time.Duration
	PollTimeout  time.Duration

	StartMode              string
	ReconcileInterval      time.Duration
	ReconcileNotFoundLimit int

	WaitTimeout       time.Duration
	WaitReadTimeout   time.Duration
	WaitMaxWaiters    int
	WaitBatchInterval time.Duration

	RelayInterval        time.Duration
	RelayBatch           int
	RelayLease           time.Duration
	EventsWebhookURL     string
	EventsWebhookTimeout time.Duration
}

// LoadConfig reads the configuration from environ, given in os.Environ
// format. Unknown PAYMENTS_ variables, unparsable values and inconsistent
// combinations are all errors.
func LoadConfig(environ []string) (Config, error) {
	l := newLoader(environ)
	cfg := Config{
		LogLevel: l.level("LOG_LEVEL", slog.LevelInfo),
		// One process runs everything locally; production can scale the
		// roles independently.
		Roles: l.roles("ROLES", "api,worker,relay"),

		HTTPAddr: l.str("HTTP_ADDR", ":8080"),
		// Metrics and probes, for the cluster network only.
		AdminAddr: l.str("ADMIN_ADDR", ":9100"),
		// Slow-header clients cannot pin connections for long.
		HTTPReadHeaderTimeout: l.duration("HTTP_READ_HEADER_TIMEOUT", 5*time.Second),
		// Request bodies are a few hundred bytes.
		HTTPReadTimeout: l.duration("HTTP_READ_TIMEOUT", 10*time.Second),
		// The long-poll soft timeout, its two bounded reads and room to write.
		HTTPWriteTimeout: l.duration("HTTP_WRITE_TIMEOUT", 25*time.Second),
		HTTPIdleTimeout:  l.duration("HTTP_IDLE_TIMEOUT", 60*time.Second),
		// Time for a load balancer to notice the failing readiness probe.
		ShutdownDrainDelay: l.duration("SHUTDOWN_DRAIN_DELAY", 3*time.Second),
		// Upper bound for in-flight HTTP requests to finish; long polls are
		// released at once, so ordinary requests need far less.
		ShutdownTimeout: l.duration("SHUTDOWN_TIMEOUT", 15*time.Second),

		DatabaseURL: l.str("DATABASE_URL", ""),
		// Worker slots, API requests, the batcher and the relay, with room
		// to spare.
		DBMaxConns:         l.integer("DB_MAX_CONNS", 12),
		DBStatementTimeout: l.duration("DB_STATEMENT_TIMEOUT", 10*time.Second),
		// Lock waiters fail fast and come back through task retries.
		DBLockTimeout: l.duration("DB_LOCK_TIMEOUT", 5*time.Second),
		// Longer than a task, because the in_tx start idles in a transaction
		// while it calls the gateway.
		DBIdleInTxTimeout: l.duration("DB_IDLE_IN_TX_TIMEOUT", 45*time.Second),

		GatewayURL: l.str("GATEWAY_URL", ""),
		// Comfortably above a healthy gateway's answer time.
		GatewayTimeout: l.duration("GATEWAY_TIMEOUT", 4*time.Second),
		// In-line retries only for requests that were never executed.
		GatewayMaxRetries: l.integer("GATEWAY_MAX_RETRIES", 2),
		// Longer back-pressure is handled by task backoff, not by waiting.
		GatewayMaxRetryAfter: l.duration("GATEWAY_MAX_RETRY_AFTER", 5*time.Second),
		WebhookSecret:        l.str("WEBHOOK_SECRET", ""),

		// The in_tx start holds a connection during each gateway call, so
		// this stays well below DB_MAX_CONNS.
		WorkerConcurrency: l.integer("WORKER_CONCURRENCY", 4),
		// Commit hooks wake the local worker; this only bounds the delay for
		// tasks created by other instances.
		WorkerIdleInterval: l.duration("WORKER_IDLE_INTERVAL", time.Second),
		// Twice the task timeout: a live worker never loses its lease.
		TaskLease: l.duration("TASK_LEASE", 60*time.Second),
		// Covers the worst gateway budget: 3 calls x 4s and 2 waits x 5s.
		TaskTimeout: l.duration("TASK_TIMEOUT", 30*time.Second),
		// With the backoff below, about 4 to 8 minutes of retries ride out a
		// short gateway outage before a task goes dead.
		TaskMaxAttempts: l.integer("TASK_MAX_ATTEMPTS", 12),
		TaskRetryBase:   l.duration("TASK_RETRY_BASE", 500*time.Millisecond),
		TaskRetryMax:    l.duration("TASK_RETRY_MAX", 2*time.Minute),

		// Short enough for an interactive checkout, cheap for the gateway.
		PollInterval: l.duration("POLL_INTERVAL", 3*time.Second),
		// After this a non-final payment needs a human, not more polls.
		PollTimeout: l.duration("POLL_TIMEOUT", 2*time.Hour),

		// two_phase calls the gateway with no transaction open (v2); in_tx
		// is the v1 start, kept to compare the two.
		StartMode: l.oneOf("START_MODE", "two_phase", "two_phase", "in_tx"),
		// Lookups of an ambiguous attempt, spaced so a gateway that is slow
		// to show a new payment gets 10 to 15 s before the attempt is
		// abandoned after three certain "not found" answers.
		ReconcileInterval:      l.duration("RECONCILE_INTERVAL", 5*time.Second),
		ReconcileNotFoundLimit: l.integer("RECONCILE_NOT_FOUND_LIMIT", 3),

		// Well under the 30 to 60 second idle timeouts common in proxies.
		WaitTimeout: l.duration("WAIT_TIMEOUT", 15*time.Second),
		// Each of the two reads of a long poll, and the batch query; short
		// so a slow database cannot push the answer past the write timeout.
		WaitReadTimeout: l.duration("WAIT_READ_TIMEOUT", 2*time.Second),
		// Each waiter costs a goroutine and a socket; this keeps an instance
		// within a few tens of megabytes for long polls.
		WaitMaxWaiters: l.integer("WAIT_MAX_WAITERS", 2000),
		// Five queries a second per instance, whatever the number of waiters.
		WaitBatchInterval: l.duration("WAIT_BATCH_INTERVAL", 200*time.Millisecond),

		// Commit hooks wake the local relay; this is the fallback cadence.
		RelayInterval: l.duration("RELAY_INTERVAL", 500*time.Millisecond),
		RelayBatch:    l.integer("RELAY_BATCH", 50),
		// Claimed events stay hidden from other relays this long; the relay
		// starts a delivery only if it can finish inside the lease.
		RelayLease:       l.duration("RELAY_LEASE", time.Minute),
		EventsWebhookURL: l.str("EVENTS_WEBHOOK_URL", ""),
		// A subscriber slower than this counts as failed and is retried.
		EventsWebhookTimeout: l.duration("EVENTS_WEBHOOK_TIMEOUT", 5*time.Second),
	}
	l.rejectUnknown()
	if err := errors.Join(l.errs...); err != nil {
		return Config{}, err
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	var errs []error
	check := func(ok bool, format string, args ...any) {
		if !ok {
			errs = append(errs, fmt.Errorf(format, args...))
		}
	}
	durations := map[string]time.Duration{
		"HTTP_READ_HEADER_TIMEOUT": c.HTTPReadHeaderTimeout, "HTTP_READ_TIMEOUT": c.HTTPReadTimeout,
		"HTTP_WRITE_TIMEOUT": c.HTTPWriteTimeout, "HTTP_IDLE_TIMEOUT": c.HTTPIdleTimeout,
		"SHUTDOWN_TIMEOUT": c.ShutdownTimeout, "DB_STATEMENT_TIMEOUT": c.DBStatementTimeout,
		"DB_LOCK_TIMEOUT": c.DBLockTimeout, "DB_IDLE_IN_TX_TIMEOUT": c.DBIdleInTxTimeout,
		"GATEWAY_TIMEOUT": c.GatewayTimeout, "GATEWAY_MAX_RETRY_AFTER": c.GatewayMaxRetryAfter,
		"WORKER_IDLE_INTERVAL": c.WorkerIdleInterval, "TASK_LEASE": c.TaskLease,
		"TASK_TIMEOUT": c.TaskTimeout, "TASK_RETRY_BASE": c.TaskRetryBase, "TASK_RETRY_MAX": c.TaskRetryMax,
		"POLL_INTERVAL": c.PollInterval, "POLL_TIMEOUT": c.PollTimeout, "WAIT_TIMEOUT": c.WaitTimeout, "WAIT_READ_TIMEOUT": c.WaitReadTimeout,
		"WAIT_BATCH_INTERVAL": c.WaitBatchInterval, "RELAY_INTERVAL": c.RelayInterval,
		"RELAY_LEASE": c.RelayLease, "EVENTS_WEBHOOK_TIMEOUT": c.EventsWebhookTimeout,
		"RECONCILE_INTERVAL": c.ReconcileInterval,
	}
	for _, name := range slices.Sorted(maps.Keys(durations)) {
		// A zero timeout would mean "already expired" or "no limit"; neither
		// is ever intended.
		check(durations[name] > 0, "%s%s must be positive", envPrefix, name)
	}
	check(c.ShutdownDrainDelay >= 0, "%sSHUTDOWN_DRAIN_DELAY must not be negative", envPrefix)
	check(c.DBMaxConns >= 1 && c.DBMaxConns <= 1000, "%sDB_MAX_CONNS must be between 1 and 1000", envPrefix)
	check(c.WorkerConcurrency >= 1, "%sWORKER_CONCURRENCY must be at least 1", envPrefix)
	check(c.TaskMaxAttempts >= 1, "%sTASK_MAX_ATTEMPTS must be at least 1", envPrefix)
	check(c.GatewayMaxRetries >= 0 && c.GatewayMaxRetries <= 10, "%sGATEWAY_MAX_RETRIES must be between 0 and 10", envPrefix)
	check(c.WaitMaxWaiters >= 1, "%sWAIT_MAX_WAITERS must be at least 1", envPrefix)
	check(c.RelayBatch >= 1, "%sRELAY_BATCH must be at least 1", envPrefix)
	check(c.ReconcileNotFoundLimit >= 1, "%sRECONCILE_NOT_FOUND_LIMIT must be at least 1", envPrefix)
	check(c.DatabaseURL != "", "%sDATABASE_URL is required", envPrefix)
	check(c.AdminAddr != "", "%sADMIN_ADDR is required", envPrefix)
	check(!c.Roles.API || c.AdminAddr != c.HTTPAddr,
		"%sADMIN_ADDR must differ from HTTP_ADDR: metrics and probes need their own listener", envPrefix)
	check(c.TaskRetryBase <= c.TaskRetryMax, "%sTASK_RETRY_BASE must not exceed TASK_RETRY_MAX", envPrefix)

	// Relations between settings: each one guards a failure mode.
	check(c.HTTPWriteTimeout >= c.WaitTimeout+2*c.WaitReadTimeout+5*time.Second,
		"%sHTTP_WRITE_TIMEOUT must exceed WAIT_TIMEOUT + 2 x WAIT_READ_TIMEOUT by at least 5s, or long polls are cut off", envPrefix)
	check(c.TaskTimeout+10*time.Second <= c.TaskLease,
		"%sTASK_TIMEOUT must be at least 10s shorter than TASK_LEASE, or a slow task is claimed twice", envPrefix)
	gatewayBudget := time.Duration(c.GatewayMaxRetries+1)*c.GatewayTimeout + time.Duration(c.GatewayMaxRetries)*c.GatewayMaxRetryAfter
	check(gatewayBudget <= c.TaskTimeout,
		"%sTASK_TIMEOUT (%s) must cover the worst gateway budget (%s)", envPrefix, c.TaskTimeout, gatewayBudget)
	check(c.DBIdleInTxTimeout > c.TaskTimeout,
		"%sDB_IDLE_IN_TX_TIMEOUT must exceed TASK_TIMEOUT: the in_tx start idles in a transaction during the gateway call", envPrefix)
	check(c.RelayLease >= 2*c.EventsWebhookTimeout,
		"%sRELAY_LEASE must be at least twice EVENTS_WEBHOOK_TIMEOUT, or the relay publishes almost nothing per batch", envPrefix)
	check(c.WorkerConcurrency+3 <= c.DBMaxConns,
		"%sWORKER_CONCURRENCY must be at most DB_MAX_CONNS-3, leaving connections for the API, the batcher and the relay", envPrefix)

	if c.Roles.Worker {
		check(validHTTPURL(c.GatewayURL), "%sGATEWAY_URL must be an http(s) URL when the worker role is on", envPrefix)
	}
	if c.Roles.API {
		check(len(c.WebhookSecret) >= 16, "%sWEBHOOK_SECRET must be at least 16 bytes when the api role is on", envPrefix)
	}
	if c.EventsWebhookURL != "" {
		check(validHTTPURL(c.EventsWebhookURL), "%sEVENTS_WEBHOOK_URL must be an http(s) URL", envPrefix)
	}
	return errors.Join(errs...)
}

func validHTTPURL(s string) bool {
	u, err := url.Parse(s)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

// loader reads prefixed variables, remembers which ones it used and
// collects parse errors instead of stopping at the first.
type loader struct {
	env  map[string]string
	used map[string]bool
	errs []error
}

func newLoader(environ []string) *loader {
	env := make(map[string]string)
	for _, kv := range environ {
		if k, v, ok := strings.Cut(kv, "="); ok && strings.HasPrefix(k, envPrefix) {
			env[k] = v
		}
	}
	return &loader{env: env, used: make(map[string]bool)}
}

func (l *loader) lookup(name string) (string, bool) {
	key := envPrefix + name
	l.used[key] = true
	v, ok := l.env[key]
	return v, ok
}

func (l *loader) fail(name string, err error) {
	l.errs = append(l.errs, fmt.Errorf("%s%s: %w", envPrefix, name, err))
}

func (l *loader) str(name, def string) string {
	if v, ok := l.lookup(name); ok {
		return v
	}
	return def
}

func (l *loader) duration(name string, def time.Duration) time.Duration {
	v, ok := l.lookup(name)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.fail(name, err)
	}
	return d
}

func (l *loader) integer(name string, def int) int {
	v, ok := l.lookup(name)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.fail(name, err)
	}
	return n
}

func (l *loader) level(name string, def slog.Level) slog.Level {
	v, ok := l.lookup(name)
	if !ok {
		return def
	}
	var lv slog.Level
	if err := lv.UnmarshalText([]byte(v)); err != nil {
		l.fail(name, err)
	}
	return lv
}

// oneOf reads a value that must be one of allowed.
func (l *loader) oneOf(name, def string, allowed ...string) string {
	v := l.str(name, def)
	if !slices.Contains(allowed, v) {
		l.fail(name, fmt.Errorf("%q is not one of %v", v, allowed))
	}
	return v
}

func (l *loader) roles(name, def string) Roles {
	v := l.str(name, def)
	var r Roles
	for _, role := range strings.FieldsFunc(v, func(c rune) bool { return c == ',' }) {
		switch strings.TrimSpace(role) {
		case "api":
			r.API = true
		case "worker":
			r.Worker = true
		case "relay":
			r.Relay = true
		default:
			l.fail(name, fmt.Errorf("unknown role %q (want api, worker, relay)", role))
		}
	}
	if r == (Roles{}) {
		l.fail(name, errors.New("at least one role is required"))
	}
	return r
}

func (l *loader) rejectUnknown() {
	var unknown []string
	for k := range l.env {
		if !l.used[k] {
			unknown = append(unknown, k)
		}
	}
	slices.Sort(unknown)
	for _, k := range unknown {
		l.errs = append(l.errs, fmt.Errorf("%s: unknown variable", k))
	}
}
