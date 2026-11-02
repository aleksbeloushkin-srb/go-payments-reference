package app

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// StartMode selects how StartPayment calls the gateway.
type StartMode string

// Start modes.
const (
	// StartTwoPhase records an attempt, calls the gateway with no
	// transaction open, then applies the answer (v2, the default).
	StartTwoPhase StartMode = "two_phase"
	// StartInTx calls the gateway inside the transaction that holds the
	// payment's row lock (v1). It is kept so the two can be compared.
	StartInTx StartMode = "in_tx"
)

// Config holds the tunables of the use cases.
type Config struct {
	// StartMode selects the v1 or the v2 start.
	StartMode StartMode
	// ReconcileInterval is the pause between two lookups of an ambiguous
	// gateway attempt.
	ReconcileInterval time.Duration
	// ReconcileNotFoundLimit is how many certain "not found" answers abandon
	// an ambiguous attempt, so that a new one may be made.
	ReconcileNotFoundLimit int
	// MaxGatewayAttempts caps the attempts per payment. When the last one is
	// abandoned, the payment stays pending and its task goes dead for an
	// operator.
	MaxGatewayAttempts int
	// DuplicateCheckDelay is how long after an attempt that follows an
	// abandoned one the gateway is asked again whether it holds more than one
	// payment for our reference.
	DuplicateCheckDelay time.Duration
	// PollInterval is the delay between two gateway polls of a payment that
	// is not final yet.
	PollInterval time.Duration
	// PollTimeout is how long after creation polling gives up. The poll task
	// then goes to the dead state, which asks a human to reconcile.
	PollTimeout time.Duration
	// WaitTimeout is the long-poll soft timeout: after it WaitPayment answers
	// "not modified" and the client asks again.
	WaitTimeout time.Duration
	// WaitReadTimeout bounds each database read of a long poll, so that the
	// answer is always written before the HTTP server's write timeout.
	WaitReadTimeout time.Duration
}

// Deps lists the collaborators of Service. Every field is required.
type Deps struct {
	Tx       ports.TxRunner
	Payments ports.PaymentStore
	Attempts ports.Attempts
	Tasks    ports.Enqueuer
	Events   ports.EventRecorder
	Gateway  ports.Gateway
	Watcher  ports.Watcher
	Clock    ports.Clock
	Metrics  ports.Metrics
	// NewID generates payment ids. Production uses uuid.NewV7, which keeps
	// primary-key inserts close together in the index.
	NewID  func() (uuid.UUID, error)
	Logger *slog.Logger
}

// Service implements the use cases.
type Service struct {
	tx       ports.TxRunner
	payments ports.PaymentStore
	attempts ports.Attempts
	tasks    ports.Enqueuer
	events   ports.EventRecorder
	gateway  ports.Gateway
	watcher  ports.Watcher
	clock    ports.Clock
	metrics  ports.Metrics
	newID    func() (uuid.UUID, error)
	log      *slog.Logger
	cfg      Config
}

// NewService returns a Service. It panics if a dependency is missing or the
// configuration is invalid: that is a wiring bug that must fail at start,
// not on the first request. platform.LoadConfig validates the environment;
// this check also covers tests and other callers.
func NewService(d Deps, cfg Config) *Service {
	if err := cfg.validate(); err != nil {
		panic("app: NewService: " + err.Error())
	}
	if d.Tx == nil || d.Payments == nil || d.Attempts == nil || d.Tasks == nil || d.Events == nil ||
		d.Gateway == nil || d.Watcher == nil || d.Clock == nil || d.Metrics == nil || d.NewID == nil || d.Logger == nil {
		panic("app: NewService: missing dependency")
	}
	return &Service{
		tx:       d.Tx,
		payments: d.Payments,
		attempts: d.Attempts,
		tasks:    d.Tasks,
		events:   d.Events,
		gateway:  d.Gateway,
		watcher:  d.Watcher,
		clock:    d.Clock,
		metrics:  d.Metrics,
		newID:    d.NewID,
		log:      d.Logger,
		cfg:      cfg,
	}
}

func (c Config) validate() error {
	switch {
	case c.StartMode != StartTwoPhase && c.StartMode != StartInTx:
		return fmt.Errorf("unknown start mode %q", c.StartMode)
	case c.ReconcileInterval <= 0, c.DuplicateCheckDelay <= 0, c.PollInterval <= 0, c.PollTimeout <= 0,
		c.WaitTimeout <= 0, c.WaitReadTimeout <= 0:
		return errors.New("every duration must be positive")
	case c.ReconcileNotFoundLimit < 1:
		return errors.New("ReconcileNotFoundLimit must be at least 1")
	case c.MaxGatewayAttempts < 1:
		return errors.New("MaxGatewayAttempts must be at least 1")
	}
	return nil
}
