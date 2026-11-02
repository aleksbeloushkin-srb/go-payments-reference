package app

import (
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// Config holds the tunables of the use cases.
type Config struct {
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
	Tasks    ports.Enqueuer
	Events   ports.EventRecorder
	Gateway  ports.Gateway
	Watcher  ports.Watcher
	Clock    ports.Clock
	// NewID generates payment ids. Production uses uuid.NewV7, which keeps
	// primary-key inserts close together in the index.
	NewID  func() (uuid.UUID, error)
	Logger *slog.Logger
}

// Service implements the use cases.
type Service struct {
	tx       ports.TxRunner
	payments ports.PaymentStore
	tasks    ports.Enqueuer
	events   ports.EventRecorder
	gateway  ports.Gateway
	watcher  ports.Watcher
	clock    ports.Clock
	newID    func() (uuid.UUID, error)
	log      *slog.Logger
	cfg      Config
}

// NewService returns a Service. It panics if a dependency is missing: that is
// a wiring bug that must fail at start, not on the first request.
func NewService(d Deps, cfg Config) *Service {
	if d.Tx == nil || d.Payments == nil || d.Tasks == nil || d.Events == nil ||
		d.Gateway == nil || d.Watcher == nil || d.Clock == nil || d.NewID == nil || d.Logger == nil {
		panic("app: NewService: missing dependency")
	}
	return &Service{
		tx:       d.Tx,
		payments: d.Payments,
		tasks:    d.Tasks,
		events:   d.Events,
		gateway:  d.Gateway,
		watcher:  d.Watcher,
		clock:    d.Clock,
		newID:    d.NewID,
		log:      d.Logger,
		cfg:      cfg,
	}
}
