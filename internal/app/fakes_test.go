package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// memStore is an in-memory PaymentStore, Enqueuer and EventRecorder. Its
// TxRunner snapshots the state and restores it when fn fails, so unit tests
// see real rollback semantics.
type memStore struct {
	mu       sync.Mutex
	payments map[uuid.UUID]payment.Payment
	attempts map[attemptID]ports.Attempt
	tasks    []ports.TaskSpec
	events   []payment.Event
	reads    int
	readDone chan struct{} // receives a value after every read when set

	// runs counts transactions; the one numbered crashOnRun crashes just
	// before its commit, like a process killed at that point.
	runs       int
	crashOnRun int
}

type attemptID struct {
	payment uuid.UUID
	number  int
}

// errCrash is the panic value of a simulated process crash.
var errCrash = errors.New("simulated crash")

func newMemStore() *memStore {
	return &memStore{payments: map[uuid.UUID]payment.Payment{}, attempts: map[attemptID]ports.Attempt{}}
}

type memState struct {
	payments map[uuid.UUID]payment.Payment
	attempts map[attemptID]ports.Attempt
	tasks    []ports.TaskSpec
	events   []payment.Event
}

func (s *memStore) snapshot() memState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return memState{maps.Clone(s.payments), maps.Clone(s.attempts), slices.Clone(s.tasks), slices.Clone(s.events)}
}

func (s *memStore) restore(st memState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.payments, s.attempts, s.tasks, s.events = st.payments, st.attempts, st.tasks, st.events
}

func (s *memStore) Run(ctx context.Context, fn func(ctx context.Context) error) error {
	snap := s.snapshot()
	s.mu.Lock()
	s.runs++
	crash := s.runs == s.crashOnRun
	s.mu.Unlock()
	if err := fn(ctx); err != nil {
		s.restore(snap)
		return err
	}
	if crash {
		s.restore(snap) // the commit never happened
		panic(errCrash)
	}
	return nil
}

// memAttempts is the ports.Attempts view of a memStore.
type memAttempts struct{ s *memStore }

func (m memAttempts) Open(_ context.Context, id uuid.UUID) (ports.Attempt, bool, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	for k, a := range m.s.attempts {
		if k.payment == id && a.State.Open() {
			return a, true, nil
		}
	}
	return ports.Attempt{}, false, nil
}

func (m memAttempts) Get(_ context.Context, id uuid.UUID, n int) (ports.Attempt, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	a, ok := m.s.attempts[attemptID{id, n}]
	if !ok {
		return ports.Attempt{}, ports.ErrAttemptNotFound
	}
	return a, nil
}

func (m memAttempts) Create(_ context.Context, id uuid.UUID, now time.Time) (ports.Attempt, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	n := 1
	for k, a := range m.s.attempts {
		if k.payment != id {
			continue
		}
		if a.State.Open() {
			return ports.Attempt{}, errors.New("a second open attempt")
		}
		n = max(n, k.number+1)
	}
	a := ports.Attempt{PaymentID: id, Number: n, IdempotencyKey: ports.AttemptKey(id, n),
		State: ports.AttemptIntent, CreatedAt: now, UpdatedAt: now}
	m.s.attempts[attemptID{id, n}] = a
	return a, nil
}

func (m memAttempts) Update(_ context.Context, a ports.Attempt, from ports.AttemptState) error {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	k := attemptID{a.PaymentID, a.Number}
	cur, ok := m.s.attempts[k]
	if !ok || cur.State != from {
		return ports.ErrAttemptChanged
	}
	m.s.attempts[k] = a
	return nil
}

// attemptStates lists the payment's attempts' states in attempt order.
func (s *memStore) attemptStates(id uuid.UUID) []ports.AttemptState {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ports.AttemptState
	for n := 1; ; n++ {
		a, ok := s.attempts[attemptID{id, n}]
		if !ok {
			return out
		}
		out = append(out, a.State)
	}
}

func (s *memStore) Insert(_ context.Context, p payment.Payment) (payment.Payment, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.payments {
		if existing.IdempotencyKey == p.IdempotencyKey {
			return existing, false, nil
		}
	}
	s.payments[p.ID] = p
	return p, true, nil
}

func (s *memStore) Payment(_ context.Context, id uuid.UUID) (payment.Payment, error) {
	s.mu.Lock()
	s.reads++
	p, ok := s.payments[id]
	s.mu.Unlock()
	if s.readDone != nil {
		s.readDone <- struct{}{}
	}
	if !ok {
		return payment.Payment{}, payment.ErrNotFound
	}
	return p, nil
}

func (s *memStore) Lock(ctx context.Context, id uuid.UUID) (payment.Payment, error) {
	return s.Payment(ctx, id)
}

func (s *memStore) Update(_ context.Context, p payment.Payment, expectedVersion int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.payments[p.ID]
	if !ok {
		return payment.ErrNotFound
	}
	if cur.Version != expectedVersion {
		return payment.ErrVersionConflict
	}
	s.payments[p.ID] = p
	return nil
}

func (s *memStore) Enqueue(_ context.Context, t ports.TaskSpec) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.tasks {
		if existing.Kind == t.Kind && existing.PaymentID == t.PaymentID && existing.DedupKey == t.DedupKey {
			return false, nil
		}
	}
	s.tasks = append(s.tasks, t)
	return true, nil
}

func (s *memStore) Record(_ context.Context, e payment.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
	return nil
}

func (s *memStore) tasksOf(kind ports.TaskKind) []ports.TaskSpec {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ports.TaskSpec
	for _, t := range s.tasks {
		if t.Kind == kind {
			out = append(out, t)
		}
	}
	return out
}

func (s *memStore) eventTypes() []payment.EventType {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []payment.EventType
	for _, e := range s.events {
		out = append(out, e.Type)
	}
	return out
}

// fakeGateway returns canned answers and records calls.
type fakeGateway struct {
	mu         sync.Mutex
	createResp ports.GatewayPayment
	createErr  error
	readResp   ports.GatewayPayment
	readErr    error
	creates    []ports.GatewayRequest
	reads      int
	found      []ports.GatewayPayment
	findErr    error
	lookups    int
}

func (g *fakeGateway) CreatePayment(_ context.Context, req ports.GatewayRequest) (ports.GatewayPayment, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.creates = append(g.creates, req)
	return g.createResp, g.createErr
}

func (g *fakeGateway) FindByReference(context.Context, string) ([]ports.GatewayPayment, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.lookups++
	return g.found, g.findErr
}

func (g *fakeGateway) Payment(_ context.Context, _ string) (ports.GatewayPayment, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.reads++
	return g.readResp, g.readErr
}

// fakeWatcher hands out a channel controlled by the test.
type fakeWatcher struct {
	mu       sync.Mutex
	ch       chan int64
	err      error
	canceled bool
}

func (w *fakeWatcher) Watch(uuid.UUID, int64) (<-chan int64, func(), error) {
	if w.err != nil {
		return nil, nil, w.err
	}
	return w.ch, func() {
		w.mu.Lock()
		w.canceled = true
		w.mu.Unlock()
	}, nil
}

// fakeMetrics records what the use cases report.
type fakeMetrics struct {
	mu         sync.Mutex
	created    int
	changes    []string
	reconciled []string
}

func (m *fakeMetrics) PaymentCreated() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.created++
}

func (m *fakeMetrics) StatusChanged(from, to payment.Status, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.changes = append(m.changes, string(from)+"->"+string(to))
}

func (m *fakeMetrics) Reconciled(outcome string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconciled = append(m.reconciled, outcome)
}

type fixedClock struct{ now time.Time }

func (c *fixedClock) Now() time.Time { return c.now }

type fixture struct {
	svc     *Service
	store   *memStore
	gateway *fakeGateway
	watcher *fakeWatcher
	clock   *fixedClock
	metrics *fakeMetrics
}

var testConfig = Config{
	StartMode:              StartTwoPhase,
	ReconcileInterval:      5 * time.Second,
	ReconcileNotFoundLimit: 3,
	PollInterval:           3 * time.Second,
	PollTimeout:            time.Hour,
	WaitTimeout:            time.Minute,
	WaitReadTimeout:        time.Second,
}

func newFixture(cfg Config) *fixture {
	f := &fixture{
		store:   newMemStore(),
		gateway: &fakeGateway{},
		watcher: &fakeWatcher{ch: make(chan int64, 1)},
		clock:   &fixedClock{now: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)},
		metrics: &fakeMetrics{},
	}
	f.svc = NewService(Deps{
		Tx:       f.store,
		Payments: f.store,
		Attempts: memAttempts{f.store},
		Tasks:    f.store,
		Events:   f.store,
		Gateway:  f.gateway,
		Watcher:  f.watcher,
		Clock:    f.clock,
		Metrics:  f.metrics,
		NewID:    uuid.NewV7,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, cfg)
	return f
}

// seed stores a payment in the given status, with a gateway reference unless
// it is still pending.
func (f *fixture) seed(status payment.Status) payment.Payment {
	m, _ := payment.NewMoney(1500, "EUR")
	p, _ := payment.New(uuid.New(), "merchant-1", m, uuid.NewString(), "hash", f.clock.now)
	p.Status = status
	if status != payment.StatusPending {
		p.GatewayRef = "gw_1"
	}
	f.store.payments[p.ID] = p
	return p
}

// v1Config runs the version 1 start (gateway call inside the transaction).
func v1Config() Config {
	cfg := testConfig
	cfg.StartMode = StartInTx
	return cfg
}
