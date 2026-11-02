package app

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// dedupGateway executes each idempotency key once, like a gateway that
// deduplicates, and can crash its caller around the execution.
type dedupGateway struct {
	mu          sync.Mutex
	status      payment.Status
	executed    map[string]ports.GatewayPayment
	references  map[string][]string // payment id -> keys executed for it
	crashBefore bool                // one-shot: crash before executing
	crashAfter  bool                // one-shot: execute, then crash before answering
	during      func()              // runs after executing, before answering
}

func newDedupGateway() *dedupGateway {
	return &dedupGateway{
		status:     payment.StatusProcessing,
		executed:   map[string]ports.GatewayPayment{},
		references: map[string][]string{},
	}
}

func (g *dedupGateway) CreatePayment(_ context.Context, req ports.GatewayRequest) (ports.GatewayPayment, error) {
	g.mu.Lock()
	if g.crashBefore {
		g.crashBefore = false
		g.mu.Unlock()
		panic(errCrash)
	}
	res, ok := g.executed[req.IdempotencyKey]
	if !ok {
		res = ports.GatewayPayment{Ref: "gw_" + req.IdempotencyKey, Status: g.status}
		g.executed[req.IdempotencyKey] = res
		ref := req.PaymentID.String()
		g.references[ref] = append(g.references[ref], req.IdempotencyKey)
	}
	crash, during := g.crashAfter, g.during
	g.crashAfter, g.during = false, nil
	g.mu.Unlock()
	if during != nil {
		during()
	}
	if crash {
		panic(errCrash)
	}
	return res, nil
}

func (g *dedupGateway) Payment(_ context.Context, ref string) (ports.GatewayPayment, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, p := range g.executed {
		if p.Ref == ref {
			return p, nil
		}
	}
	return ports.GatewayPayment{}, ports.ErrGatewayNotFound
}

func (g *dedupGateway) FindByReference(_ context.Context, reference string) ([]ports.GatewayPayment, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := []ports.GatewayPayment{}
	for _, key := range g.references[reference] {
		out = append(out, g.executed[key])
	}
	return out, nil
}

// executions is the number of payments the gateway created: what the client
// would be charged for.
func (g *dedupGateway) executions() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.executed)
}

// survive runs fn like a worker process: a simulated crash ends the run
// instead of failing the test.
func survive(fn func() error) (crashed bool, err error) {
	defer func() {
		if p := recover(); p != nil {
			if p != errCrash {
				panic(p)
			}
			crashed = true
		}
	}()
	return false, fn()
}

func newTwoPhaseFixture() (*fixture, *dedupGateway) {
	f := newFixture(testConfig)
	gw := newDedupGateway()
	f.svc.gateway = gw
	return f, gw
}

// One test per row of the crash-point table in proposal 0001: crash at the
// point, run the task again as a fresh process would, and check that the
// payment ends started and the gateway executed it exactly once.
func TestTwoPhaseStartCrashPoints(t *testing.T) {
	tests := []struct {
		name  string
		crash func(f *fixture, gw *dedupGateway)
		// state left behind by the crash
		wantAttempts   []ports.AttemptState
		wantExecutions int
	}{
		{
			name:  "before tx1 commits",
			crash: func(f *fixture, _ *dedupGateway) { f.store.crashOnRun = 1 },
		},
		{
			name:           "after tx1, before the call",
			crash:          func(_ *fixture, gw *dedupGateway) { gw.crashBefore = true },
			wantAttempts:   []ports.AttemptState{ports.AttemptIntent},
			wantExecutions: 0,
		},
		{
			name:           "after the call, before tx2",
			crash:          func(_ *fixture, gw *dedupGateway) { gw.crashAfter = true },
			wantAttempts:   []ports.AttemptState{ports.AttemptIntent},
			wantExecutions: 1,
		},
		{
			name:           "during tx2",
			crash:          func(f *fixture, _ *dedupGateway) { f.store.crashOnRun = 2 },
			wantAttempts:   []ports.AttemptState{ports.AttemptIntent},
			wantExecutions: 1,
		},
		{
			name:           "after tx2",
			crash:          func(*fixture, *dedupGateway) {}, // the worker dies before settling the task
			wantAttempts:   []ports.AttemptState{ports.AttemptDone},
			wantExecutions: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, gw := newTwoPhaseFixture()
			p := f.seed(payment.StatusPending)
			tt.crash(f, gw)

			if _, err := survive(func() error { return f.svc.StartPayment(context.Background(), p.ID) }); err != nil {
				t.Fatalf("first run: %v", err)
			}
			if got := f.store.attemptStates(p.ID); !slices.Equal(got, tt.wantAttempts) {
				t.Fatalf("attempts after the crash = %v, want %v", got, tt.wantAttempts)
			}
			if n := gw.executions(); n != tt.wantExecutions {
				t.Fatalf("executions after the crash = %d, want %d", n, tt.wantExecutions)
			}

			// The task runs again: the lease expired, another worker claims it.
			f.store.crashOnRun = 0
			if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
				t.Fatalf("retry: %v", err)
			}

			got := f.store.payments[p.ID]
			if got.Status != payment.StatusProcessing || got.Version != 2 {
				t.Fatalf("payment = %s v%d, want processing v2", got.Status, got.Version)
			}
			if states := f.store.attemptStates(p.ID); !slices.Equal(states, []ports.AttemptState{ports.AttemptDone}) {
				t.Fatalf("attempts = %v, want one done attempt", states)
			}
			if n := gw.executions(); n != 1 {
				t.Fatalf("gateway executions = %d, want exactly 1", n)
			}
			if n := len(f.store.tasksOf(ports.TaskPollPayment)); n != 1 {
				t.Fatalf("poll tasks = %d, want 1", n)
			}
		})
	}
}

func TestTwoPhaseStartOutcomes(t *testing.T) {
	tests := []struct {
		name         string
		resp         ports.GatewayPayment
		err          error
		wantStatus   payment.Status
		wantAttempt  ports.AttemptState
		wantErr      bool
		wantPoll     bool
		wantReconcil bool
	}{
		{name: "processing", resp: ports.GatewayPayment{Ref: "gw_1", Status: payment.StatusProcessing},
			wantStatus: payment.StatusProcessing, wantAttempt: ports.AttemptDone, wantPoll: true},
		{name: "immediate success", resp: ports.GatewayPayment{Ref: "gw_1", Status: payment.StatusSucceeded},
			wantStatus: payment.StatusSucceeded, wantAttempt: ports.AttemptDone},
		{name: "decline", err: ports.ErrGatewayRejected,
			wantStatus: payment.StatusFailed, wantAttempt: ports.AttemptDone},
		{name: "ambiguous goes to reconciliation", err: ports.ErrGatewayAmbiguous,
			wantStatus: payment.StatusPending, wantAttempt: ports.AttemptUnknown, wantReconcil: true},
		{name: "not executed keeps the intent for a retry", err: ports.ErrGatewayUnavailable,
			wantStatus: payment.StatusPending, wantAttempt: ports.AttemptIntent, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(testConfig)
			p := f.seed(payment.StatusPending)
			f.gateway.createResp, f.gateway.createErr = tt.resp, tt.err

			err := f.svc.StartPayment(context.Background(), p.ID)

			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if got := f.store.payments[p.ID].Status; got != tt.wantStatus {
				t.Fatalf("status = %s, want %s", got, tt.wantStatus)
			}
			if got := f.store.attemptStates(p.ID); !slices.Equal(got, []ports.AttemptState{tt.wantAttempt}) {
				t.Fatalf("attempts = %v, want [%s]", got, tt.wantAttempt)
			}
			if key := f.gateway.creates[0].IdempotencyKey; key != p.ID.String()+":1" {
				t.Fatalf("idempotency key = %q, want <payment id>:1", key)
			}
			if got := len(f.store.tasksOf(ports.TaskPollPayment)) == 1; got != tt.wantPoll {
				t.Fatalf("poll task = %v, want %v", got, tt.wantPoll)
			}
			if got := len(f.store.tasksOf(ports.TaskReconcilePayment)) == 1; got != tt.wantReconcil {
				t.Fatalf("reconcile task = %v, want %v", got, tt.wantReconcil)
			}
		})
	}
}

// A retry after an unexecuted call reuses the open intent and its key.
func TestTwoPhaseRetryReusesTheKey(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusPending)
	f.gateway.createErr = ports.ErrGatewayUnavailable
	_ = f.svc.StartPayment(context.Background(), p.ID)
	f.gateway.createErr = nil
	f.gateway.createResp = ports.GatewayPayment{Ref: "gw_1", Status: payment.StatusProcessing}

	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if a, b := f.gateway.creates[0].IdempotencyKey, f.gateway.creates[1].IdempotencyKey; a != b {
		t.Fatalf("keys %q and %q, want the same key for the same attempt", a, b)
	}
}

// A gateway event that lands between tx1 and tx2 is applied at once (no
// lock is held during the call), and the late answer of tx2 does not move
// the payment back.
func TestTwoPhaseWebhookBetweenTx1AndTx2(t *testing.T) {
	f, gw := newTwoPhaseFixture()
	p := f.seed(payment.StatusPending)
	gw.during = func() {
		payload, _ := json.Marshal(GatewayEvent{ID: "evt_1", PaymentID: p.ID, GatewayRef: "gw_" + p.ID.String() + ":1", Status: payment.StatusSucceeded})
		if err := f.svc.ApplyGatewayEvent(context.Background(), payload); err != nil {
			t.Errorf("webhook during the call: %v", err)
		}
	}

	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	got := f.store.payments[p.ID]
	if got.Status != payment.StatusSucceeded || got.Version != 2 {
		t.Fatalf("payment = %s v%d, want succeeded v2 (the processing answer is stale)", got.Status, got.Version)
	}
	if states := f.store.attemptStates(p.ID); !slices.Equal(states, []ports.AttemptState{ports.AttemptDone}) {
		t.Fatalf("attempts = %v, want the attempt closed", states)
	}
}

// A worker whose lease expired during the call finishes after another
// worker settled the attempt: tx2 changes nothing.
func TestTwoPhaseLateWorkerChangesNothing(t *testing.T) {
	f, gw := newTwoPhaseFixture()
	p := f.seed(payment.StatusPending)
	gw.during = func() {
		// Another worker settles the same attempt meanwhile.
		f.store.mu.Lock()
		k := attemptID{p.ID, 1}
		a := f.store.attempts[k]
		a.State = ports.AttemptDone
		f.store.attempts[k] = a
		q := f.store.payments[p.ID]
		q.Status, q.Version = payment.StatusSucceeded, 2
		f.store.payments[p.ID] = q
		f.store.mu.Unlock()
	}

	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if got := f.store.payments[p.ID]; got.Status != payment.StatusSucceeded || got.Version != 2 {
		t.Fatalf("payment = %s v%d, want the other worker's result untouched", got.Status, got.Version)
	}
	if len(f.metrics.changes) != 0 {
		t.Fatalf("reported changes = %v, want none from the late worker", f.metrics.changes)
	}
}

// While an attempt is being reconciled, a start retry does not call the
// gateway again.
func TestTwoPhaseUnknownAttemptBlocksNewCalls(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusPending)
	f.gateway.createErr = ports.ErrGatewayAmbiguous
	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}

	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if n := len(f.gateway.creates); n != 1 {
		t.Fatalf("gateway calls = %d, want 1: reconciliation owns the unknown attempt", n)
	}
}

func reconcile(t *testing.T, f *fixture, id uuid.UUID, attempt int) error {
	t.Helper()
	payload, _ := json.Marshal(reconcilePayload{Attempt: attempt})
	return f.svc.ReconcilePayment(context.Background(), id, payload)
}

// ambiguousStart leaves attempt 1 unknown.
func ambiguousStart(t *testing.T, f *fixture) payment.Payment {
	t.Helper()
	p := f.seed(payment.StatusPending)
	f.gateway.createErr = ports.ErrGatewayAmbiguous
	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	f.gateway.createErr = nil
	return p
}

func TestReconcileFound(t *testing.T) {
	f := newFixture(testConfig)
	p := ambiguousStart(t, f)
	f.gateway.found = []ports.GatewayPayment{{Ref: "gw_9", Status: payment.StatusProcessing}}

	if err := reconcile(t, f, p.ID, 1); err != nil {
		t.Fatal(err)
	}
	got := f.store.payments[p.ID]
	if got.Status != payment.StatusProcessing || got.GatewayRef != "gw_9" {
		t.Fatalf("payment = %s %s, want the gateway's payment applied", got.Status, got.GatewayRef)
	}
	if states := f.store.attemptStates(p.ID); !slices.Equal(states, []ports.AttemptState{ports.AttemptDone}) {
		t.Fatalf("attempts = %v", states)
	}
	if !slices.Equal(f.metrics.reconciled, []string{reconcileFound}) {
		t.Fatalf("reconcile outcomes = %v", f.metrics.reconciled)
	}
}

func TestReconcileNotFoundAbandonsAfterTheLimit(t *testing.T) {
	f := newFixture(testConfig)
	p := ambiguousStart(t, f)
	f.gateway.found = []ports.GatewayPayment{}

	for i := 1; i < testConfig.ReconcileNotFoundLimit; i++ {
		var later *ports.RetryLaterError
		if err := reconcile(t, f, p.ID, 1); !errors.As(err, &later) {
			t.Fatalf("lookup %d: err = %v, want RetryLater", i, err)
		}
	}
	if err := reconcile(t, f, p.ID, 1); err != nil {
		t.Fatalf("last lookup: %v", err)
	}
	if states := f.store.attemptStates(p.ID); !slices.Equal(states, []ports.AttemptState{ports.AttemptAbandoned}) {
		t.Fatalf("attempts = %v, want abandoned", states)
	}
	restart := f.store.tasksOf(ports.TaskStartPayment)
	if len(restart) != 1 || restart[0].DedupKey != "start:2" {
		t.Fatalf("start tasks = %+v, want one for attempt 2", restart)
	}

	// The new attempt uses a new key.
	f.gateway.createResp = ports.GatewayPayment{Ref: "gw_2", Status: payment.StatusProcessing}
	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if key := f.gateway.creates[len(f.gateway.creates)-1].IdempotencyKey; key != p.ID.String()+":2" {
		t.Fatalf("new attempt key = %q, want <payment id>:2", key)
	}
	want := []string{reconcileNotFound, reconcileNotFound, reconcileAbandoned}
	if !slices.Equal(f.metrics.reconciled, want) {
		t.Fatalf("reconcile outcomes = %v, want %v", f.metrics.reconciled, want)
	}
}

func TestReconcileLookupFailureIsRetriedNotCounted(t *testing.T) {
	f := newFixture(testConfig)
	p := ambiguousStart(t, f)
	f.gateway.findErr = ports.ErrGatewayUnavailable

	err := reconcile(t, f, p.ID, 1)

	var later *ports.RetryLaterError
	var perm *ports.PermanentError
	if err == nil || errors.As(err, &later) || errors.As(err, &perm) {
		t.Fatalf("err = %v, want a plain retryable error", err)
	}
	a, _ := memAttempts{f.store}.Get(context.Background(), p.ID, 1)
	if a.State != ports.AttemptUnknown || a.NotFound != 0 {
		t.Fatalf("attempt = %+v, want still unknown with no not-found counted", a)
	}
}

func TestReconcileDuplicateNeedsAnOperator(t *testing.T) {
	f := newFixture(testConfig)
	p := ambiguousStart(t, f)
	f.gateway.found = []ports.GatewayPayment{{Ref: "gw_1", Status: payment.StatusSucceeded}, {Ref: "gw_2", Status: payment.StatusSucceeded}}

	var perm *ports.PermanentError
	if err := reconcile(t, f, p.ID, 1); !errors.As(err, &perm) {
		t.Fatalf("err = %v, want permanent", err)
	}
}

// The v1 start never records attempts.
func TestInTxStartRecordsNoAttempts(t *testing.T) {
	f := newFixture(v1Config())
	p := f.seed(payment.StatusPending)
	f.gateway.createResp = ports.GatewayPayment{Ref: "gw_1", Status: payment.StatusProcessing}

	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if states := f.store.attemptStates(p.ID); len(states) != 0 {
		t.Fatalf("attempts = %v, want none", states)
	}
	if key := f.gateway.creates[0].IdempotencyKey; key != p.ID.String() {
		t.Fatalf("v1 key = %q, want the payment id", key)
	}
}
