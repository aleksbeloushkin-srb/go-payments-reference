package app

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// Regression tests for the double-charge and stuck-attempt paths found in
// the v2 review. Each one replays the failing sequence and checks how many
// payments the gateway executed.

// A v1 (in_tx) call executes at the gateway and its commit is lost; the
// service then runs the two-phase start. Attempt 1 sends the v1 key, so the
// gateway deduplicates the retry.
func TestModeSwitchInTxToTwoPhaseExecutesOnce(t *testing.T) {
	f, gw := newTwoPhaseFixture()
	f.svc.cfg.StartMode = StartInTx
	p := f.seed(payment.StatusPending)
	gw.crashAfter = true
	if crashed, _ := survive(func() error { return f.svc.StartPayment(context.Background(), p.ID) }); !crashed {
		t.Fatal("want the in_tx run to crash after the call")
	}

	f.svc.cfg.StartMode = StartTwoPhase // deploy v2
	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}

	if n := gw.executions(); n != 1 {
		t.Fatalf("gateway executions = %d, want 1 across the mode switch", n)
	}
	if got := f.store.payments[p.ID].Status; got != payment.StatusProcessing {
		t.Fatalf("status = %s", got)
	}
}

// A two-phase call executes and crashes before tx2; the service then runs in
// in_tx mode. A payment with attempts is left to the two-phase start, which
// reuses the open intent and its key.
func TestModeSwitchTwoPhaseToInTxExecutesOnce(t *testing.T) {
	f, gw := newTwoPhaseFixture()
	p := f.seed(payment.StatusPending)
	gw.crashAfter = true
	if crashed, _ := survive(func() error { return f.svc.StartPayment(context.Background(), p.ID) }); !crashed {
		t.Fatal("want the two_phase run to crash after the call")
	}

	f.svc.cfg.StartMode = StartInTx // roll back to v1
	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}

	if n := gw.executions(); n != 1 {
		t.Fatalf("gateway executions = %d, want 1 across the mode switch", n)
	}
	if states := f.store.attemptStates(p.ID); !slices.Equal(states, []ports.AttemptState{ports.AttemptDone}) {
		t.Fatalf("attempts = %v, want the open attempt finished", states)
	}
}

// A crash after the call, then the gateway's own event, then the retry: the
// event closes the attempt, so nothing stays open for the stuck-attempt
// alert.
func TestCrashThenGatewayEventClosesTheAttempt(t *testing.T) {
	f, gw := newTwoPhaseFixture()
	p := f.seed(payment.StatusPending)
	gw.crashAfter = true
	if crashed, _ := survive(func() error { return f.svc.StartPayment(context.Background(), p.ID) }); !crashed {
		t.Fatal("want a crash after the call")
	}
	event, _ := json.Marshal(GatewayEvent{ID: "evt_1", PaymentID: p.ID, GatewayRef: "gw_" + p.ID.String(), Status: payment.StatusSucceeded})
	if err := f.svc.ApplyGatewayEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if states := f.store.attemptStates(p.ID); !slices.Equal(states, []ports.AttemptState{ports.AttemptDone}) {
		t.Fatalf("attempts after the event = %v, want done", states)
	}

	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if n := gw.executions(); n != 1 {
		t.Fatalf("gateway executions = %d, want 1", n)
	}
	if got := f.store.payments[p.ID].Status; got != payment.StatusSucceeded {
		t.Fatalf("status = %s", got)
	}
}

// A payment that left pending without a gateway reference gets its open
// attempt reconciled instead of closed blindly.
func TestStartRetryReconcilesAnOpenAttemptWithoutReference(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusPending)
	if _, err := (memAttempts{f.store}).Create(context.Background(), p.ID, f.clock.now); err != nil {
		t.Fatal(err)
	}
	f.store.mu.Lock()
	q := f.store.payments[p.ID]
	q.Status, q.Version = payment.StatusCanceled, 2
	f.store.payments[p.ID] = q
	f.store.mu.Unlock()

	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if states := f.store.attemptStates(p.ID); !slices.Equal(states, []ports.AttemptState{ports.AttemptUnknown}) {
		t.Fatalf("attempts = %v, want unknown", states)
	}
	if n := len(f.store.tasksOf(ports.TaskReconcilePayment)); n != 1 {
		t.Fatalf("reconcile tasks = %d, want 1", n)
	}
}

// laggingGateway executes like dedupGateway, answers its first create
// ambiguously, and its lookup shows payments only once visible is set.
type laggingGateway struct {
	*dedupGateway
	visible   bool
	ambiguous bool
}

func (g *laggingGateway) CreatePayment(ctx context.Context, req ports.GatewayRequest) (ports.GatewayPayment, error) {
	res, err := g.dedupGateway.CreatePayment(ctx, req)
	if err == nil && g.ambiguous {
		g.ambiguous = false
		return ports.GatewayPayment{}, ports.ErrGatewayAmbiguous
	}
	return res, err
}

func (g *laggingGateway) FindByReference(ctx context.Context, ref string) ([]ports.GatewayPayment, error) {
	if !g.visible {
		return []ports.GatewayPayment{}, nil
	}
	return g.dedupGateway.FindByReference(ctx, ref)
}

// The gateway executes attempt 1 but its lookup shows it only after the
// attempt was abandoned; attempt 2 executes too. The delayed duplicate check
// after attempt 2 finds both, counts the duplicate and goes dead for an
// operator.
func TestAbandonedAttemptThatExecutedIsDetected(t *testing.T) {
	f := newFixture(testConfig)
	gw := &laggingGateway{dedupGateway: newDedupGateway(), ambiguous: true}
	f.svc.gateway = gw
	p := f.seed(payment.StatusPending)
	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	for range testConfig.ReconcileNotFoundLimit {
		_ = reconcile(t, f, p.ID, 1)
	}
	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil { // the start:2 task
		t.Fatal(err)
	}
	if gw.executions() != 2 {
		t.Fatalf("executions = %d, want the double charge this test is about", gw.executions())
	}

	checks := f.store.tasksOf(ports.TaskCheckDuplicates)
	if len(checks) != 1 || checks[0].DedupKey != "duplicates:2" ||
		!checks[0].RunAfter.Equal(f.clock.now.Add(testConfig.DuplicateCheckDelay)) {
		t.Fatalf("duplicate checks = %+v, want one for attempt 2 after the delay", checks)
	}

	gw.visible = true // the gateway's lookup catches up
	err := f.svc.CheckDuplicates(context.Background(), p.ID, checks[0].Payload)
	var perm *ports.PermanentError
	if !errors.As(err, &perm) {
		t.Fatalf("err = %v, want a dead task for an operator", err)
	}
	if f.metrics.duplicates != 1 {
		t.Fatalf("duplicates detected = %d, want 1", f.metrics.duplicates)
	}
}

// A definite answer that arrives for an abandoned attempt means the gateway
// executed a key we gave up on: it is not applied, and a duplicate check
// runs at once.
func TestAnswerForAbandonedAttemptChecksForDuplicates(t *testing.T) {
	f, gw := newTwoPhaseFixture()
	p := f.seed(payment.StatusPending)
	gw.during = func() {
		// While the call is in flight, reconciliation gives the attempt up.
		f.store.mu.Lock()
		k := attemptID{p.ID, 1}
		a := f.store.attempts[k]
		a.State = ports.AttemptAbandoned
		f.store.attempts[k] = a
		f.store.mu.Unlock()
	}

	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if got := f.store.payments[p.ID].Status; got != payment.StatusPending {
		t.Fatalf("status = %s, want the answer for the abandoned attempt not applied", got)
	}
	checks := f.store.tasksOf(ports.TaskCheckDuplicates)
	if len(checks) != 1 || !checks[0].RunAfter.Equal(f.clock.now) {
		t.Fatalf("duplicate checks = %+v, want one, due now", checks)
	}
}

// The duplicate check compares the gateway's charges with our record.
func TestCheckDuplicatesOutcomes(t *testing.T) {
	payload, _ := json.Marshal(attemptPayload{Attempt: 2})
	charge := func(ref string, st payment.Status) ports.GatewayPayment {
		return ports.GatewayPayment{Ref: ref, Status: st}
	}
	tests := []struct {
		name         string
		status       payment.Status
		ref          string
		found        []ports.GatewayPayment
		findErr      error
		wantErr      bool
		wantPerm     bool
		wantLater    bool
		wantDup      int
		wantMismatch int
	}{
		{name: "our charge only", status: payment.StatusSucceeded, ref: "gw_1",
			found: []ports.GatewayPayment{charge("gw_1", payment.StatusSucceeded)}},
		{name: "a declined extra payment charges nothing", status: payment.StatusSucceeded, ref: "gw_1",
			found: []ports.GatewayPayment{charge("gw_1", payment.StatusSucceeded), charge("gw_2", payment.StatusFailed)}},
		{name: "two charges", status: payment.StatusSucceeded, ref: "gw_1",
			found:   []ports.GatewayPayment{charge("gw_1", payment.StatusSucceeded), charge("gw_2", payment.StatusProcessing)},
			wantErr: true, wantPerm: true, wantDup: 1},
		{name: "lookup lags behind our charge: retried", status: payment.StatusSucceeded, ref: "gw_1",
			found: []ports.GatewayPayment{}, wantErr: true},
		{name: "lookup fails: retried", status: payment.StatusSucceeded, ref: "gw_1",
			findErr: ports.ErrGatewayUnavailable, wantErr: true},
		{name: "failed and nothing charged is correct", status: payment.StatusFailed,
			found: []ports.GatewayPayment{}},
		{name: "failed but the gateway holds a charge", status: payment.StatusFailed,
			found:   []ports.GatewayPayment{charge("gw_1", payment.StatusSucceeded)},
			wantErr: true, wantPerm: true, wantMismatch: 1},
		{name: "canceled but the gateway shows our ref as charged", status: payment.StatusCanceled, ref: "gw_1",
			found:   []ports.GatewayPayment{charge("gw_1", payment.StatusProcessing)},
			wantErr: true, wantPerm: true, wantMismatch: 1},
		{name: "still pending without a reference: later", status: payment.StatusPending,
			wantErr: true, wantLater: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(testConfig)
			p := f.seed(tt.status)
			q := f.store.payments[p.ID]
			q.GatewayRef = tt.ref
			f.store.payments[p.ID] = q
			f.gateway.found, f.gateway.findErr = tt.found, tt.findErr

			err := f.svc.CheckDuplicates(context.Background(), p.ID, payload)

			var perm *ports.PermanentError
			var later *ports.RetryLaterError
			if (err != nil) != tt.wantErr || errors.As(err, &perm) != tt.wantPerm || errors.As(err, &later) != tt.wantLater {
				t.Fatalf("err = %v, wantErr %v, permanent %v, later %v", err, tt.wantErr, tt.wantPerm, tt.wantLater)
			}
			if f.metrics.duplicates != tt.wantDup || f.metrics.mismatches != tt.wantMismatch {
				t.Fatalf("duplicates = %d, mismatches = %d; want %d and %d", f.metrics.duplicates, f.metrics.mismatches, tt.wantDup, tt.wantMismatch)
			}
		})
	}
}

// decliningGateway declines every create once decline is set, without
// executing anything.
type decliningGateway struct {
	*laggingGateway
	decline bool
}

func (g *decliningGateway) CreatePayment(ctx context.Context, req ports.GatewayRequest) (ports.GatewayPayment, error) {
	if g.decline {
		return ports.GatewayPayment{}, ports.ErrGatewayRejected
	}
	return g.laggingGateway.CreatePayment(ctx, req)
}

// abandonFirstAttempt runs an ambiguous attempt 1 through the not-found
// limit, so that it is abandoned.
func abandonFirstAttempt(t *testing.T, f *fixture, p payment.Payment) {
	t.Helper()
	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	for range testConfig.ReconcileNotFoundLimit {
		_ = reconcile(t, f, p.ID, 1)
	}
	if states := f.store.attemptStates(p.ID); !slices.Equal(states, []ports.AttemptState{ports.AttemptAbandoned}) {
		t.Fatalf("attempts = %v, want attempt 1 abandoned", states)
	}
}

// Attempt 1 executes late (after it was abandoned) and attempt 2 is
// declined: we report failed while the client was charged. The duplicate
// check must report that, not "no duplicate".
func TestLateExecutionThenDeclinedNextAttemptIsAnIncident(t *testing.T) {
	f := newFixture(testConfig)
	gw := &decliningGateway{laggingGateway: &laggingGateway{dedupGateway: newDedupGateway(), ambiguous: true}}
	f.svc.gateway = gw
	p := f.seed(payment.StatusPending)
	abandonFirstAttempt(t, f, p)

	gw.decline = true // issuers often decline a repeated identical charge
	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if got := f.store.payments[p.ID].Status; got != payment.StatusFailed {
		t.Fatalf("status = %s, want failed", got)
	}
	checks := f.store.tasksOf(ports.TaskCheckDuplicates)
	if len(checks) != 1 {
		t.Fatalf("duplicate checks = %d, want 1", len(checks))
	}

	gw.visible = true // the late payment for attempt 1 shows up
	err := f.svc.CheckDuplicates(context.Background(), p.ID, checks[0].Payload)

	var perm *ports.PermanentError
	if !errors.As(err, &perm) || f.metrics.mismatches != 1 {
		t.Fatalf("err = %v, mismatches = %d; want a dead task and one mismatch", err, f.metrics.mismatches)
	}
	if gw.executions() != 1 {
		t.Fatalf("executions = %d, want the one late charge", gw.executions())
	}
}

// Attempt 1 is abandoned without executing and attempt 2 is declined: the
// gateway correctly holds nothing, and the check must not page anyone.
func TestNothingExecutedThenDeclinedIsCorrect(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusPending)
	f.gateway.createErr = ports.ErrGatewayAmbiguous
	f.gateway.found = []ports.GatewayPayment{}
	abandonFirstAttempt(t, f, p)

	f.gateway.createErr = ports.ErrGatewayRejected
	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	checks := f.store.tasksOf(ports.TaskCheckDuplicates)
	if len(checks) != 1 {
		t.Fatalf("duplicate checks = %d, want 1", len(checks))
	}

	if err := f.svc.CheckDuplicates(context.Background(), p.ID, checks[0].Payload); err != nil {
		t.Fatalf("err = %v, want nil: nothing was charged", err)
	}
	if f.metrics.duplicates != 0 || f.metrics.mismatches != 0 {
		t.Fatalf("duplicates = %d, mismatches = %d; want none", f.metrics.duplicates, f.metrics.mismatches)
	}
}

// A gateway that answers ambiguously to every create for one payment, and
// never shows it: each cycle abandons an attempt, until the cap. Then the
// reconcile task goes dead, no new attempt starts, and the payment stays
// pending for an operator.
func TestAttemptChainIsCapped(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusPending)
	f.gateway.createErr = ports.ErrGatewayAmbiguous
	f.gateway.found = []ports.GatewayPayment{}

	var last error
	for n := 1; n <= testConfig.MaxGatewayAttempts; n++ {
		if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
			t.Fatalf("attempt %d start: %v", n, err)
		}
		for range testConfig.ReconcileNotFoundLimit {
			last = reconcile(t, f, p.ID, n)
		}
		f.clock.now = f.clock.now.Add(time.Minute)
	}

	var perm *ports.PermanentError
	if !errors.As(last, &perm) {
		t.Fatalf("last reconcile = %v, want a dead task at the cap", last)
	}
	want := []ports.AttemptState{ports.AttemptAbandoned, ports.AttemptAbandoned, ports.AttemptAbandoned}
	if states := f.store.attemptStates(p.ID); !slices.Equal(states, want) {
		t.Fatalf("attempts = %v, want %v", states, want)
	}
	for _, task := range f.store.tasksOf(ports.TaskStartPayment) {
		if task.DedupKey == "start:4" {
			t.Fatal("a fourth attempt was scheduled past the cap")
		}
	}
	// A start run by hand past the cap does not call the gateway either.
	calls := len(f.gateway.creates)
	if err := f.svc.StartPayment(context.Background(), p.ID); !errors.As(err, &perm) {
		t.Fatalf("start past the cap = %v, want permanent", err)
	}
	if len(f.gateway.creates) != calls {
		t.Fatal("the gateway was called past the cap")
	}
	if got := f.store.payments[p.ID].Status; got != payment.StatusPending {
		t.Fatalf("status = %s, want pending", got)
	}
}

// 409, 425, or a connection lost after the request was written: the attempt
// keeps its key and the task retries; nothing is abandoned.
func TestResendSameKeyKeepsTheKey(t *testing.T) {
	f := newFixture(testConfig)
	p := f.seed(payment.StatusPending)
	f.gateway.createErr = errors.Join(ports.ErrGatewayAmbiguous, ports.ErrGatewayResendSameKey)

	err := f.svc.StartPayment(context.Background(), p.ID)

	var perm *ports.PermanentError
	if err == nil || errors.As(err, &perm) {
		t.Fatalf("err = %v, want a plain retryable error", err)
	}
	if states := f.store.attemptStates(p.ID); !slices.Equal(states, []ports.AttemptState{ports.AttemptIntent}) {
		t.Fatalf("attempts = %v, want the intent kept", states)
	}
	if n := len(f.store.tasksOf(ports.TaskReconcilePayment)); n != 0 {
		t.Fatalf("reconcile tasks = %d, want none", n)
	}

	f.gateway.createErr = nil
	f.gateway.createResp = ports.GatewayPayment{Ref: "gw_1", Status: payment.StatusProcessing}
	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if a, b := f.gateway.creates[0].IdempotencyKey, f.gateway.creates[1].IdempotencyKey; a != b {
		t.Fatalf("keys %q and %q, want the same key", a, b)
	}
}

// While an attempt is unknown, a start retry neither calls the gateway nor
// creates a second reconcile task: the existing one is deduplicated. A
// reconcile task that went dead is therefore requeued by an operator, not
// revived by the start task (see the GatewayAttemptStuck runbook).
func TestStartRetryLeavesAnUnknownAttemptToReconcile(t *testing.T) {
	f := newFixture(testConfig)
	p := ambiguousStart(t, f)
	tasks, calls := len(f.store.tasks), len(f.gateway.creates)

	if err := f.svc.StartPayment(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if len(f.store.tasks) != tasks || len(f.gateway.creates) != calls {
		t.Fatalf("tasks %d -> %d, calls %d -> %d; want no change", tasks, len(f.store.tasks), calls, len(f.gateway.creates))
	}
}

func TestNewServiceRejectsInvalidConfig(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"zero value":         func(c *Config) { *c = Config{} },
		"unknown start mode": func(c *Config) { c.StartMode = "eventually" },
		"no lookups":         func(c *Config) { c.ReconcileNotFoundLimit = 0 },
		"no attempts":        func(c *Config) { c.MaxGatewayAttempts = 0 },
		"no check delay":     func(c *Config) { c.DuplicateCheckDelay = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig
			mutate(&cfg)
			defer func() {
				if recover() == nil {
					t.Fatal("NewService accepted an invalid configuration")
				}
			}()
			newFixture(cfg)
		})
	}
}
