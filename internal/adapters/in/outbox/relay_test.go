package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

type fakeStore struct {
	pending  []ports.OutboxRecord
	marked   []int64
	released []int64
}

func (s *fakeStore) Claim(_ context.Context, _ time.Time, limit int, _ time.Duration) ([]ports.OutboxRecord, error) {
	n := min(limit, len(s.pending))
	out := s.pending[:n]
	s.pending = s.pending[n:]
	return out, nil
}

func (s *fakeStore) MarkPublished(_ context.Context, ids []int64, _ time.Time) error {
	s.marked = append(s.marked, ids...)
	return nil
}

func (s *fakeStore) Release(_ context.Context, ids []int64) error {
	s.released = append(s.released, ids...)
	return nil
}

type fakePublisher struct {
	failOn int64
	seen   []int64
	clock  *stepClock // advanced by every delivery when set
}

func (p *fakePublisher) Publish(_ context.Context, r ports.OutboxRecord) error {
	if p.clock != nil {
		p.clock.advance(20 * time.Second)
	}
	if r.ID == p.failOn {
		return errors.New("broker unavailable")
	}
	p.seen = append(p.seen, r.ID)
	return nil
}

type stepClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *stepClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func records(ids ...int64) []ports.OutboxRecord {
	out := make([]ports.OutboxRecord, len(ids))
	for i, id := range ids {
		out[i] = ports.OutboxRecord{ID: id, Type: "payment.created"}
	}
	return out
}

var testConfig = Config{Interval: time.Hour, Batch: 10, Lease: time.Minute, PublishTimeout: 5 * time.Second}

func newRelay(store Store, pub Publisher, clock ports.Clock) *Relay {
	return NewRelay(store, pub, clock, nil, testConfig, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func newClock() *stepClock { return &stepClock{now: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)} }

func TestRelayPublishesAndMarksInOrder(t *testing.T) {
	store := &fakeStore{pending: records(1, 2, 3)}
	pub := &fakePublisher{}

	n, err := newRelay(store, pub, newClock()).RunOnce(context.Background())

	if err != nil || n != 3 {
		t.Fatalf("RunOnce = %d, %v", n, err)
	}
	if !slices.Equal(pub.seen, []int64{1, 2, 3}) || !slices.Equal(store.marked, []int64{1, 2, 3}) || len(store.released) != 0 {
		t.Fatalf("published %v, marked %v, released %v", pub.seen, store.marked, store.released)
	}
}

func TestRelayStopsAtFirstFailureAndReleasesTheRest(t *testing.T) {
	store := &fakeStore{pending: records(1, 2, 3)}
	pub := &fakePublisher{failOn: 2}

	n, err := newRelay(store, pub, newClock()).RunOnce(context.Background())

	if err == nil || n != 1 {
		t.Fatalf("RunOnce = %d, %v; want 1 and an error", n, err)
	}
	if !slices.Equal(store.marked, []int64{1}) || !slices.Equal(store.released, []int64{2, 3}) {
		t.Fatalf("marked %v, released %v", store.marked, store.released)
	}
}

// A slow subscriber must not keep publishing past the lease: another relay
// could then claim the same events and publish them a second time.
func TestRelayStopsBeforeTheLeaseRunsOut(t *testing.T) {
	clock := newClock()
	store := &fakeStore{pending: records(1, 2, 3, 4, 5)}
	pub := &fakePublisher{clock: clock} // every delivery takes 20s

	n, err := newRelay(store, pub, clock).RunOnce(context.Background())

	// Lease 60s, timeout 5s: deliveries start at 0s, 20s and 40s; at 60s
	// there is no room for another one.
	if err != nil || n != 3 {
		t.Fatalf("RunOnce = %d, %v; want 3", n, err)
	}
	if !slices.Equal(store.released, []int64{4, 5}) {
		t.Fatalf("released %v, want the events the lease had no room for", store.released)
	}
}

func TestFanoutStopsAtFirstError(t *testing.T) {
	first, second := &fakePublisher{failOn: 7}, &fakePublisher{}
	if err := (Fanout{first, second}).Publish(context.Background(), ports.OutboxRecord{ID: 7}); err == nil {
		t.Fatal("want the first publisher's error")
	}
	if len(second.seen) != 0 {
		t.Fatal("the second publisher must not be called after a failure")
	}
}

func TestRelayRunStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- newRelay(&fakeStore{}, &fakePublisher{}, newClock()).Run(ctx) }()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
}
