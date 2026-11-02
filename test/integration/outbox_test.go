//go:build integration

package integration

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/adapters/in/outbox"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/app"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// recordEvents writes n outbox events outside any transaction.
func recordEvents(t *testing.T, e *env, n int) {
	t.Helper()
	for range n {
		p := newPayment(t, uuid.NewString())
		if err := e.outbox.Record(context.Background(), payment.NewEvent(payment.EventCreated, p)); err != nil {
			t.Fatal(err)
		}
	}
}

func relayConfig(batch int) outbox.Config {
	return outbox.Config{Interval: time.Hour, Batch: batch, Lease: time.Minute, PublishTimeout: 5 * time.Second}
}

// publisherFunc adapts a function to outbox.Publisher.
type publisherFunc func(ctx context.Context, r ports.OutboxRecord) error

func (f publisherFunc) Publish(ctx context.Context, r ports.OutboxRecord) error { return f(ctx, r) }

func TestOutboxClaimLeaseAndMark(t *testing.T) {
	e := newEnv(t, app.Config{})
	ctx := context.Background()
	recordEvents(t, e, 3)
	t0 := e.clock.Now()

	first, err := e.outbox.Claim(ctx, t0, 2, 45*time.Second)
	if err != nil || len(first) != 2 || first[0].ID > first[1].ID {
		t.Fatalf("first claim = %+v, %v; want two events in id order", first, err)
	}
	rest, _ := e.outbox.Claim(ctx, t0.Add(time.Second), 10, 45*time.Second)
	if len(rest) != 1 {
		t.Fatalf("second claim got %d events, want only the unclaimed one", len(rest))
	}
	if again, _ := e.outbox.Claim(ctx, t0.Add(10*time.Second), 10, 45*time.Second); len(again) != 0 {
		t.Fatalf("claimed %d events whose lease is still running", len(again))
	}

	if err := e.outbox.MarkPublished(ctx, []int64{first[0].ID}, t0); err != nil {
		t.Fatal(err)
	}
	if err := e.outbox.Release(ctx, []int64{rest[0].ID}); err != nil {
		t.Fatal(err)
	}
	released, _ := e.outbox.Claim(ctx, t0.Add(11*time.Second), 10, 45*time.Second)
	if len(released) != 1 || released[0].ID != rest[0].ID {
		t.Fatalf("after release got %+v, want the released event at once", released)
	}
	expired, _ := e.outbox.Claim(ctx, t0.Add(46*time.Second), 10, 45*time.Second)
	if len(expired) != 1 || expired[0].ID != first[1].ID {
		t.Fatalf("after expiry got %+v, want the event whose lease ran out", expired)
	}
	if n := count(t, `SELECT count(*) FROM outbox WHERE published_at IS NOT NULL`); n != 1 {
		t.Fatalf("published rows = %d, want 1", n)
	}
}

// A failed delivery marks what went out before it and releases the rest,
// which the next batch publishes in order.
func TestRelayFailureMarksOnlyPublishedEvents(t *testing.T) {
	e := newEnv(t, app.Config{})
	recordEvents(t, e, 3)
	var seen []int64
	fail := true
	pub := publisherFunc(func(_ context.Context, r ports.OutboxRecord) error {
		if r.ID == 2 && fail {
			fail = false
			return errors.New("subscriber down")
		}
		seen = append(seen, r.ID)
		return nil
	})
	relay := outbox.NewRelay(e.outbox, pub, e.clock, nil, relayConfig(10), discard())

	if n, err := relay.RunOnce(context.Background()); err == nil || n != 1 {
		t.Fatalf("first batch = %d, %v; want 1 and an error", n, err)
	}
	if n := count(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL AND claimed_until IS NULL`); n != 2 {
		t.Fatalf("released rows = %d, want 2", n)
	}
	if n, err := relay.RunOnce(context.Background()); err != nil || n != 2 {
		t.Fatalf("second batch = %d, %v", n, err)
	}
	if !slices.Equal(seen, []int64{1, 2, 3}) {
		t.Fatalf("published %v, want 1, 2, 3", seen)
	}
}

// The relay holds no database connection while it publishes: a slow
// subscriber cannot pin a connection or hit the idle-in-transaction limit.
func TestRelayHoldsNoConnectionWhilePublishing(t *testing.T) {
	e := newEnv(t, app.Config{})
	recordEvents(t, e, 2)
	var acquired []int32
	pub := publisherFunc(func(context.Context, ports.OutboxRecord) error {
		acquired = append(acquired, pool.Stat().AcquiredConns())
		return nil
	})

	if _, err := outbox.NewRelay(e.outbox, pub, e.clock, nil, relayConfig(10), discard()).RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(acquired, []int32{0, 0}) {
		t.Fatalf("connections acquired during deliveries = %v, want none", acquired)
	}
}

// Two relays running at the same time claim disjoint batches, so no event
// is published twice.
func TestConcurrentRelaysPublishEachEventOnce(t *testing.T) {
	e := newEnv(t, app.Config{})
	recordEvents(t, e, 6)

	var mu sync.Mutex
	counts := map[int64]int{}
	record := func(id int64) {
		mu.Lock()
		counts[id]++
		mu.Unlock()
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	slow := publisherFunc(func(_ context.Context, r ports.OutboxRecord) error {
		once.Do(func() { close(started) })
		<-release // hold the first batch until the second relay has run
		record(r.ID)
		return nil
	})
	fast := publisherFunc(func(_ context.Context, r ports.OutboxRecord) error {
		record(r.ID)
		return nil
	})

	done := make(chan error)
	go func() {
		_, err := outbox.NewRelay(e.outbox, slow, e.clock, nil, relayConfig(3), discard()).RunOnce(context.Background())
		done <- err
	}()
	<-started
	if n, err := outbox.NewRelay(e.outbox, fast, e.clock, nil, relayConfig(10), discard()).RunOnce(context.Background()); err != nil || n != 3 {
		t.Fatalf("second relay published %d, %v; want the 3 unclaimed events", n, err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if len(counts) != 6 {
		t.Fatalf("published %d distinct events, want 6", len(counts))
	}
	for id, n := range counts {
		if n != 1 {
			t.Fatalf("event %d published %d times", id, n)
		}
	}
	if n := count(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL`); n != 0 {
		t.Fatalf("unpublished rows = %d, want 0", n)
	}
}
