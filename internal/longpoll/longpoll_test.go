package longpoll

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

type fakeVersions struct {
	mu       sync.Mutex
	versions map[uuid.UUID]int64
	err      error
	calls    chan []uuid.UUID
}

func (f *fakeVersions) Versions(_ context.Context, ids []uuid.UUID) (map[uuid.UUID]int64, error) {
	if f.calls != nil {
		f.calls <- ids
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	out := map[uuid.UUID]int64{}
	for _, id := range ids {
		if v, ok := f.versions[id]; ok {
			out[id] = v
		}
	}
	return out, nil
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// received reports whether ch already holds a value, without blocking.
func received(ch <-chan int64) (int64, bool) {
	select {
	case v, ok := <-ch:
		return v, ok
	default:
		return 0, false
	}
}

func TestPollWakesOnlyWaitersBehindTheVersion(t *testing.T) {
	reg := NewRegistry(10)
	id := uuid.New()
	src := &fakeVersions{versions: map[uuid.UUID]int64{id: 3}}
	b := NewBatcher(reg, src, BatcherConfig{Interval: time.Hour, QueryTimeout: time.Second}, discard())

	behind, cancelBehind, _ := reg.Watch(id, 2)
	defer cancelBehind()
	current, cancelCurrent, _ := reg.Watch(id, 3)
	defer cancelCurrent()

	if err := b.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	if v, ok := received(behind); !ok || v != 3 {
		t.Fatalf("waiter at v2: got %d, %v; want woken with 3", v, ok)
	}
	if _, ok := received(current); ok {
		t.Fatal("waiter at v3 must keep waiting")
	}
	if reg.Len() != 1 {
		t.Fatalf("Len = %d, want 1 (the woken waiter is removed)", reg.Len())
	}
}

func TestPollKeepsWaitersOfMissingIDs(t *testing.T) {
	reg := NewRegistry(10)
	b := NewBatcher(reg, &fakeVersions{}, BatcherConfig{Interval: time.Hour, QueryTimeout: time.Second}, discard())
	ch, cancel, _ := reg.Watch(uuid.New(), 1)
	defer cancel()

	if err := b.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := received(ch); ok || reg.Len() != 1 {
		t.Fatal("a waiter whose payment is not visible yet must keep waiting")
	}
}

func TestPollErrorKeepsWaiters(t *testing.T) {
	reg := NewRegistry(10)
	b := NewBatcher(reg, &fakeVersions{err: errors.New("db down")}, BatcherConfig{Interval: time.Hour, QueryTimeout: time.Second}, discard())
	_, cancel, _ := reg.Watch(uuid.New(), 1)
	defer cancel()

	if err := b.Poll(context.Background()); err == nil {
		t.Fatal("want the source error")
	}
	if reg.Len() != 1 {
		t.Fatal("waiters must survive a failed batch")
	}
}

func TestPollWithoutWaitersDoesNotQuery(t *testing.T) {
	src := &fakeVersions{calls: make(chan []uuid.UUID, 1)}
	b := NewBatcher(NewRegistry(10), src, BatcherConfig{Interval: time.Hour, QueryTimeout: time.Second}, discard())

	if err := b.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(src.calls) != 0 {
		t.Fatal("an empty registry must not hit the database")
	}
}

func TestWatchRespectsLimit(t *testing.T) {
	reg := NewRegistry(1)
	_, cancel, err := reg.Watch(uuid.New(), 0)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := reg.Watch(uuid.New(), 0); !errors.Is(err, ports.ErrTooManyWaiters) {
		t.Fatalf("err = %v, want ErrTooManyWaiters", err)
	}
	cancel()
	if _, _, err := reg.Watch(uuid.New(), 0); err != nil {
		t.Fatalf("a slot freed by cancel must be reusable: %v", err)
	}
}

func TestCancelIsIdempotent(t *testing.T) {
	reg := NewRegistry(10)
	id := uuid.New()
	_, cancel, _ := reg.Watch(id, 0)
	reg.Notify(id, 1)

	cancel()
	cancel()

	if reg.Len() != 0 {
		t.Fatalf("Len = %d, want 0", reg.Len())
	}
}

func TestCloseReleasesWaitersAndRejectsNewOnes(t *testing.T) {
	reg := NewRegistry(10)
	ch, cancel, _ := reg.Watch(uuid.New(), 0)
	defer cancel()

	reg.Close()

	if v, ok := <-ch; ok {
		t.Fatalf("got %d, want a closed channel", v)
	}
	if _, _, err := reg.Watch(uuid.New(), 0); !errors.Is(err, ports.ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	reg.Close() // second close is a no-op
}

func TestRunPollsUntilCancelled(t *testing.T) {
	reg := NewRegistry(10)
	_, cancelWatch, _ := reg.Watch(uuid.New(), 0)
	defer cancelWatch()
	src := &fakeVersions{calls: make(chan []uuid.UUID)}
	b := NewBatcher(reg, src, BatcherConfig{Interval: time.Millisecond, QueryTimeout: time.Second}, discard())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- b.Run(ctx) }()

	<-src.calls // the loop reached the source at least once
	cancel()
	// Drain a call that may already be in flight so Run can observe ctx.
	for {
		select {
		case <-src.calls:
			continue
		case err := <-done:
			if err != nil {
				t.Fatalf("Run = %v, want nil on cancellation", err)
			}
			return
		}
	}
}

func TestConcurrentWatchNotifyCancel(t *testing.T) {
	reg := NewRegistry(500)
	id := uuid.New()
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, cancel, err := reg.Watch(id, int64(i%3))
			if err != nil {
				t.Error(err)
				return
			}
			defer cancel()
			reg.Notify(id, 2)
			received(ch)
		}()
	}
	wg.Wait()
	reg.Notify(id, 100)
	if reg.Len() != 0 {
		t.Fatalf("Len = %d, want 0", reg.Len())
	}
}
