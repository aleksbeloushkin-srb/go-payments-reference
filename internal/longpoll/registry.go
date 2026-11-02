package longpoll

import (
	"sync"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// Registry is the set of waiters of one instance. Its zero value is not
// usable; call NewRegistry.
type Registry struct {
	mu      sync.Mutex
	waiters map[uuid.UUID]map[*waiter]struct{}
	count   int
	limit   int
	closed  bool
}

type waiter struct {
	seen int64
	// ch has capacity 1 and receives at most one value, so Notify never
	// blocks on a waiter whose client has already gone.
	ch chan int64
}

// NewRegistry returns a registry that accepts at most limit waiters.
func NewRegistry(limit int) *Registry {
	return &Registry{waiters: make(map[uuid.UUID]map[*waiter]struct{}), limit: limit}
}

// Watch registers a waiter for id. See ports.Watcher.
func (r *Registry) Watch(id uuid.UUID, seen int64) (<-chan int64, func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, nil, ports.ErrUnavailable
	}
	if r.count >= r.limit {
		return nil, nil, ports.ErrTooManyWaiters
	}
	w := &waiter{seen: seen, ch: make(chan int64, 1)}
	set := r.waiters[id]
	if set == nil {
		set = make(map[*waiter]struct{})
		r.waiters[id] = set
	}
	set[w] = struct{}{}
	r.count++
	return w.ch, func() { r.remove(id, w) }, nil
}

// remove deregisters w. It is idempotent: the waiter may already have been
// removed by Notify or Close.
func (r *Registry) remove(id uuid.UUID, w *waiter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.drop(id, w)
}

// drop deletes w from the registry. The caller holds r.mu.
func (r *Registry) drop(id uuid.UUID, w *waiter) {
	set, ok := r.waiters[id]
	if !ok {
		return
	}
	if _, ok := set[w]; !ok {
		return
	}
	delete(set, w)
	if len(set) == 0 {
		delete(r.waiters, id)
	}
	r.count--
}

// IDs returns the payment ids that currently have waiters.
func (r *Registry) IDs() []uuid.UUID {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]uuid.UUID, 0, len(r.waiters))
	for id := range r.waiters {
		ids = append(ids, id)
	}
	return ids
}

// Notify wakes and removes every waiter of id that has seen an older
// version. It returns the number of waiters woken.
func (r *Registry) Notify(id uuid.UUID, version int64) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	woken := 0
	for w := range r.waiters[id] {
		if version > w.seen {
			w.ch <- version
			r.drop(id, w)
			woken++
		}
	}
	return woken
}

// Len returns the number of registered waiters.
func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

// Close wakes every waiter without a value and rejects new ones. The HTTP
// server calls it when shutdown starts, so long polls answer "not modified"
// at once instead of holding the drain for the whole soft timeout.
func (r *Registry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	for id, set := range r.waiters {
		for w := range set {
			close(w.ch)
		}
		delete(r.waiters, id)
	}
	r.count = 0
}
