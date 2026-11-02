package ports

import "errors"

var (
	// ErrLeaseLost means the task was re-claimed by another worker after our
	// lease expired, so our result must not be recorded.
	ErrLeaseLost = errors.New("task lease lost")

	// ErrTooManyWaiters means the long-poll registry of this instance is
	// full. The HTTP layer answers 429 with Retry-After.
	ErrTooManyWaiters = errors.New("too many waiters")

	// ErrUnavailable means the instance is shutting down or otherwise cannot
	// serve the request right now. The HTTP layer answers 503.
	ErrUnavailable = errors.New("temporarily unavailable")
)
