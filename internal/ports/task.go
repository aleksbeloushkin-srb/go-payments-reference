package ports

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// TaskKind names a background command.
type TaskKind string

// Task kinds.
const (
	TaskStartPayment      TaskKind = "start_payment"
	TaskPollPayment       TaskKind = "poll_payment"
	TaskApplyGatewayEvent TaskKind = "apply_gateway_event"
	TaskReconcilePayment  TaskKind = "reconcile_payment"
)

// TaskKinds lists every kind. A test checks that each one has a handler.
var TaskKinds = []TaskKind{TaskStartPayment, TaskPollPayment, TaskApplyGatewayEvent, TaskReconcilePayment}

// TaskSpec describes a task to schedule.
type TaskSpec struct {
	Kind      TaskKind
	PaymentID uuid.UUID
	// DedupKey makes enqueueing idempotent: (Kind, PaymentID, DedupKey) is
	// unique and a second insert is ignored.
	DedupKey string
	Payload  json.RawMessage
	RunAfter time.Time
}

// Task is a claimed command handed to a handler.
type Task struct {
	ID        int64
	Kind      TaskKind
	PaymentID uuid.UUID
	DedupKey  string
	Payload   json.RawMessage
	// Attempts counts failed runs so far.
	Attempts int
	RunAfter time.Time
	// LeaseUntil is the lease deadline set at claim time. The queue also uses
	// it as a fencing token: settling a task whose lease was taken over by
	// another worker fails with ErrLeaseLost.
	LeaseUntil time.Time
}

// RetryLaterError asks the worker to run the same task again after a delay
// without counting a failed attempt. Polling uses it: "not final yet" is not
// a failure.
type RetryLaterError struct {
	After  time.Duration
	Reason string
}

func (e *RetryLaterError) Error() string {
	return fmt.Sprintf("retry in %s: %s", e.After, e.Reason)
}

// RetryLater returns a *RetryLaterError.
func RetryLater(after time.Duration, reason string) error {
	return &RetryLaterError{After: after, Reason: reason}
}

// RetryAfterError is a failed attempt that also says when to try again, for
// example a 429 whose Retry-After is longer than the client waits in-line.
// Unlike RetryLaterError it counts as a failed attempt; the worker waits at
// least After, or longer if its own backoff says so.
type RetryAfterError struct {
	After time.Duration
	Err   error
}

func (e *RetryAfterError) Error() string { return e.Err.Error() }

// Unwrap returns the wrapped error.
func (e *RetryAfterError) Unwrap() error { return e.Err }

// PermanentError marks a failure that retrying cannot fix. The worker moves
// the task to the dead state at once.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return "permanent: " + e.Err.Error() }

// Unwrap returns the wrapped error.
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err in a *PermanentError.
func Permanent(err error) error {
	return &PermanentError{Err: err}
}
