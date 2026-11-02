package ports

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// AttemptState is the lifecycle state of a gateway attempt.
type AttemptState string

// Attempt states.
const (
	// AttemptIntent is recorded and committed before the call. The call may
	// or may not have happened.
	AttemptIntent AttemptState = "intent"
	// AttemptDone means the gateway's answer was applied to the payment.
	AttemptDone AttemptState = "done"
	// AttemptUnknown means the call ended ambiguously; reconcile decides.
	AttemptUnknown AttemptState = "unknown"
	// AttemptAbandoned means the gateway certainly has no payment for the
	// attempt, so a new attempt with a new key may follow.
	AttemptAbandoned AttemptState = "abandoned"
)

// AttemptStates lists every state.
var AttemptStates = []AttemptState{AttemptIntent, AttemptDone, AttemptUnknown, AttemptAbandoned}

// Open reports whether the attempt still waits for an outcome.
func (s AttemptState) Open() bool {
	return s == AttemptIntent || s == AttemptUnknown
}

// Attempt is one call, made or about to be made, to create a payment at the
// gateway.
type Attempt struct {
	PaymentID      uuid.UUID
	Number         int
	IdempotencyKey string
	State          AttemptState
	GatewayRef     string
	// NotFound counts certain "not found" answers seen by reconcile.
	NotFound  int
	CreatedAt time.Time
	UpdatedAt time.Time
}

// AttemptKey is the idempotency key of attempt n: stable while the attempt
// is retried, different for a deliberate new attempt.
func AttemptKey(paymentID uuid.UUID, n int) string {
	return fmt.Sprintf("%s:%d", paymentID, n)
}

// Attempts records calls to the gateway before they happen. Every write runs
// in a transaction that holds the payment's row lock, which serializes all
// changes to one payment's attempts.
type Attempts interface {
	// Open returns the payment's open attempt (intent or unknown), if any.
	Open(ctx context.Context, paymentID uuid.UUID) (Attempt, bool, error)
	// Get returns attempt number n of the payment.
	Get(ctx context.Context, paymentID uuid.UUID, n int) (Attempt, error)
	// Create inserts the payment's next attempt in the intent state.
	Create(ctx context.Context, paymentID uuid.UUID, now time.Time) (Attempt, error)
	// Update writes the state, gateway reference, not-found count and
	// update time if the stored state is still from.
	Update(ctx context.Context, a Attempt, from AttemptState) error
}
