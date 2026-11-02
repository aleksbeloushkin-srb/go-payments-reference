package ports

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
)

// Clock returns the current time. Use cases never call time.Now directly, so
// tests can pin time.
type Clock interface {
	Now() time.Time
}

// TxRunner runs fn inside one database transaction carried in ctx.
// Repositories called with that ctx join the transaction, a nested Run joins
// the outer one, and fn returning an error rolls everything back.
type TxRunner interface {
	Run(ctx context.Context, fn func(ctx context.Context) error) error
}

// PaymentStore stores payments.
type PaymentStore interface {
	// Insert stores p unless a payment with the same idempotency key exists.
	// It returns the stored row and whether this call inserted it.
	Insert(ctx context.Context, p payment.Payment) (stored payment.Payment, inserted bool, err error)
	// Payment returns payment.ErrNotFound for an unknown id.
	Payment(ctx context.Context, id uuid.UUID) (payment.Payment, error)
	// Lock reads the payment and locks its row until the surrounding
	// transaction ends. It fails if ctx carries no transaction.
	Lock(ctx context.Context, id uuid.UUID) (payment.Payment, error)
	// Update persists p if the stored version still equals expectedVersion,
	// otherwise it returns payment.ErrVersionConflict.
	Update(ctx context.Context, p payment.Payment, expectedVersion int64) error
}

// Enqueuer schedules background tasks.
type Enqueuer interface {
	// Enqueue inserts t unless a task with the same kind, payment id and
	// dedup key exists; enqueued reports which case happened.
	Enqueue(ctx context.Context, t TaskSpec) (enqueued bool, err error)
}

// EventRecorder writes integration events to the outbox in the caller's
// transaction.
type EventRecorder interface {
	Record(ctx context.Context, e payment.Event) error
}

// Gateway is the external payment gateway.
type Gateway interface {
	// CreatePayment asks the gateway to execute a payment. The gateway
	// deduplicates by IdempotencyKey, so repeating the call is safe.
	CreatePayment(ctx context.Context, req GatewayRequest) (GatewayPayment, error)
	// Payment reads the gateway-side state by reference.
	Payment(ctx context.Context, ref string) (GatewayPayment, error)
	// FindByReference lists the gateway payments created for our reference
	// (the payment id). An empty list is a certain "not found"; an error
	// means the gateway could not tell.
	FindByReference(ctx context.Context, reference string) ([]GatewayPayment, error)
}

// Metrics receives business measurements. Use cases call it only after the
// transaction that made the change has committed, so rolled-back work is
// never counted.
type Metrics interface {
	PaymentCreated()
	// StatusChanged reports a committed status change; age is the time since
	// the payment was created.
	StatusChanged(from, to payment.Status, age time.Duration)
	// Reconciled reports how one reconciliation of an ambiguous gateway
	// attempt ended: found, not_found, abandoned, duplicate or error.
	Reconciled(outcome string)
	// DuplicateDetected reports that the gateway holds more than one
	// payment for one bank payment: the client was charged twice.
	DuplicateDetected()
	// ChargeMismatch reports that the gateway holds a charge our records do
	// not show: an executed payment for a payment we report failed or
	// canceled.
	ChargeMismatch()
}

// Watcher lets a use case wait until a payment's version moves past a marker
// the client has already seen.
type Watcher interface {
	// Watch subscribes to changes of id beyond seenVersion. The channel
	// receives the newer version once; it is closed without a value when the
	// watcher shuts down. cancel must always be called. Watch returns
	// ErrTooManyWaiters when the per-instance cap is reached.
	Watch(id uuid.UUID, seenVersion int64) (changes <-chan int64, cancel func(), err error)
}
