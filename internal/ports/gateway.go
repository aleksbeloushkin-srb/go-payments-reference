package ports

import (
	"errors"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
)

// GatewayRequest is what the gateway needs to execute a payment.
type GatewayRequest struct {
	// IdempotencyKey must be stable across retries of the same payment. We
	// use the payment id.
	IdempotencyKey string
	PaymentID      uuid.UUID
	MerchantID     string
	Amount         payment.Money
}

// GatewayPayment is the gateway-side view of a payment. The adapter
// translates the gateway's wire vocabulary into domain statuses.
type GatewayPayment struct {
	Ref    string
	Status payment.Status
}

// Gateway error classes. Adapters wrap one of them so use cases can decide
// what to do without knowing about HTTP.
var (
	// ErrGatewayUnavailable means the request was definitely not executed
	// (429, connection refused) or a read failed. Retrying later is safe.
	ErrGatewayUnavailable = errors.New("gateway unavailable")
	// ErrGatewayAmbiguous means a write may or may not have been executed
	// (timeout after sending, 5xx, connection reset). Only a retry with the
	// same idempotency key, or a lookup, can settle it.
	ErrGatewayAmbiguous = errors.New("gateway outcome unknown")
	// ErrGatewayInProgress means the gateway still processes a request with
	// this idempotency key (409, 425). It always comes together with
	// ErrGatewayAmbiguous. The key is proven to be known to the gateway, so
	// the request is repeated later with the same key, never abandoned.
	ErrGatewayInProgress = errors.New("gateway request with this key in progress")
	// ErrGatewayRejected means the gateway declined the payment for good: a
	// validation or business refusal (400, 402, 422). The payment fails.
	ErrGatewayRejected = errors.New("gateway declined the payment")
	// ErrGatewayMisconfigured means the gateway refused us rather than the
	// payment: bad credentials, a wrong URL, an unknown 4xx. It always comes
	// together with ErrGatewayUnavailable, so the work is retried and the
	// payment never fails because of our own configuration.
	ErrGatewayMisconfigured = errors.New("gateway refused our credentials or route")
	// ErrGatewayNotFound means the gateway does not know the reference.
	ErrGatewayNotFound = errors.New("gateway reference not found")
)
