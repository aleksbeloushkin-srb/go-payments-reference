package payment

import (
	"errors"
	"fmt"
)

// Sentinel errors. Callers match them with errors.Is; the HTTP adapter maps
// them to status codes in one place.
var (
	// ErrNotFound means the requested payment does not exist.
	ErrNotFound = errors.New("payment not found")
	// ErrInvalidTransition means the state machine rejected a status change.
	ErrInvalidTransition = errors.New("invalid status transition")
	// ErrIdempotencyConflict means an Idempotency-Key was reused with a
	// different request body.
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")
	// ErrVersionConflict means an optimistic version check failed: somebody
	// else changed the row between our read and our write.
	ErrVersionConflict = errors.New("version conflict")
)

// ValidationError reports a bad input field. It becomes HTTP 400.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid %s: %s", e.Field, e.Reason)
}
