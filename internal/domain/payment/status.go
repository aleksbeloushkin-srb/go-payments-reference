package payment

// Status is the lifecycle state of a payment.
type Status string

// Payment statuses.
//
//	pending ─► processing ◄─► requires_action
//	   │           │                 │
//	   └───────────┴─────────────────┴──► succeeded | failed | canceled
//
// pending: accepted by the bank API, not yet sent to the gateway.
// processing: the gateway accepted it and is working on it.
// requires_action: the gateway waits for the client (for example, a
// 3-D Secure challenge or a confirmation in the banking app).
// succeeded, failed and canceled are final: nothing leaves them.
const (
	StatusPending        Status = "pending"
	StatusProcessing     Status = "processing"
	StatusRequiresAction Status = "requires_action"
	StatusSucceeded      Status = "succeeded"
	StatusFailed         Status = "failed"
	StatusCanceled       Status = "canceled"
)

// transitions is the explicit state machine. A status missing from the map
// has no outgoing transitions.
var transitions = map[Status][]Status{
	StatusPending:        {StatusProcessing, StatusRequiresAction, StatusSucceeded, StatusFailed, StatusCanceled},
	StatusProcessing:     {StatusRequiresAction, StatusSucceeded, StatusFailed, StatusCanceled},
	StatusRequiresAction: {StatusProcessing, StatusSucceeded, StatusFailed, StatusCanceled},
}

// Valid reports whether s is one of the known statuses.
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusProcessing, StatusRequiresAction,
		StatusSucceeded, StatusFailed, StatusCanceled:
		return true
	}
	return false
}

// Final reports whether s is a terminal status.
func (s Status) Final() bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusCanceled
}

// CanTransitionTo reports whether the state machine allows s -> next. Staying
// in the same status is not a transition and returns false.
func (s Status) CanTransitionTo(next Status) bool {
	for _, n := range transitions[s] {
		if n == next {
			return true
		}
	}
	return false
}
