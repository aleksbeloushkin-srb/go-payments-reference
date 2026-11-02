package app

import (
	"context"
	"fmt"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// HandleTask dispatches a claimed task to its handler. The queue worker calls
// it. An unknown kind is a permanent error: retrying will not teach this
// binary a new task type.
func (s *Service) HandleTask(ctx context.Context, t ports.Task) error {
	switch t.Kind {
	case ports.TaskStartPayment:
		return s.StartPayment(ctx, t.PaymentID)
	case ports.TaskPollPayment:
		return s.PollPayment(ctx, t.PaymentID)
	case ports.TaskApplyGatewayEvent:
		return s.ApplyGatewayEvent(ctx, t.Payload)
	case ports.TaskReconcilePayment:
		return s.ReconcilePayment(ctx, t.PaymentID, t.Payload)
	case ports.TaskCheckDuplicates:
		return s.CheckDuplicates(ctx, t.PaymentID, t.Payload)
	default:
		return ports.Permanent(fmt.Errorf("unknown task kind %q", t.Kind))
	}
}
