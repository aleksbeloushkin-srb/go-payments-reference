package app

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// Every declared task kind must reach a handler. A kind added to
// ports.TaskKinds but forgotten in HandleTask would otherwise go dead in
// production with "unknown task kind".
func TestHandleTaskKnowsEveryKind(t *testing.T) {
	f := newFixture(testConfig)
	for _, kind := range ports.TaskKinds {
		err := f.svc.HandleTask(context.Background(), ports.Task{Kind: kind, PaymentID: uuid.New(), Payload: []byte("{}")})
		if err != nil && strings.Contains(err.Error(), "unknown task kind") {
			t.Fatalf("kind %q has no handler", kind)
		}
	}
}

func TestHandleTaskUnknownKindIsPermanent(t *testing.T) {
	f := newFixture(testConfig)
	var perm *ports.PermanentError
	if err := f.svc.HandleTask(context.Background(), ports.Task{Kind: "refund"}); !errors.As(err, &perm) {
		t.Fatalf("err = %v, want permanent", err)
	}
}
