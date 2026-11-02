package publisher

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

type delivery struct {
	eventID, eventType, body string
}

func TestWebhookSendsEventWithID(t *testing.T) {
	got := make(chan delivery, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Copy what the test needs inside the handler; the request is not
		// valid after the handler returns.
		body, _ := io.ReadAll(r.Body)
		got <- delivery{r.Header.Get("Event-ID"), r.Header.Get("Event-Type"), string(body)}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	rec := ports.OutboxRecord{ID: 42, PaymentID: uuid.New(), Type: "payment.created", Payload: []byte(`{"a":1}`)}

	if err := NewWebhook(srv.URL, srv.Client()).Publish(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if d := <-got; d != (delivery{"42", "payment.created", `{"a":1}`}) {
		t.Fatalf("delivery = %+v", d)
	}
}

func TestWebhookNon2xxIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	if err := NewWebhook(srv.URL, srv.Client()).Publish(context.Background(), ports.OutboxRecord{ID: 1}); err == nil {
		t.Fatal("want an error for 503")
	}
}
