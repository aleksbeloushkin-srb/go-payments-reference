// Package publisher delivers outbox events to the outside world: to the log
// and, optionally, to a subscriber's HTTP endpoint. Both types satisfy
// outbox.Publisher, the interface declared by the relay that uses them.
package publisher

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// Log writes each event to a structured log. It stands in for a message
// broker in this reference service.
type Log struct {
	log *slog.Logger
}

// NewLog returns a Log publisher.
func NewLog(log *slog.Logger) *Log {
	return &Log{log: log}
}

// Publish logs the event.
func (l *Log) Publish(ctx context.Context, r ports.OutboxRecord) error {
	l.log.InfoContext(ctx, "event published",
		"event_id", r.ID, "type", r.Type, "payment_id", r.PaymentID, "created_at", r.CreatedAt)
	return nil
}

// Webhook POSTs each event as JSON to a fixed URL. Event-ID carries the
// outbox id, so the receiver can drop the duplicates that at-least-once
// delivery produces. The caller's context bounds each delivery.
type Webhook struct {
	url    string
	client *http.Client
}

// NewWebhook returns a Webhook publisher.
func NewWebhook(url string, client *http.Client) *Webhook {
	return &Webhook{url: url, client: client}
}

// Publish delivers the event and treats any non-2xx answer as a failure.
func (w *Webhook) Publish(ctx context.Context, r ports.OutboxRecord) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(r.Payload))
	if err != nil {
		return fmt.Errorf("webhook: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Event-ID", strconv.FormatInt(r.ID, 10))
	req.Header.Set("Event-Type", r.Type)
	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("webhook: status %d", resp.StatusCode)
	}
	return nil
}
