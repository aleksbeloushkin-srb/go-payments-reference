package httpapi

import (
	"context"
	"io/fs"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/app"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// PaymentCreator creates payments.
type PaymentCreator interface {
	CreatePayment(ctx context.Context, req app.CreatePaymentRequest) (app.CreatePaymentResult, error)
}

// PaymentReader reads payments and waits for their changes.
type PaymentReader interface {
	Payment(ctx context.Context, id uuid.UUID) (payment.Payment, error)
	WaitPayment(ctx context.Context, id uuid.UUID, seenVersion int64) (app.WaitResult, error)
}

// EventAccepter takes gateway webhooks.
type EventAccepter interface {
	AcceptGatewayEvent(ctx context.Context, e app.GatewayEvent) error
}

// Deps lists what the handlers need. Every field is required.
type Deps struct {
	Creator PaymentCreator
	Reader  PaymentReader
	Events  EventAccepter
	// WebhookSecret verifies gateway webhook signatures.
	WebhookSecret []byte
	// Static is the web UI file system.
	Static  fs.FS
	Clock   ports.Clock
	Metrics *Metrics
	Logger  *slog.Logger
}

// waitPattern is the long-poll route. Its duration is measured separately.
const waitPattern = "GET /v1/payments/{id}/wait"

type handler struct {
	creator PaymentCreator
	reader  PaymentReader
	events  EventAccepter
	secret  []byte
	clock   ports.Clock
	log     *slog.Logger
}

// NewHandler returns the root handler with every route and middleware.
func NewHandler(d Deps) http.Handler {
	h := &handler{
		creator: d.Creator,
		reader:  d.Reader,
		events:  d.Events,
		secret:  d.WebhookSecret,
		clock:   d.Clock,
		log:     d.Logger,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/payments", h.createPayment)
	mux.HandleFunc("GET /v1/payments/{id}", h.payment)
	mux.HandleFunc(waitPattern, h.waitPayment)
	mux.HandleFunc("POST /v1/gateway/events", h.gatewayEvent)
	mux.Handle("GET /", http.FileServerFS(d.Static))

	return chain(mux, requestID, logging(d.Logger), instrument(d.Metrics), recoverer(d.Logger))
}
