package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/app"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/platform"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
	"github.com/aleksbeloushkin-srb/go-payments-reference/pkg/hmacsig"
)

var (
	now      = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	secret   = []byte("0123456789abcdef")
	sample   = payment.Payment{ID: uuid.MustParse("0195d1c0-0000-7000-8000-000000000001"), MerchantID: "m-1", Amount: payment.Money{AmountMinor: 1500, Currency: "EUR"}, Status: payment.StatusProcessing, Version: 2}
	errBoom  = errors.New("database is on fire")
	staticFS = fstest.MapFS{"index.html": {Data: []byte("<h1>bank</h1>")}}
)

type fixedClock struct{}

func (fixedClock) Now() time.Time { return now }

// fakeService implements the three consumer interfaces with canned answers.
type fakeService struct {
	createRes app.CreatePaymentResult
	createErr error
	readErr   error
	waitRes   app.WaitResult
	waitErr   error
	acceptErr error

	gotCreate app.CreatePaymentRequest
	gotEvent  app.GatewayEvent
	gotWait   int64
}

func (f *fakeService) CreatePayment(_ context.Context, req app.CreatePaymentRequest) (app.CreatePaymentResult, error) {
	f.gotCreate = req
	return f.createRes, f.createErr
}

func (f *fakeService) Payment(context.Context, uuid.UUID) (payment.Payment, error) {
	return sample, f.readErr
}

func (f *fakeService) WaitPayment(_ context.Context, _ uuid.UUID, seen int64) (app.WaitResult, error) {
	f.gotWait = seen
	return f.waitRes, f.waitErr
}

func (f *fakeService) AcceptGatewayEvent(_ context.Context, e app.GatewayEvent) error {
	f.gotEvent = e
	return f.acceptErr
}

func newTestHandler(svc *fakeService, logs io.Writer) http.Handler {
	return NewHandler(Deps{
		Creator:       svc,
		Reader:        svc,
		Events:        svc,
		Ready:         func() bool { return true },
		WebhookSecret: secret,
		Static:        staticFS,
		Clock:         fixedClock{},
		Metrics:       NewMetrics(prometheus.NewRegistry()),
		Logger:        platform.NewLogger(logs, slog.LevelDebug),
	})
}

func do(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("body %q is not JSON: %v", rec.Body.String(), err)
	}
	return m
}

func logLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for line := range strings.Lines(buf.String()) {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

// The order request id -> logging -> recover means: the panic becomes a 500
// response, the access log records that 500, and both log lines carry the
// request id that the client sees in the response header.
func TestMiddlewareOrder(t *testing.T) {
	var logs bytes.Buffer
	log := platform.NewLogger(&logs, slog.LevelInfo)
	h := chain(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("nil pointer") }),
		requestID, logging(log), recoverer(log))

	rec := do(h, httptest.NewRequest(http.MethodGet, "/boom", nil))

	if rec.Code != http.StatusInternalServerError || decodeBody(t, rec)["code"] != "internal" {
		t.Fatalf("response = %d %s", rec.Code, rec.Body)
	}
	id := rec.Header().Get(requestIDHeader)
	if id == "" {
		t.Fatal("missing request id header")
	}
	lines := logLines(t, &logs)
	if len(lines) != 2 {
		t.Fatalf("log lines = %v", lines)
	}
	panicLine, accessLine := lines[0], lines[1]
	if panicLine["msg"] != "panic recovered" || panicLine["request_id"] != id {
		t.Fatalf("panic line = %v", panicLine)
	}
	if accessLine["msg"] != "http request" || accessLine["status"] != float64(500) || accessLine["request_id"] != id {
		t.Fatalf("access line = %v", accessLine)
	}
}

func TestRequestIDIsKeptOnlyWhenWellFormed(t *testing.T) {
	h := newTestHandler(&fakeService{}, io.Discard)
	for incoming, keep := range map[string]bool{"abc-123": true, "bad id with spaces": false, "": false} {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		req.Header.Set(requestIDHeader, incoming)
		got := do(h, req).Header().Get(requestIDHeader)
		if (got == incoming) != keep || got == "" {
			t.Errorf("incoming %q -> %q, keep %v", incoming, got, keep)
		}
	}
}

func TestErrorMapping(t *testing.T) {
	tests := []struct {
		err        error
		wantStatus int
		wantCode   string
	}{
		{&payment.ValidationError{Field: "currency", Reason: "bad"}, http.StatusBadRequest, "invalid_argument"},
		{fmt.Errorf("lookup: %w", payment.ErrNotFound), http.StatusNotFound, "not_found"},
		{payment.ErrIdempotencyConflict, http.StatusUnprocessableEntity, "idempotency_conflict"},
		{ports.ErrTooManyWaiters, http.StatusTooManyRequests, "too_many_waiters"},
		{ports.ErrUnavailable, http.StatusServiceUnavailable, "unavailable"},
		{errBoom, http.StatusInternalServerError, "internal"},
	}
	for _, tt := range tests {
		t.Run(tt.wantCode, func(t *testing.T) {
			h := newTestHandler(&fakeService{readErr: tt.err}, io.Discard)
			rec := do(h, httptest.NewRequest(http.MethodGet, "/v1/payments/"+sample.ID.String(), nil))
			body := decodeBody(t, rec)
			if rec.Code != tt.wantStatus || body["code"] != tt.wantCode {
				t.Fatalf("got %d %v", rec.Code, body)
			}
			if strings.Contains(rec.Body.String(), "fire") {
				t.Fatal("internal error details leaked to the client")
			}
		})
	}
}

func TestCreatePayment(t *testing.T) {
	created := app.CreatePaymentResult{Payment: sample, Created: true}
	tests := []struct {
		name       string
		body       string
		res        app.CreatePaymentResult
		err        error
		wantStatus int
	}{
		{"created", `{"merchant_id":"m-1","amount_minor":1500,"currency":"EUR"}`, created, nil, http.StatusCreated},
		{"replay", `{"merchant_id":"m-1","amount_minor":1500,"currency":"EUR"}`, app.CreatePaymentResult{Payment: sample}, nil, http.StatusOK},
		{"conflict", `{"merchant_id":"m-1","amount_minor":1,"currency":"EUR"}`, created, payment.ErrIdempotencyConflict, http.StatusUnprocessableEntity},
		{"unknown field", `{"merchant_id":"m-1","amount":1500}`, created, nil, http.StatusBadRequest},
		{"two objects", `{"merchant_id":"m-1"}{}`, created, nil, http.StatusBadRequest},
		{"not json", `amount=1500`, created, nil, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &fakeService{createRes: tt.res, createErr: tt.err}
			req := httptest.NewRequest(http.MethodPost, "/v1/payments", strings.NewReader(tt.body))
			req.Header.Set("Idempotency-Key", "key-1")

			rec := do(newTestHandler(svc, io.Discard), req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
			}
			if tt.wantStatus == http.StatusCreated {
				body := decodeBody(t, rec)
				if body["id"] != sample.ID.String() || body["version"] != float64(2) || rec.Header().Get("Location") == "" {
					t.Fatalf("body = %v", body)
				}
				if svc.gotCreate.IdempotencyKey != "key-1" || svc.gotCreate.AmountMinor != 1500 {
					t.Fatalf("request = %+v", svc.gotCreate)
				}
			}
		})
	}
}

func TestCreatePaymentBodyLimit(t *testing.T) {
	big := `{"merchant_id":"` + strings.Repeat("x", maxCreateBody) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/payments", strings.NewReader(big))
	rec := do(newTestHandler(&fakeService{}, io.Discard), req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestWaitPayment(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		res          app.WaitResult
		err          error
		wantStatus   int
		wantModified any
	}{
		{"modified", "?version=1", app.WaitResult{Payment: sample, Modified: true}, nil, http.StatusOK, true},
		{"soft timeout", "?version=2", app.WaitResult{Payment: sample}, nil, http.StatusOK, false},
		{"registry full", "?version=2", app.WaitResult{}, ports.ErrTooManyWaiters, http.StatusTooManyRequests, nil},
		{"missing version", "", app.WaitResult{}, nil, http.StatusBadRequest, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &fakeService{waitRes: tt.res, waitErr: tt.err}
			rec := do(newTestHandler(svc, io.Discard),
				httptest.NewRequest(http.MethodGet, "/v1/payments/"+sample.ID.String()+"/wait"+tt.query, nil))

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
			}
			body := decodeBody(t, rec)
			if tt.wantModified != nil {
				if body["modified"] != tt.wantModified || body["status"] != "processing" || body["version"] != float64(2) {
					t.Fatalf("body = %v", body)
				}
			}
			if tt.wantStatus == http.StatusTooManyRequests && rec.Header().Get("Retry-After") == "" {
				t.Fatal("429 must carry Retry-After")
			}
		})
	}
}

func signedEvent(t *testing.T, body string, signedAt time.Time) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/gateway/events", strings.NewReader(body))
	req.Header.Set(hmacsig.Header, hmacsig.Sign(secret, signedAt, []byte(body)))
	return req
}

func TestGatewayEvent(t *testing.T) {
	body := fmt.Sprintf(`{"id":"evt_1","type":"payment.updated","created":1,"data":{"ref":"gw_1","reference":%q,"status":"succeeded"}}`, sample.ID)

	t.Run("valid signature is accepted", func(t *testing.T) {
		svc := &fakeService{}
		rec := do(newTestHandler(svc, io.Discard), signedEvent(t, body, now))
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
		}
		want := app.GatewayEvent{ID: "evt_1", PaymentID: sample.ID, GatewayRef: "gw_1", Status: payment.StatusSucceeded}
		if svc.gotEvent != want {
			t.Fatalf("event = %+v", svc.gotEvent)
		}
	})
	t.Run("tampered body is rejected", func(t *testing.T) {
		req := signedEvent(t, body, now)
		req.Body = io.NopCloser(strings.NewReader(strings.Replace(body, "succeeded", "failed", 1)))
		if rec := do(newTestHandler(&fakeService{}, io.Discard), req); rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d", rec.Code)
		}
	})
	t.Run("old signature is rejected", func(t *testing.T) {
		rec := do(newTestHandler(&fakeService{}, io.Discard), signedEvent(t, body, now.Add(-time.Hour)))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d", rec.Code)
		}
	})
	t.Run("unknown payment is 404 so the gateway stops retrying a bad reference", func(t *testing.T) {
		rec := do(newTestHandler(&fakeService{acceptErr: payment.ErrNotFound}, io.Discard), signedEvent(t, body, now))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d", rec.Code)
		}
	})
}

func TestStaticAndHealth(t *testing.T) {
	h := newTestHandler(&fakeService{}, io.Discard)
	if rec := do(h, httptest.NewRequest(http.MethodGet, "/", nil)); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "bank") {
		t.Fatalf("GET / = %d %s", rec.Code, rec.Body)
	}
	if rec := do(h, httptest.NewRequest(http.MethodGet, "/readyz", nil)); rec.Code != http.StatusOK {
		t.Fatalf("readyz = %d", rec.Code)
	}
	if rec := do(h, httptest.NewRequest(http.MethodGet, "/debug/vars", nil)); rec.Code != http.StatusNotFound {
		t.Fatalf("debug/vars = %d, want 404: metrics live on the admin listener", rec.Code)
	}
}

func TestReadyzReportsNotReady(t *testing.T) {
	h := NewHandler(Deps{
		Creator: &fakeService{}, Reader: &fakeService{}, Events: &fakeService{},
		Ready: func() bool { return false }, WebhookSecret: secret, Static: staticFS,
		Clock: fixedClock{}, Metrics: NewMetrics(prometheus.NewRegistry()), Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if rec := do(h, httptest.NewRequest(http.MethodGet, "/readyz", nil)); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz = %d, want 503", rec.Code)
	}
}

// A gateway may add fields to its events; they are ignored, not rejected.
func TestGatewayEventToleratesUnknownFields(t *testing.T) {
	body := fmt.Sprintf(`{"id":"evt_2","type":"payment.updated","created":1,"livemode":true,"data":{"ref":"gw_1","reference":%q,"status":"succeeded","fee_minor":3}}`, sample.ID)
	svc := &fakeService{}
	if rec := do(newTestHandler(svc, io.Discard), signedEvent(t, body, now)); rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	if svc.gotEvent.ID != "evt_2" {
		t.Fatalf("event = %+v", svc.gotEvent)
	}
}

// A long poll abandoned by its client is logged as 499, not 200.
func TestClientGoneIsLoggedAs499(t *testing.T) {
	var logs bytes.Buffer
	h := newTestHandler(&fakeService{waitErr: context.Canceled}, &logs)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/v1/payments/"+sample.ID.String()+"/wait?version=1", nil).WithContext(ctx)

	do(h, req)

	var access map[string]any
	for _, line := range logLines(t, &logs) {
		if line["msg"] == "http request" {
			access = line
		}
	}
	if access["status"] != float64(statusClientClosedRequest) {
		t.Fatalf("access log = %v, want status 499", access)
	}
}

// Request metrics use the route pattern, never the raw path, and keep long
// polls out of the latency histogram.
func TestRequestMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	svc := &fakeService{waitRes: app.WaitResult{Payment: sample}}
	h := NewHandler(Deps{
		Creator: svc, Reader: svc, Events: svc, Ready: func() bool { return true },
		WebhookSecret: secret, Static: staticFS, Clock: fixedClock{}, Metrics: m,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	do(h, httptest.NewRequest(http.MethodGet, "/v1/payments/"+sample.ID.String(), nil))
	do(h, httptest.NewRequest(http.MethodGet, "/v1/payments/"+uuid.NewString(), nil))
	do(h, httptest.NewRequest(http.MethodGet, "/v1/payments/"+sample.ID.String()+"/wait?version=1", nil))
	do(h, httptest.NewRequest("BREW", "/v1/payments", nil))

	if got := testutil.ToFloat64(m.requests.WithLabelValues("/v1/payments/{id}", "GET", "200")); got != 2 {
		t.Fatalf("requests for the payment route = %v, want 2 under one series", got)
	}
	if got := testutil.ToFloat64(m.requests.WithLabelValues("/v1/payments/{id}/wait", "GET", "200")); got != 1 {
		t.Fatalf("long polls counted = %v, want 1", got)
	}
	// The path exists for POST only, so ServeMux answers 405 without a pattern.
	if got := testutil.ToFloat64(m.requests.WithLabelValues("unmatched", "OTHER", "405")); got != 1 {
		t.Fatalf("unknown method = %v, want it folded into OTHER / unmatched", got)
	}
	// The latency histogram has the payment route only; the long poll went
	// to its own histogram.
	if n := testutil.CollectAndCount(m.duration); n != 2 {
		t.Fatalf("latency series = %d, want 2 (payment route and unmatched)", n)
	}
	for _, mf := range mustGather(t, reg) {
		if mf.GetName() != "http_request_duration_seconds" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, l := range metric.GetLabel() {
				if l.GetName() == "route" && strings.HasSuffix(l.GetValue(), "/wait") {
					t.Fatal("a long poll leaked into http_request_duration_seconds")
				}
			}
		}
	}
	var lp dto.Metric
	if err := m.longPoll.Write(&lp); err != nil || lp.GetHistogram().GetSampleCount() != 1 {
		t.Fatalf("long-poll histogram samples = %d (%v), want 1", lp.GetHistogram().GetSampleCount(), err)
	}
}

// A recovered panic is counted as a 500.
func TestRequestMetricsCountRecoveredPanics(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /boom", func(http.ResponseWriter, *http.Request) { panic("bug") })
	h := chain(mux, requestID, logging(log), instrument(m), recoverer(log))

	do(h, httptest.NewRequest(http.MethodGet, "/boom", nil))

	if got := testutil.ToFloat64(m.requests.WithLabelValues("/boom", "GET", "500")); got != 1 {
		t.Fatalf("panicking request counted as %v 500s, want 1", got)
	}
}

func mustGather(t *testing.T, g prometheus.Gatherer) []*dto.MetricFamily {
	t.Helper()
	mfs, err := g.Gather()
	if err != nil {
		t.Fatal(err)
	}
	return mfs
}
