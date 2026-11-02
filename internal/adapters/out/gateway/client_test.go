package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

// recorder replaces the client's sleep and remembers requested waits.
type recorder struct {
	mu    sync.Mutex
	waits []time.Duration
}

func (r *recorder) wait(_ context.Context, d time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.waits = append(r.waits, d)
	return nil
}

func newClient(t *testing.T, url string, cfg Config) (*Client, *recorder) {
	t.Helper()
	cfg.BaseURL = url
	if cfg.CallTimeout == 0 {
		cfg.CallTimeout = 2 * time.Second
	}
	c, err := NewClient(cfg, &http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	c.wait = rec.wait
	return c, rec
}

func request() ports.GatewayRequest {
	id := uuid.MustParse("0195d1c0-0000-7000-8000-000000000001")
	return ports.GatewayRequest{
		IdempotencyKey: id.String(),
		PaymentID:      id,
		MerchantID:     "m-1",
		Amount:         payment.Money{AmountMinor: 1500, Currency: "EUR"},
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func TestCreatePaymentSendsIdempotencyKeyAndBody(t *testing.T) {
	var got struct {
		key  string
		body createRequest
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.key = r.Header.Get("Idempotency-Key")
		_ = json.NewDecoder(r.Body).Decode(&got.body)
		writeJSON(w, http.StatusCreated, paymentResponse{Ref: "gw_1", Status: "processing"})
	}))
	defer srv.Close()
	c, _ := newClient(t, srv.URL, Config{})

	res, err := c.CreatePayment(context.Background(), request())

	if err != nil {
		t.Fatal(err)
	}
	if res != (ports.GatewayPayment{Ref: "gw_1", Status: payment.StatusProcessing}) {
		t.Fatalf("res = %+v", res)
	}
	want := createRequest{Reference: request().PaymentID.String(), MerchantID: "m-1", AmountMinor: 1500, Currency: "EUR"}
	if got.key != request().IdempotencyKey || got.body != want {
		t.Fatalf("sent key %q body %+v", got.key, got.body)
	}
}

func TestRateLimitedRequestIsRetriedAfterRetryAfter(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		writeJSON(w, http.StatusCreated, paymentResponse{Ref: "gw_1", Status: "succeeded"})
	}))
	defer srv.Close()
	c, rec := newClient(t, srv.URL, Config{MaxRetries: 2, MaxRetryAfter: 5 * time.Second})

	res, err := c.CreatePayment(context.Background(), request())

	if err != nil || res.Status != payment.StatusSucceeded {
		t.Fatalf("res = %+v, err = %v", res, err)
	}
	if calls.Load() != 2 || len(rec.waits) != 1 || rec.waits[0] != 2*time.Second {
		t.Fatalf("calls = %d, waits = %v; want 2 calls and one 2s wait", calls.Load(), rec.waits)
	}
}

func TestRetryPolicy(t *testing.T) {
	tests := []struct {
		name      string
		handler   http.HandlerFunc
		read      bool
		cfg       Config
		wantErr   error
		wantCalls int32
	}{
		{
			name: "429 retries are bounded",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
			},
			cfg:     Config{MaxRetries: 2, MaxRetryAfter: 5 * time.Second},
			wantErr: ports.ErrGatewayUnavailable, wantCalls: 3,
		},
		{
			name: "429 with a long Retry-After is not waited for",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", "60")
				w.WriteHeader(http.StatusTooManyRequests)
			},
			cfg:     Config{MaxRetries: 2, MaxRetryAfter: 5 * time.Second},
			wantErr: ports.ErrGatewayUnavailable, wantCalls: 1,
		},
		{
			name:    "500 on a write is ambiguous and not retried",
			handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) },
			cfg:     Config{MaxRetries: 2},
			wantErr: ports.ErrGatewayAmbiguous, wantCalls: 1,
		},
		{
			name:    "500 on a read is plainly unavailable",
			handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) },
			read:    true,
			cfg:     Config{MaxRetries: 2},
			wantErr: ports.ErrGatewayUnavailable, wantCalls: 1,
		},
		{
			name:    "400 is a decline",
			handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadRequest) },
			cfg:     Config{MaxRetries: 2},
			wantErr: ports.ErrGatewayRejected, wantCalls: 1,
		},
		{
			name:    "402 is a decline",
			handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusPaymentRequired) },
			cfg:     Config{MaxRetries: 2},
			wantErr: ports.ErrGatewayRejected, wantCalls: 1,
		},
		{
			name:    "422 is a decline",
			handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnprocessableEntity) },
			cfg:     Config{MaxRetries: 2},
			wantErr: ports.ErrGatewayRejected, wantCalls: 1,
		},
		{
			name:    "409 means the same key is still in progress",
			handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusConflict) },
			cfg:     Config{MaxRetries: 2},
			wantErr: ports.ErrGatewayAmbiguous, wantCalls: 1,
		},
		{
			name:    "425 means the same key is still in progress",
			handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTooEarly) },
			cfg:     Config{MaxRetries: 2},
			wantErr: ports.ErrGatewayAmbiguous, wantCalls: 1,
		},
		{
			name:    "408 is unavailable",
			handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusRequestTimeout) },
			cfg:     Config{MaxRetries: 2},
			wantErr: ports.ErrGatewayUnavailable, wantCalls: 1,
		},
		{
			name:    "404 on a read is not found",
			handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) },
			read:    true,
			cfg:     Config{MaxRetries: 2},
			wantErr: ports.ErrGatewayNotFound, wantCalls: 1,
		},
		{
			name: "unknown status in a 2xx write is ambiguous",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(w, http.StatusCreated, paymentResponse{Ref: "gw_1", Status: "teleported"})
			},
			cfg:     Config{MaxRetries: 2},
			wantErr: ports.ErrGatewayAmbiguous, wantCalls: 1,
		},
		{
			name: "timeout after sending is ambiguous and not retried",
			handler: func(_ http.ResponseWriter, r *http.Request) {
				// Read the body first: only then can the server notice that
				// the client hung up and cancel r.Context().
				_, _ = io.Copy(io.Discard, r.Body)
				<-r.Context().Done() // executed, but the answer never comes
			},
			cfg:     Config{MaxRetries: 2, CallTimeout: 50 * time.Millisecond},
			wantErr: ports.ErrGatewayAmbiguous, wantCalls: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				tt.handler(w, r)
			}))
			defer srv.Close()
			c, _ := newClient(t, srv.URL, tt.cfg)

			var err error
			if tt.read {
				_, err = c.Payment(context.Background(), "gw_1")
			} else {
				_, err = c.CreatePayment(context.Background(), request())
			}

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if calls.Load() != tt.wantCalls {
				t.Fatalf("calls = %d, want %d", calls.Load(), tt.wantCalls)
			}
		})
	}
}

// Credentials, routes and unknown 4xx answers are our mistake, not the
// payment's: the error is "unavailable" (retry) and "misconfigured" (alert),
// never a decline.
func TestConfigurationErrorsNeverDeclineThePayment(t *testing.T) {
	for _, code := range []int{401, 403, 404, 405, 418} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }))
		c, _ := newClient(t, srv.URL, Config{MaxRetries: 2})

		_, err := c.CreatePayment(context.Background(), request())
		srv.Close()

		if !errors.Is(err, ports.ErrGatewayUnavailable) || !errors.Is(err, ports.ErrGatewayMisconfigured) || errors.Is(err, ports.ErrGatewayRejected) {
			t.Errorf("status %d: err = %v, want unavailable and misconfigured, not rejected", code, err)
		}
	}
}

// A Retry-After the client will not wait for in-line travels with the error
// so the task layer can respect it.
func TestLongRetryAfterIsPassedOn(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	c, _ := newClient(t, srv.URL, Config{MaxRetries: 2, MaxRetryAfter: 5 * time.Second})

	_, err := c.CreatePayment(context.Background(), request())

	var hint *ports.RetryAfterError
	if !errors.As(err, &hint) || hint.After != time.Minute {
		t.Fatalf("err = %v, want a RetryAfterError of 1m", err)
	}
}

func TestConnectionRefusedIsRetried(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nothing listens on url any more
	c, rec := newClient(t, url, Config{MaxRetries: 2})

	_, err := c.CreatePayment(context.Background(), request())

	if !errors.Is(err, ports.ErrGatewayUnavailable) {
		t.Fatalf("err = %v, want unavailable", err)
	}
	if len(rec.waits) != 2 {
		t.Fatalf("waits = %v, want 2 retries of a request that was never sent", rec.waits)
	}
}

func TestRetryAfterBeyondCallerDeadlineIsNotWaited(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	c, rec := newClient(t, srv.URL, Config{MaxRetries: 2, MaxRetryAfter: 5 * time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := c.CreatePayment(ctx, request())

	if !errors.Is(err, ports.ErrGatewayUnavailable) || calls.Load() != 1 || len(rec.waits) != 0 {
		t.Fatalf("err = %v, calls = %d, waits = %v", err, calls.Load(), rec.waits)
	}
}

func TestRetryAfterParsing(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	tests := map[string]time.Duration{
		"":                              defaultRetryAfter,
		"3":                             3 * time.Second,
		"-4":                            0,
		"soon":                          defaultRetryAfter,
		"Sun, 01 Mar 2026 12:00:07 GMT": 7 * time.Second,
		"Sun, 01 Mar 2026 11:00:00 GMT": 0,
	}
	for in, want := range tests {
		if got := retryAfter(in, now); got != want {
			t.Errorf("retryAfter(%q) = %s, want %s", in, got, want)
		}
	}
}
