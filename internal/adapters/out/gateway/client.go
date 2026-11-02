// Package gateway is the HTTP client of the external payment gateway.
//
// The retry policy is the interesting part. A payment request is not
// idempotent by itself, so the client retries only when it knows the gateway
// did not execute the request:
//
//   - 429 Too Many Requests: the gateway refused before doing anything. Wait
//     for Retry-After (if it fits the caller's deadline) and try again.
//   - a dial error (connection refused, DNS failure): nothing was sent.
//
// Every other failure of a write is ambiguous: a timeout after the request
// left, a reset connection, a 5xx, or a 409/425 saying that a request with
// this key is still in progress may hide a payment that went through. The
// client reports ports.ErrGatewayAmbiguous and does not retry; the task
// layer retries later with the same Idempotency-Key, which the gateway uses
// to return the original result instead of charging twice.
//
// Only an explicit decline (400, 402, 422) fails a payment. 401, 403, 404,
// 405 and unknown 4xx answers mean our credentials or URL are wrong: they
// are reported as ports.ErrGatewayMisconfigured, counted in
// gateway_requests_total with outcome "misconfigured" and retried, because a
// configuration mistake must not turn into failed payments.
//
// One more retry layer hides in net/http: for a request that carries an
// Idempotency-Key header and a re-readable body, http.Transport may resend
// it on a fresh connection if a reused keep-alive connection dies before any
// response byte arrives. That is safe here for the same reason, but it is a
// reminder that "the client retried" can happen below your own code.
package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/domain/payment"
	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/ports"
)

const (
	// dialRetryDelay is the pause before retrying a request that failed to
	// connect.
	dialRetryDelay = 200 * time.Millisecond
	// defaultRetryAfter applies to a 429 without a usable Retry-After.
	defaultRetryAfter = time.Second
	// maxResponseBytes caps what we read from the gateway.
	maxResponseBytes = 1 << 20
)

// Operations, the op label of the gateway metrics.
const (
	opCreate = "create"
	opRead   = "read"
)

// Metrics are the gateway client's Prometheus metrics: one observation per
// HTTP exchange, so in-line retries are visible.
type Metrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// NewMetrics creates the gateway metrics and registers them on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_requests_total",
			Help: "Gateway HTTP exchanges by operation and classified outcome: ok, declined, rate_limited, ambiguous, misconfigured, error.",
		}, []string{"op", "outcome"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gateway_request_duration_seconds",
			Help:    "Duration of one gateway HTTP exchange.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8},
		}, []string{"op"}),
	}
	reg.MustRegister(m.requests, m.duration)
	// Create every series at zero. increase() and rate() ignore the first
	// sample of a new series, so a counter that first appears at 1 would
	// never trigger the alert that watches it.
	for _, op := range []string{opCreate, opRead} {
		for _, o := range outcomes {
			m.requests.WithLabelValues(op, o)
		}
	}
	return m
}

// outcomes lists every value of the outcome label.
var outcomes = []string{"ok", "declined", "rate_limited", "ambiguous", "misconfigured", "error"}

// outcome names the class of a result for the metrics.
func outcome(err error) string {
	var hint *ports.RetryAfterError
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ports.ErrGatewayRejected):
		return "declined"
	case errors.As(err, &hint):
		return "rate_limited"
	case errors.Is(err, ports.ErrGatewayMisconfigured):
		return "misconfigured"
	case errors.Is(err, ports.ErrGatewayAmbiguous):
		return "ambiguous"
	default:
		return "error"
	}
}

// result is the classified outcome of one HTTP exchange.
type result struct {
	payment ports.GatewayPayment
	err     error
	// retry reports that the request was definitely not executed, so the
	// client may send it again after wait.
	retry bool
	wait  time.Duration
}

func failed(err error) result { return result{err: err} }

// Config tunes the client.
type Config struct {
	BaseURL string
	// CallTimeout bounds one HTTP exchange. The effective deadline is the
	// earlier of this and the caller's own deadline.
	CallTimeout time.Duration
	// MaxRetries is how many extra attempts the client makes for requests
	// that were definitely not executed.
	MaxRetries int
	// MaxRetryAfter is the longest Retry-After the client is willing to wait
	// for in-line; a longer one is returned to the caller as unavailable.
	MaxRetryAfter time.Duration
}

// Client talks to the gateway's REST API.
type Client struct {
	base    *url.URL
	http    *http.Client
	metrics *Metrics
	cfg     Config
	// wait pauses between attempts. Tests replace it to avoid real sleeps.
	wait func(ctx context.Context, d time.Duration) error
}

// NewClient returns a Client for cfg.BaseURL using hc.
func NewClient(cfg Config, hc *http.Client, m *Metrics) (*Client, error) {
	base, err := url.Parse(cfg.BaseURL)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("gateway: invalid base URL %q", cfg.BaseURL)
	}
	return &Client{base: base, http: hc, metrics: m, cfg: cfg, wait: sleepCtx}, nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

type createRequest struct {
	Reference   string `json:"reference"`
	MerchantID  string `json:"merchant_id"`
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
}

type paymentResponse struct {
	Ref    string `json:"ref"`
	Status string `json:"status"`
}

// CreatePayment asks the gateway to execute a payment.
func (c *Client) CreatePayment(ctx context.Context, req ports.GatewayRequest) (ports.GatewayPayment, error) {
	body, err := json.Marshal(createRequest{
		Reference:   req.PaymentID.String(),
		MerchantID:  req.MerchantID,
		AmountMinor: req.Amount.AmountMinor,
		Currency:    string(req.Amount.Currency),
	})
	if err != nil {
		return ports.GatewayPayment{}, fmt.Errorf("gateway: encode request: %w", err)
	}
	return c.call(ctx, opCreate, http.MethodPost, "/v1/payments", body, req.IdempotencyKey)
}

// Payment reads a payment by the gateway's reference.
func (c *Client) Payment(ctx context.Context, ref string) (ports.GatewayPayment, error) {
	return c.call(ctx, opRead, http.MethodGet, "/v1/payments/"+url.PathEscape(ref), nil, "")
}

func (c *Client) call(ctx context.Context, op, method, path string, body []byte, idempotencyKey string) (ports.GatewayPayment, error) {
	for attempt := 0; ; attempt++ {
		start := time.Now()
		r := c.once(ctx, method, path, body, idempotencyKey)
		c.metrics.duration.WithLabelValues(op).Observe(time.Since(start).Seconds())
		c.metrics.requests.WithLabelValues(op, outcome(r.err)).Inc()
		if r.err == nil {
			return r.payment, nil
		}
		if !r.retry || attempt >= c.cfg.MaxRetries || !fits(ctx, r.wait) {
			return ports.GatewayPayment{}, r.err
		}
		if werr := c.wait(ctx, r.wait); werr != nil {
			return ports.GatewayPayment{}, fmt.Errorf("%w (stopped waiting: %w)", r.err, werr)
		}
	}
}

// fits reports whether waiting d still leaves time before ctx's deadline.
func fits(ctx context.Context, d time.Duration) bool {
	deadline, ok := ctx.Deadline()
	return !ok || time.Until(deadline) > d
}

// once performs one HTTP exchange and classifies the result.
func (c *Client) once(ctx context.Context, method, path string, body []byte, idempotencyKey string) result {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.CallTimeout)
	defer cancel()
	write := method != http.MethodGet

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base.JoinPath(path).String(), reader)
	if err != nil {
		return failed(fmt.Errorf("gateway: build request: %w", err))
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return transportFailure(err, write)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
		_ = resp.Body.Close()
	}()
	return c.classify(resp, write, path)
}

// classify maps an HTTP answer to a result. Writes and reads differ: a 5xx
// on a write may hide an executed payment, on a read it is only a failure.
func (c *Client) classify(resp *http.Response, write bool, path string) result {
	switch code := resp.StatusCode; {
	case code == http.StatusOK || code == http.StatusCreated:
		res, err := decode(resp.Body)
		if err != nil {
			return failed(unreadable(err, write))
		}
		return result{payment: res}
	case code == http.StatusTooManyRequests:
		wait := retryAfter(resp.Header.Get("Retry-After"), time.Now())
		err := &ports.RetryAfterError{
			After: wait,
			Err:   fmt.Errorf("%w: rate limited, retry after %s", ports.ErrGatewayUnavailable, wait),
		}
		// The task layer honours the hint if the client gives up in-line.
		return result{err: err, retry: wait <= c.cfg.MaxRetryAfter, wait: wait}
	case code == http.StatusNotFound && !write:
		return failed(fmt.Errorf("%w: %s", ports.ErrGatewayNotFound, path))
	case code == http.StatusRequestTimeout:
		return failed(fmt.Errorf("%w: status %d", ports.ErrGatewayUnavailable, code))
	case write && (code == http.StatusBadRequest || code == http.StatusPaymentRequired || code == http.StatusUnprocessableEntity):
		return failed(fmt.Errorf("%w: status %d: %s", ports.ErrGatewayRejected, code, snippet(resp.Body)))
	case write && (code == http.StatusConflict || code == http.StatusTooEarly):
		// A request with this idempotency key is still being processed.
		return failed(fmt.Errorf("%w: status %d, request with this key still in progress", ports.ErrGatewayAmbiguous, code))
	case code >= 400 && code < 500:
		return failed(fmt.Errorf("%w: %w: status %d: %s",
			ports.ErrGatewayUnavailable, ports.ErrGatewayMisconfigured, code, snippet(resp.Body)))
	case write:
		return failed(fmt.Errorf("%w: status %d", ports.ErrGatewayAmbiguous, code))
	default:
		return failed(fmt.Errorf("%w: status %d", ports.ErrGatewayUnavailable, code))
	}
}

// transportFailure classifies an error from http.Client.Do.
func transportFailure(err error, write bool) result {
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		// The connection was never established, so nothing was sent.
		return result{err: fmt.Errorf("%w: %w", ports.ErrGatewayUnavailable, err), retry: true, wait: dialRetryDelay}
	}
	if write {
		return failed(fmt.Errorf("%w: %w", ports.ErrGatewayAmbiguous, err))
	}
	return failed(fmt.Errorf("%w: %w", ports.ErrGatewayUnavailable, err))
}

// unreadable classifies a 2xx response we could not understand. For a write
// the gateway did act, we just do not know the result.
func unreadable(err error, write bool) error {
	if write {
		return fmt.Errorf("%w: unreadable response: %w", ports.ErrGatewayAmbiguous, err)
	}
	return fmt.Errorf("%w: unreadable response: %w", ports.ErrGatewayUnavailable, err)
}

func decode(r io.Reader) (ports.GatewayPayment, error) {
	var body paymentResponse
	if err := json.NewDecoder(io.LimitReader(r, maxResponseBytes)).Decode(&body); err != nil {
		return ports.GatewayPayment{}, err
	}
	// The gateway's status vocabulary matches the domain one, so the mapping
	// is a validation. A real gateway usually needs a translation table here.
	status := payment.Status(body.Status)
	if !status.Valid() || status == payment.StatusPending || body.Ref == "" {
		return ports.GatewayPayment{}, fmt.Errorf("unexpected payment %q in status %q", body.Ref, body.Status)
	}
	return ports.GatewayPayment{Ref: body.Ref, Status: status}, nil
}

// retryAfter parses a Retry-After header given in seconds or as an HTTP date.
func retryAfter(v string, now time.Time) time.Duration {
	if v == "" {
		return defaultRetryAfter
	}
	if secs, err := strconv.Atoi(v); err == nil {
		return time.Duration(max(secs, 0)) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		return max(t.Sub(now), 0)
	}
	return defaultRetryAfter
}

func snippet(r io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(r, 200))
	return string(bytes.TrimSpace(b))
}
