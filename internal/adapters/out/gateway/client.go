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
// left, a reset connection or a 5xx may hide a payment that went through.
// The client reports ports.ErrGatewayAmbiguous and does not retry; the task
// layer decides what to do. Some ambiguous answers also carry
// ports.ErrGatewayResendSameKey: the only safe next step is the same request
// with the same key (409, 425, or a connection lost after the request was
// written).
//
// "Nothing was sent" is decided from what the transport actually did, not
// from the error type. net/http replays a request that carries an
// Idempotency-Key header on a new connection when a reused keep-alive
// connection fails after the request was written; if that new connection
// cannot be dialed, Do returns a dial error although the gateway already has
// the request. An httptrace.ClientTrace records whether any attempt wrote
// the request, and only a dial error with nothing written counts as "not
// executed".
//
// Only an explicit decline (400, 402, 422) fails a payment, except a 422
// whose error code says the key was reused with a different body: the body
// for a key is a function of the payment's immutable fields, so that answer
// means a bug or a key collision on our side, never the client's payment
// being refused. It is treated as a misconfiguration: retried and alerted. 401, 403, 404,
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
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
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
	opLookup = "lookup"
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
	for _, op := range []string{opCreate, opRead, opLookup} {
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
	err error
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
	var res ports.GatewayPayment
	err = c.call(ctx, opCreate, http.MethodPost, "/v1/payments", body, req.IdempotencyKey, func(r io.Reader) (err error) {
		res, err = decode(r)
		return err
	})
	return res, err
}

// Payment reads a payment by the gateway's reference.
func (c *Client) Payment(ctx context.Context, ref string) (ports.GatewayPayment, error) {
	var res ports.GatewayPayment
	err := c.call(ctx, opRead, http.MethodGet, "/v1/payments/"+url.PathEscape(ref), nil, "", func(r io.Reader) (err error) {
		res, err = decode(r)
		return err
	})
	return res, err
}

// FindByReference lists the gateway payments created for reference.
func (c *Client) FindByReference(ctx context.Context, reference string) ([]ports.GatewayPayment, error) {
	var found []ports.GatewayPayment
	path := "/v1/payments?reference=" + url.QueryEscape(reference)
	err := c.call(ctx, opLookup, http.MethodGet, path, nil, "", func(r io.Reader) (err error) {
		found, err = decodeList(r)
		return err
	})
	return found, err
}

// call runs one request, retrying in-line only what was not executed. decode
// reads a 2xx body.
func (c *Client) call(ctx context.Context, op, method, path string, body []byte, idempotencyKey string, decode func(io.Reader) error) error {
	for attempt := 0; ; attempt++ {
		start := time.Now()
		r := c.once(ctx, method, path, body, idempotencyKey, decode)
		c.metrics.duration.WithLabelValues(op).Observe(time.Since(start).Seconds())
		c.metrics.requests.WithLabelValues(op, outcome(r.err)).Inc()
		if r.err == nil {
			return nil
		}
		if !r.retry || attempt >= c.cfg.MaxRetries || !fits(ctx, r.wait) {
			return r.err
		}
		if werr := c.wait(ctx, r.wait); werr != nil {
			return fmt.Errorf("%w (stopped waiting: %w)", r.err, werr)
		}
	}
}

// fits reports whether waiting d still leaves time before ctx's deadline.
func fits(ctx context.Context, d time.Duration) bool {
	deadline, ok := ctx.Deadline()
	return !ok || time.Until(deadline) > d
}

// once performs one HTTP exchange and classifies the result.
func (c *Client) once(ctx context.Context, method, path string, body []byte, idempotencyKey string, decode func(io.Reader) error) result {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.CallTimeout)
	defer cancel()
	write := method != http.MethodGet

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	// JoinPath keeps a path prefix of the base URL; the query is set apart
	// because JoinPath would escape the "?".
	pathPart, query, _ := strings.Cut(path, "?")
	target := c.base.JoinPath(pathPart)
	target.RawQuery = query
	req, err := http.NewRequestWithContext(ctx, method, target.String(), reader)
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

	var wrote, reused atomic.Bool
	req = req.WithContext(httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if info.Reused {
				reused.Store(true)
			}
		},
		// Called after each write attempt, also a failed one: the bytes may
		// have reached the gateway, so any call counts as written.
		WroteRequest: func(httptrace.WroteRequestInfo) { wrote.Store(true) },
	}))
	resp, err := c.http.Do(req)
	if err != nil {
		return transportFailure(err, write, wrote.Load(), reused.Load())
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
		_ = resp.Body.Close()
	}()
	return c.classify(resp, write, path, decode)
}

// classify maps an HTTP answer to a result. Writes and reads differ: a 5xx
// on a write may hide an executed payment, on a read it is only a failure.
func (c *Client) classify(resp *http.Response, write bool, path string, decode func(io.Reader) error) result {
	code := resp.StatusCode
	var errBody []byte
	if code >= 400 {
		errBody, _ = io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	}
	switch {
	case code == http.StatusOK || code == http.StatusCreated:
		if err := decode(io.LimitReader(resp.Body, maxResponseBytes)); err != nil {
			return failed(unreadable(err, write))
		}
		return result{}
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
	case write && code == http.StatusUnprocessableEntity && errorCode(errBody) == codeKeyReused:
		// The key was used before with a different body: our bug or a key
		// collision, never a decline of this payment.
		return failed(fmt.Errorf("%w: %w: status %d: idempotency key reused with a different request",
			ports.ErrGatewayUnavailable, ports.ErrGatewayMisconfigured, code))
	case write && (code == http.StatusBadRequest || code == http.StatusPaymentRequired || code == http.StatusUnprocessableEntity):
		return failed(fmt.Errorf("%w: status %d: %s", ports.ErrGatewayRejected, code, snippet(errBody)))
	case write && code == http.StatusConflict:
		// A request with this idempotency key is still being processed.
		return failed(fmt.Errorf("%w: %w: status %d", ports.ErrGatewayAmbiguous, ports.ErrGatewayResendSameKey, code))
	case write && code == http.StatusTooEarly:
		// 425 Too Early (RFC 8470): the gateway would not process the request
		// yet, for example because it arrived in TLS early data, and expects
		// it again. Sending it again with the same key is the safe retry.
		return failed(fmt.Errorf("%w: %w: status %d", ports.ErrGatewayAmbiguous, ports.ErrGatewayResendSameKey, code))
	case code >= 400 && code < 500:
		return failed(fmt.Errorf("%w: %w: status %d: %s",
			ports.ErrGatewayUnavailable, ports.ErrGatewayMisconfigured, code, snippet(errBody)))
	case write:
		return failed(fmt.Errorf("%w: status %d", ports.ErrGatewayAmbiguous, code))
	default:
		return failed(fmt.Errorf("%w: status %d", ports.ErrGatewayUnavailable, code))
	}
}

// transportFailure classifies an error from http.Client.Do. wrote reports
// whether any attempt of the transport wrote the request, reused whether it
// ran on a reused connection.
func transportFailure(err error, write, wrote, reused bool) result {
	var opErr *net.OpError
	dial := errors.As(err, &opErr) && opErr.Op == "dial"
	switch {
	case dial && (!write || !wrote):
		// Nothing was written, or it was a read: safe to send again now.
		return result{err: fmt.Errorf("%w: %w", ports.ErrGatewayUnavailable, err), retry: true, wait: dialRetryDelay}
	case !write:
		return failed(fmt.Errorf("%w: %w", ports.ErrGatewayUnavailable, err))
	case dial:
		// The request was written on a connection that then failed, and the
		// transport's replay could not connect: the gateway may have it.
		conn := "a new connection"
		if reused {
			conn = "a reused connection"
		}
		return failed(fmt.Errorf("%w: %w: request written on %s, then the replay failed to connect: %w",
			ports.ErrGatewayAmbiguous, ports.ErrGatewayResendSameKey, conn, err))
	default:
		return failed(fmt.Errorf("%w: %w", ports.ErrGatewayAmbiguous, err))
	}
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
	if err := json.NewDecoder(r).Decode(&body); err != nil {
		return ports.GatewayPayment{}, err
	}
	return toPayment(body)
}

func decodeList(r io.Reader) ([]ports.GatewayPayment, error) {
	var body []paymentResponse
	if err := json.NewDecoder(r).Decode(&body); err != nil {
		return nil, err
	}
	out := make([]ports.GatewayPayment, 0, len(body))
	for _, b := range body {
		p, err := toPayment(b)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

func toPayment(body paymentResponse) (ports.GatewayPayment, error) {
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

// codeKeyReused is the gateway's error code for an idempotency key reused
// with a different request body.
const codeKeyReused = "idempotency_key_reused"

// errorCode reads the machine-readable code of a gateway error body, or "".
func errorCode(b []byte) string {
	var body struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(b, &body)
	return body.Code
}

func snippet(b []byte) string {
	if len(b) > 200 {
		b = b[:200]
	}
	return string(bytes.TrimSpace(b))
}
