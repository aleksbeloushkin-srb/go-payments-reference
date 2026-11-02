package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/aleksbeloushkin-srb/go-payments-reference/pkg/hmacsig"
)

// record is one payment known to the fake gateway.
type record struct {
	Ref         string `json:"ref"`
	Reference   string `json:"reference"`
	MerchantID  string `json:"merchant_id"`
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
	Status      string `json:"status"`
	fingerprint string
}

type createBody struct {
	Reference   string `json:"reference"`
	MerchantID  string `json:"merchant_id"`
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
}

// gateway simulates an external payment gateway. Every goroutine it starts
// is tracked by wg and stops when ctx is cancelled.
type gateway struct {
	cfg    config
	log    *slog.Logger
	client *http.Client
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	closed  bool
	byKey   map[string]*record
	byRef   map[string]*record
	faults  []string
	latency time.Duration
	webhook string
	stats   map[string]int
}

func newGateway(cfg config, log *slog.Logger, client *http.Client) *gateway {
	ctx, cancel := context.WithCancel(context.Background())
	g := &gateway{cfg: cfg, log: log, client: client, ctx: ctx, cancel: cancel}
	g.reset()
	return g
}

func (g *gateway) reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.byKey == nil {
		g.byKey, g.byRef = map[string]*record{}, map[string]*record{}
	}
	g.faults = append([]string(nil), g.cfg.Faults...)
	g.latency = g.cfg.Latency
	g.webhook = g.cfg.WebhookMode
	g.stats = map[string]int{}
}

// stop releases hanging requests and stops background work. It does not
// wait; close does.
func (g *gateway) stop() { g.cancel() }

// close stops the gateway and waits for its goroutines.
func (g *gateway) close() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
	g.cancel()
	g.wg.Wait()
}

// goBackground starts fn unless the gateway is closing. Add happens under
// the same lock that close takes, so it never races with wg.Wait.
func (g *gateway) goBackground(fn func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return
	}
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		fn()
	}()
}

func (g *gateway) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/payments", g.create)
	mux.HandleFunc("GET /v1/payments/{ref}", g.read)
	mux.HandleFunc("GET /v1/payments", g.findByReference)
	mux.HandleFunc("GET /_control", g.showControl)
	mux.HandleFunc("POST /_control", g.updateControl)
	mux.HandleFunc("DELETE /_control", g.resetControl)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

func (g *gateway) count(name string) {
	g.mu.Lock()
	g.stats[name]++
	g.mu.Unlock()
}

func (g *gateway) nextFault() (fault string, latency time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.faults) > 0 {
		fault, g.faults = g.faults[0], g.faults[1:]
	}
	return fault, g.latency
}

func (g *gateway) create(w http.ResponseWriter, r *http.Request) {
	g.count("create_calls")
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Idempotency-Key header is required"})
		return
	}
	var in createBody
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&in); err != nil || in.Reference == "" || in.AmountMinor <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}
	// Drain the body: only then does net/http notice a client that hangs up.
	_, _ = io.Copy(io.Discard, r.Body)

	fault, latency := g.nextFault()
	switch fault {
	case "429":
		g.count("fault_429")
		w.Header().Set("Retry-After", strconv.Itoa(int(g.cfg.RetryAfter/time.Second)))
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "rate limited"})
		return
	case "500":
		g.count("fault_500")
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	case "409":
		g.count("fault_409")
		writeJSON(w, http.StatusConflict, map[string]string{"error": "a request with this idempotency key is in progress"})
		return
	case "401":
		g.count("fault_401")
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}

	rec, created, err := g.store(key, in)
	if err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error(), "code": "idempotency_key_reused"})
		return
	}
	if created {
		g.count("executions")
		snapshot := *rec
		g.goBackground(func() { g.settle(snapshot) })
	}
	if fault == "hang" {
		// The payment exists now, but the caller never hears about it: the
		// ambiguous outcome a client has to survive.
		g.count("fault_hang")
		select {
		case <-r.Context().Done():
		case <-g.ctx.Done():
		}
		return
	}
	// A slow gateway: the payment is executed, the answer takes latency.
	// Webhooks settle in parallel, so with a settle delay shorter than the
	// latency a webhook arrives before the answer to the create call.
	if !wait(r.Context(), latency) {
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, g.view(rec.Ref))
}

// store deduplicates by Idempotency-Key, as real gateways do.
func (g *gateway) store(key string, in createBody) (*record, bool, error) {
	fp := fmt.Sprintf("%s|%s|%d|%s", in.Reference, in.MerchantID, in.AmountMinor, in.Currency)
	g.mu.Lock()
	defer g.mu.Unlock()
	if rec, ok := g.byKey[key]; ok {
		if rec.fingerprint != fp {
			return nil, false, errors.New("idempotency key reused with a different request")
		}
		return rec, false, nil
	}
	rec := &record{
		Ref:         "gw_" + randomHex(8),
		Reference:   in.Reference,
		MerchantID:  in.MerchantID,
		AmountMinor: in.AmountMinor,
		Currency:    in.Currency,
		Status:      initialStatus(in.AmountMinor),
		fingerprint: fp,
	}
	g.byKey[key], g.byRef[rec.Ref] = rec, rec
	return rec, true, nil
}

// Outcome rules by the last two digits of the amount in minor units.
func initialStatus(amountMinor int64) string {
	switch amountMinor % 100 {
	case 97, 98:
		return "requires_action"
	default:
		return "processing"
	}
}

func nextStatuses(amountMinor int64) []string {
	switch amountMinor % 100 {
	case 99:
		return []string{"failed"}
	case 98:
		return []string{"processing", "succeeded"}
	case 97:
		return []string{"canceled"}
	default:
		return []string{"succeeded"}
	}
}

// view returns a copy of the record under the lock.
func (g *gateway) view(ref string) record {
	g.mu.Lock()
	defer g.mu.Unlock()
	return *g.byRef[ref]
}

func (g *gateway) read(w http.ResponseWriter, r *http.Request) {
	g.count("read_calls")
	g.mu.Lock()
	rec, ok := g.byRef[r.PathValue("ref")]
	var out record
	if ok {
		out = *rec
	}
	g.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown reference"})
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// findByReference lists the payments created for one merchant reference.
// Real gateways offer a similar lookup; it is how a client settles an
// ambiguous outcome without creating the payment again.
func (g *gateway) findByReference(w http.ResponseWriter, r *http.Request) {
	reference := r.URL.Query().Get("reference")
	if reference == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "reference query parameter is required"})
		return
	}
	g.mu.Lock()
	out := []record{}
	for _, rec := range g.byRef {
		if rec.Reference == reference {
			out = append(out, *rec)
		}
	}
	g.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

// settle walks a payment through its remaining statuses and sends a webhook
// for each change.
func (g *gateway) settle(rec record) {
	for _, status := range nextStatuses(rec.AmountMinor) {
		if !wait(g.ctx, g.cfg.SettleDelay) {
			return
		}
		g.mu.Lock()
		g.byRef[rec.Ref].Status = status
		mode := g.webhook
		g.mu.Unlock()

		event := map[string]any{
			"id":      "evt_" + randomHex(12),
			"type":    "payment.updated",
			"created": time.Now().Unix(),
			"data":    map[string]string{"ref": rec.Ref, "reference": rec.Reference, "status": status},
		}
		switch mode {
		case "drop":
			g.count("webhooks_dropped")
		case "duplicate":
			g.deliver(event)
			g.deliver(event)
		default:
			g.deliver(event)
		}
	}
}

// deliver POSTs a signed event, retrying with backoff until the receiver
// answers 2xx or the attempts run out.
func (g *gateway) deliver(event map[string]any) {
	body, _ := json.Marshal(event)
	delay := 500 * time.Millisecond
	for attempt := 1; attempt <= 5; attempt++ {
		err := g.post(body)
		if err == nil {
			g.count("webhooks_sent")
			return
		}
		g.log.Warn("webhook delivery failed", "event_id", event["id"], "attempt", attempt, "err", err)
		if !wait(g.ctx, delay) {
			return
		}
		delay *= 2
	}
	g.count("webhooks_failed")
}

func (g *gateway) post(body []byte) error {
	ctx, cancel := context.WithTimeout(g.ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.cfg.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(hmacsig.Header, hmacsig.Sign([]byte(g.cfg.WebhookSecret), time.Now(), body))
	resp, err := g.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

type controlView struct {
	Faults      []string       `json:"faults"`
	WebhookMode string         `json:"webhook_mode"`
	Latency     string         `json:"latency"`
	Stats       map[string]int `json:"stats"`
}

func (g *gateway) control() controlView {
	g.mu.Lock()
	defer g.mu.Unlock()
	return controlView{
		Faults:      append([]string{}, g.faults...),
		WebhookMode: g.webhook,
		Latency:     g.latency.String(),
		Stats:       maps.Clone(g.stats),
	}
}

func (g *gateway) showControl(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, g.control())
}

// updateControl changes fault injection at run time:
//
//	POST /_control?faults=429,409,hang   append to the fault queue (429, 500, 409, 401, hang)
//	POST /_control?webhook=drop          normal | drop | duplicate
//	POST /_control?latency=300ms         delay of each create answer, after executing
func (g *gateway) updateControl(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	faults, err := parseFaults(q.Get("faults"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	mode := q.Get("webhook")
	if mode != "" && mode != "normal" && mode != "drop" && mode != "duplicate" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "webhook must be normal, drop or duplicate"})
		return
	}
	var latency time.Duration
	if v := q.Get("latency"); v != "" {
		if latency, err = time.ParseDuration(v); err != nil || latency < 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid latency"})
			return
		}
	}
	g.mu.Lock()
	g.faults = append(g.faults, faults...)
	if mode != "" {
		g.webhook = mode
	}
	if q.Has("latency") {
		g.latency = latency
	}
	g.mu.Unlock()
	writeJSON(w, http.StatusOK, g.control())
}

func (g *gateway) resetControl(w http.ResponseWriter, _ *http.Request) {
	g.reset()
	writeJSON(w, http.StatusOK, g.control())
}

// wait pauses for d unless ctx ends first; it reports whether the full
// pause elapsed.
func wait(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
