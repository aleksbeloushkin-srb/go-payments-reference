package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aleksbeloushkin-srb/go-payments-reference/pkg/hmacsig"
)

const testSecret = "0123456789abcdef"

type delivery struct {
	ID   string `json:"id"`
	Data struct {
		Ref    string `json:"ref"`
		Status string `json:"status"`
	} `json:"data"`
}

// receiver is the payments service stand-in: it verifies signatures and
// hands every delivery to the test through a channel.
func receiver(t *testing.T) (*httptest.Server, chan delivery) {
	t.Helper()
	got := make(chan delivery, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := hmacsig.Verify([]byte(testSecret), r.Header.Get(hmacsig.Header), body, time.Now(), time.Minute); err != nil {
			t.Errorf("bad signature: %v", err)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var d delivery
		_ = json.Unmarshal(body, &d)
		got <- d
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(srv.Close)
	return srv, got
}

// quiesce stops new background work and waits for the running settle
// goroutines to finish, without cancelling them. close cancels first, so a
// settle that has not reached its first step yet would exit and count
// nothing; tests that assert on settle results must wait with quiesce.
func (g *gateway) quiesce() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
	g.wg.Wait()
}

func newTestGateway(t *testing.T, webhookURL string, faults ...string) (*gateway, *httptest.Server) {
	t.Helper()
	cfg := config{WebhookURL: webhookURL, WebhookSecret: testSecret, WebhookMode: "normal", RetryAfter: time.Second, Faults: faults}
	gw := newGateway(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), &http.Client{})
	srv := httptest.NewServer(gw.handler())
	t.Cleanup(func() {
		gw.stop()
		srv.Close()
		gw.close()
	})
	return gw, srv
}

func create(t *testing.T, ctx context.Context, url, key string, amount int64) (*http.Response, record, error) {
	t.Helper()
	body := `{"reference":"p-1","merchant_id":"m-1","amount_minor":` + itoa(amount) + `,"currency":"EUR"}`
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url+"/v1/payments", strings.NewReader(body))
	req.Header.Set("Idempotency-Key", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, record{}, err
	}
	defer resp.Body.Close()
	var rec record
	_ = json.NewDecoder(resp.Body).Decode(&rec)
	return resp, rec, nil
}

func itoa(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}

func TestCreateIsIdempotent(t *testing.T) {
	_, srv := newTestGateway(t, "http://127.0.0.1:1/unused")

	first, rec1, err := create(t, context.Background(), srv.URL, "k1", 1250)
	if err != nil || first.StatusCode != http.StatusCreated {
		t.Fatalf("first = %v, %v", first, err)
	}
	second, rec2, _ := create(t, context.Background(), srv.URL, "k1", 1250)
	if second.StatusCode != http.StatusOK || rec2.Ref != rec1.Ref {
		t.Fatalf("replay = %d %s, want 200 %s", second.StatusCode, rec2.Ref, rec1.Ref)
	}
	conflict, _, _ := create(t, context.Background(), srv.URL, "k1", 9999)
	if conflict.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("different body with the same key = %d, want 422", conflict.StatusCode)
	}
	// The error code tells a reused key apart from a declined payment.
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/payments",
		strings.NewReader(`{"reference":"p-1","merchant_id":"m-1","amount_minor":1,"currency":"EUR"}`))
	req.Header.Set("Idempotency-Key", "k1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct{ Code string }
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Code != "idempotency_key_reused" {
		t.Fatalf("error code = %q", body.Code)
	}
}

func TestInjectedFaultsAreConsumedInOrder(t *testing.T) {
	gw, srv := newTestGateway(t, "http://127.0.0.1:1/unused", "429", "500", "409", "401")

	r1, _, _ := create(t, context.Background(), srv.URL, "k1", 1250)
	if r1.StatusCode != http.StatusTooManyRequests || r1.Header.Get("Retry-After") != "1" {
		t.Fatalf("first = %d, Retry-After %q", r1.StatusCode, r1.Header.Get("Retry-After"))
	}
	r2, _, _ := create(t, context.Background(), srv.URL, "k1", 1250)
	if r2.StatusCode != http.StatusInternalServerError {
		t.Fatalf("second = %d, want 500", r2.StatusCode)
	}
	for _, want := range []int{http.StatusConflict, http.StatusUnauthorized} {
		r, _, _ := create(t, context.Background(), srv.URL, "k1", 1250)
		if r.StatusCode != want {
			t.Fatalf("got %d, want %d", r.StatusCode, want)
		}
	}
	r3, _, _ := create(t, context.Background(), srv.URL, "k1", 1250)
	if r3.StatusCode != http.StatusCreated {
		t.Fatalf("last = %d, want 201: faults before it executed nothing", r3.StatusCode)
	}
	if st := gw.control().Stats; st["fault_429"] != 1 || st["fault_500"] != 1 || st["fault_409"] != 1 || st["fault_401"] != 1 {
		t.Fatalf("stats = %v", st)
	}
}

// "hang" executes the payment and then never answers: the client times out
// without knowing the outcome, and a retry with the same key finds it.
func TestHangExecutesThenHangs(t *testing.T) {
	_, srv := newTestGateway(t, "http://127.0.0.1:1/unused", "hang")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if _, _, err := create(t, ctx, srv.URL, "k1", 1250); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want a client timeout", err)
	}
	retry, rec, err := create(t, context.Background(), srv.URL, "k1", 1250)
	if err != nil || retry.StatusCode != http.StatusOK || rec.Ref == "" {
		t.Fatalf("retry = %v %+v %v, want the payment created by the hung call", retry, rec, err)
	}

	resp, err := http.Get(srv.URL + "/v1/payments?reference=p-1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var found []record
	_ = json.NewDecoder(resp.Body).Decode(&found)
	if len(found) != 1 || found[0].Ref != rec.Ref {
		t.Fatalf("lookup by reference = %+v, want exactly the one payment", found)
	}
}

func TestSettleSendsSignedWebhooks(t *testing.T) {
	tests := []struct {
		amount   int64
		initial  string
		statuses []string
	}{
		{1250, "processing", []string{"succeeded"}},
		{1299, "processing", []string{"failed"}},
		{1298, "requires_action", []string{"processing", "succeeded"}},
		{1297, "requires_action", []string{"canceled"}},
	}
	for _, tt := range tests {
		t.Run(itoa(tt.amount), func(t *testing.T) {
			hook, got := receiver(t)
			_, srv := newTestGateway(t, hook.URL)

			_, rec, err := create(t, context.Background(), srv.URL, "k", tt.amount)
			if err != nil || rec.Status != tt.initial {
				t.Fatalf("create = %+v, %v; want %s", rec, err, tt.initial)
			}
			for _, want := range tt.statuses {
				d := <-got
				if d.Data.Status != want || d.Data.Ref != rec.Ref || !strings.HasPrefix(d.ID, "evt_") {
					t.Fatalf("delivery = %+v, want %s", d, want)
				}
			}
		})
	}
}

func TestWebhookModes(t *testing.T) {
	t.Run("duplicate sends the same event twice", func(t *testing.T) {
		hook, got := receiver(t)
		gw, srv := newTestGateway(t, hook.URL)
		gw.cfg.WebhookMode = "duplicate"
		gw.reset()

		_, _, _ = create(t, context.Background(), srv.URL, "k", 1250)
		a, b := <-got, <-got
		if a.ID != b.ID {
			t.Fatalf("ids %s and %s, want the same event redelivered", a.ID, b.ID)
		}
	})
	t.Run("drop sends nothing", func(t *testing.T) {
		hook, got := receiver(t)
		gw, srv := newTestGateway(t, hook.URL)
		resp, err := http.Post(srv.URL+"/_control?webhook=drop", "", nil)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("control: %v %v", resp, err)
		}
		resp.Body.Close()

		_, _, _ = create(t, context.Background(), srv.URL, "k", 1250)
		gw.quiesce() // lets settling finish; close would cancel it first
		if len(got) != 0 || gw.control().Stats["webhooks_dropped"] != 1 {
			t.Fatalf("deliveries = %d, stats = %v", len(got), gw.control().Stats)
		}
	})
}

// Latency delays the answer, not the execution: the payment exists, and its
// first webhook can arrive, before the caller hears back.
func TestLatencyDelaysTheAnswerAfterExecuting(t *testing.T) {
	gw, srv := newTestGateway(t, "http://127.0.0.1:1/unused")
	gw.mu.Lock()
	gw.latency = time.Hour
	gw.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if _, _, err := create(t, ctx, srv.URL, "k1", 1250); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want a client timeout", err)
	}
	if st := gw.control().Stats; st["executions"] != 1 {
		t.Fatalf("stats = %v, want the payment executed despite the slow answer", st)
	}
}

func TestReadPayment(t *testing.T) {
	_, srv := newTestGateway(t, "http://127.0.0.1:1/unused")
	_, rec, _ := create(t, context.Background(), srv.URL, "k", 1250)

	resp, err := http.Get(srv.URL + "/v1/payments/" + rec.Ref)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("read = %v %v", resp, err)
	}
	resp.Body.Close()
	missing, _ := http.Get(srv.URL + "/v1/payments/gw_missing")
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("missing = %d", missing.StatusCode)
	}
	missing.Body.Close()
}

func TestLoadConfig(t *testing.T) {
	base := []string{"GATEWAY_WEBHOOK_URL=http://payments:8080/v1/gateway/events", "GATEWAY_WEBHOOK_SECRET=" + testSecret}
	if _, err := loadConfig(base); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	for _, bad := range []string{"GATEWAY_FAULTS=429,teapot", "GATEWAY_WEBHOOK_MODE=sometimes", "GATEWAY_LATENCY=soon", "GATEWAY_COLOUR=pink"} {
		if _, err := loadConfig(append(base, bad)); err == nil {
			t.Errorf("%s: want an error", bad)
		}
	}
}
