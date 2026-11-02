package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
)

func TestLoggerAddsRequestID(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, slog.LevelInfo).With("component", "test")

	log.InfoContext(WithRequestID(context.Background(), "req-1"), "hello")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("not JSON: %q", buf.String())
	}
	if rec["request_id"] != "req-1" || rec["component"] != "test" {
		t.Fatalf("record = %v", rec)
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestGroupStopsAllComponents(t *testing.T) {
	g := NewGroup(discardLogger())
	stopped := make(chan string, 2)
	for _, name := range []string{"a", "b"} {
		g.Go(name, func(ctx context.Context) error {
			<-ctx.Done()
			stopped <- name
			return nil
		})
	}

	if err := g.Stop(); err != nil {
		t.Fatalf("Stop = %v", err)
	}
	if len(stopped) != 2 {
		t.Fatalf("stopped %d components, want 2", len(stopped))
	}
}

func TestGroupFailureCancelsOthers(t *testing.T) {
	g := NewGroup(discardLogger())
	g.Go("healthy", func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	})
	g.Go("broken", func(context.Context) error { return errors.New("boom") })

	<-g.Done() // the failure alone must trigger shutdown
	err := g.Stop()
	if err == nil || !strings.Contains(err.Error(), "broken: boom") {
		t.Fatalf("Stop = %v", err)
	}
}

func TestGroupEarlyReturnAndPanicAreFailures(t *testing.T) {
	for name, run := range map[string]func(context.Context) error{
		"early": func(context.Context) error { return nil },
		"panic": func(context.Context) error { panic("bug") },
	} {
		g := NewGroup(discardLogger())
		g.Go(name, run)
		<-g.Done()
		if err := g.Stop(); err == nil {
			t.Fatalf("%s: Stop = nil, want an error", name)
		}
	}
}

func TestReadiness(t *testing.T) {
	var r Readiness
	if r.Ready() {
		t.Fatal("must start not ready")
	}
	r.Set(true)
	if !r.Ready() {
		t.Fatal("must be ready after Set(true)")
	}
}
