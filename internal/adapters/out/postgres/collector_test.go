package postgres

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/aleksbeloushkin-srb/go-payments-reference/pkg/txctx"
)

// failingDB fails every query. Its embedded interface is nil: the collector
// only calls Query.
type failingDB struct {
	txctx.Querier
}

func (failingDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("connection refused")
}

// When the database cannot be read, the collector emits only the error
// counter: no stale gauges.
func TestQueueCollectorQueryError(t *testing.T) {
	c := NewQueueCollector(failingDB{}, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)

	for range 2 {
		mfs, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		if len(mfs) != 1 || mfs[0].GetName() != "metrics_collect_errors_total" {
			t.Fatalf("gathered %d families, want only metrics_collect_errors_total", len(mfs))
		}
	}
	if got := testutil.ToFloat64(c.errors); got != 2 {
		t.Fatalf("metrics_collect_errors_total = %v, want 2", got)
	}
}
