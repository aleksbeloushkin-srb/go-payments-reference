package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewAdminHandler serves the operator endpoints of the admin listener:
// Prometheus metrics, liveness and readiness. The admin port is meant for
// the cluster network (the scraper, the orchestrator's probes), never for
// clients, and it runs in every role, so a worker-only process is observable
// too.
func NewAdminHandler(metrics prometheus.Gatherer, ready func() bool, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(metrics, promhttp.HandlerOpts{
		ErrorLog: slog.NewLogLogger(log.Handler(), slog.LevelError),
	}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	// readyz turns 503 as soon as shutdown starts, so the load balancer
	// stops sending new requests before the server stops accepting them.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	return mux
}
