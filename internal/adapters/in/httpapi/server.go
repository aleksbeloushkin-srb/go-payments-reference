package httpapi

import (
	"log/slog"
	"net/http"
	"time"
)

// ServerConfig holds the server timeouts. All four must be set: the zero
// value of each means "no limit".
type ServerConfig struct {
	Addr              string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	// WriteTimeout must exceed the long-poll soft timeout, or the server
	// cuts off a /wait response before it is written.
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

// NewServer returns an http.Server for h.
func NewServer(cfg ServerConfig, h http.Handler, log *slog.Logger) *http.Server {
	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           h,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
}
