// Command fakegateway simulates the external payment gateway used by the
// payments service in development, tests and the end-to-end script.
//
// It deduplicates create requests by Idempotency-Key, settles payments after
// a delay, sends signed webhooks, and can inject failures: latency, 429 with
// Retry-After, 500, 409 "same key in progress", 401 "bad credentials",
// "execute then hang" (an ambiguous outcome), dropped and duplicated
// webhooks. Configure it with GATEWAY_ variables or at run time
// through /_control.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(ctx, stop, log); err != nil {
		log.Error("fakegateway failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, stopSignals context.CancelFunc, log *slog.Logger) error {
	cfg, err := loadConfig(os.Environ())
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	gw := newGateway(cfg, log, &http.Client{Timeout: 10 * time.Second})
	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           gw.handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	srv.RegisterOnShutdown(gw.stop)

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return err
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	log.Info("fake gateway listening", "addr", cfg.Addr, "faults", cfg.Faults, "webhook_mode", cfg.WebhookMode)

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		return err
	}
	stopSignals()

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	gw.close()
	if serr := <-serveErr; !errors.Is(serr, http.ErrServerClosed) {
		err = errors.Join(err, serr)
	}
	return err
}
