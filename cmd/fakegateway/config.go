package main

import (
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"time"
)

const envPrefix = "GATEWAY_"

// config of the fake gateway. Defaults favour a demo you can watch.
type config struct {
	Addr string
	// WebhookURL receives status events, signed with WebhookSecret.
	WebhookURL    string
	WebhookSecret string
	// SettleDelay is the pause before each status change, long enough to see
	// "processing" in the UI.
	SettleDelay time.Duration
	// Latency is added to every create call.
	Latency time.Duration
	// RetryAfter is sent with injected 429 answers.
	RetryAfter time.Duration
	// Faults is the initial fault queue, consumed one per create call.
	Faults []string
	// WebhookMode is normal, drop or duplicate.
	WebhookMode string
}

var (
	knownFaults       = []string{"429", "500", "409", "401", "hang"}
	knownWebhookModes = []string{"normal", "drop", "duplicate"}
)

func loadConfig(environ []string) (config, error) {
	env := map[string]string{}
	for _, kv := range environ {
		if k, v, ok := strings.Cut(kv, "="); ok && strings.HasPrefix(k, envPrefix) {
			env[strings.TrimPrefix(k, envPrefix)] = v
		}
	}
	var errs []error
	take := func(name, def string) string {
		v, ok := env[name]
		delete(env, name)
		if !ok {
			return def
		}
		return v
	}
	duration := func(name string, def time.Duration) time.Duration {
		v := take(name, "")
		if v == "" {
			return def
		}
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			errs = append(errs, fmt.Errorf("%s%s: invalid duration %q", envPrefix, name, v))
		}
		return d
	}

	cfg := config{
		Addr:          take("ADDR", ":9090"),
		WebhookURL:    take("WEBHOOK_URL", ""),
		WebhookSecret: take("WEBHOOK_SECRET", ""),
		SettleDelay:   duration("SETTLE_DELAY", 1500*time.Millisecond),
		Latency:       duration("LATENCY", 0),
		RetryAfter:    duration("RETRY_AFTER", time.Second),
		WebhookMode:   take("WEBHOOK_MODE", "normal"),
	}
	faults, err := parseFaults(take("FAULTS", ""))
	if err != nil {
		errs = append(errs, err)
	}
	cfg.Faults = faults

	for _, k := range slices.Sorted(maps.Keys(env)) {
		errs = append(errs, fmt.Errorf("%s%s: unknown variable", envPrefix, k))
	}
	if u, err := url.Parse(cfg.WebhookURL); err != nil || u.Host == "" {
		errs = append(errs, fmt.Errorf("%sWEBHOOK_URL must be an http(s) URL", envPrefix))
	}
	if len(cfg.WebhookSecret) < 16 {
		errs = append(errs, fmt.Errorf("%sWEBHOOK_SECRET must be at least 16 bytes", envPrefix))
	}
	if !slices.Contains(knownWebhookModes, cfg.WebhookMode) {
		errs = append(errs, fmt.Errorf("%sWEBHOOK_MODE must be one of %v", envPrefix, knownWebhookModes))
	}
	return cfg, errors.Join(errs...)
}

// parseFaults reads a comma-separated fault list such as "429,429,hang".
func parseFaults(s string) ([]string, error) {
	var out []string
	for _, f := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' }) {
		f = strings.TrimSpace(f)
		if !slices.Contains(knownFaults, f) {
			return nil, fmt.Errorf("unknown fault %q (want one of %v)", f, knownFaults)
		}
		out = append(out, f)
	}
	return out, nil
}
