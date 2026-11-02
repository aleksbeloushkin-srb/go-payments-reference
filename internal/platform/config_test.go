package platform

import (
	"strings"
	"testing"
	"time"
)

func baseEnv() []string {
	return []string{
		"PAYMENTS_DATABASE_URL=postgres://localhost/payments",
		"PAYMENTS_GATEWAY_URL=http://localhost:9090",
		"PAYMENTS_WEBHOOK_SECRET=0123456789abcdef",
		"HOME=/root", // variables without the prefix are ignored
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := LoadConfig(baseEnv())
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if !cfg.Roles.API || !cfg.Roles.Worker || !cfg.Roles.Relay {
		t.Fatalf("roles = %+v, want all", cfg.Roles)
	}
	if cfg.HTTPWriteTimeout <= cfg.WaitTimeout {
		t.Fatal("default write timeout must exceed the long-poll soft timeout")
	}
	if cfg.WaitTimeout != 15*time.Second || cfg.TaskLease != time.Minute || cfg.AdminAddr != ":9100" || cfg.StartMode != "two_phase" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadConfigErrors(t *testing.T) {
	tests := []struct {
		name string
		env  []string
		want string
	}{
		{"unknown variable", []string{"PAYMENTS_WAIT_TIMOUT=10s"}, "PAYMENTS_WAIT_TIMOUT: unknown variable"},
		{"bad duration", []string{"PAYMENTS_WAIT_TIMEOUT=ten"}, "PAYMENTS_WAIT_TIMEOUT"},
		{"bad integer", []string{"PAYMENTS_RELAY_BATCH=many"}, "PAYMENTS_RELAY_BATCH"},
		{"zero timeout", []string{"PAYMENTS_GATEWAY_TIMEOUT=0s"}, "GATEWAY_TIMEOUT must be positive"},
		{"unknown role", []string{"PAYMENTS_ROLES=api,cron"}, `unknown role "cron"`},
		{"no roles", []string{"PAYMENTS_ROLES=,"}, "at least one role"},
		{"write timeout too short", []string{"PAYMENTS_HTTP_WRITE_TIMEOUT=16s"}, "HTTP_WRITE_TIMEOUT must exceed WAIT_TIMEOUT"},
		{"task timeout vs lease", []string{"PAYMENTS_TASK_LEASE=35s"}, "TASK_TIMEOUT must be at least 10s shorter"},
		{"gateway budget", []string{"PAYMENTS_GATEWAY_TIMEOUT=10s"}, "worst gateway budget"},
		{"pool too small", []string{"PAYMENTS_DB_MAX_CONNS=5"}, "WORKER_CONCURRENCY must be at most"},
		{"idle in tx", []string{"PAYMENTS_DB_IDLE_IN_TX_TIMEOUT=30s"}, "DB_IDLE_IN_TX_TIMEOUT must exceed"},
		{"short secret", []string{"PAYMENTS_WEBHOOK_SECRET=short"}, "WEBHOOK_SECRET must be at least 16"},
		{"bad gateway url", []string{"PAYMENTS_GATEWAY_URL=localhost:9090"}, "GATEWAY_URL must be an http(s) URL"},
		{"admin on the api port", []string{"PAYMENTS_ADMIN_ADDR=:8080"}, "ADMIN_ADDR must differ from HTTP_ADDR"},
		{"unknown start mode", []string{"PAYMENTS_START_MODE=eventually"}, `"eventually" is not one of [two_phase in_tx]`},
		{"no reconcile lookups", []string{"PAYMENTS_RECONCILE_NOT_FOUND_LIMIT=0"}, "RECONCILE_NOT_FOUND_LIMIT must be at least 1"},
		{"abandon window too short", []string{"PAYMENTS_RECONCILE_INTERVAL=2s"}, "RECONCILE_INTERVAL x RECONCILE_NOT_FOUND_LIMIT must cover"},
		{"no gateway attempts", []string{"PAYMENTS_MAX_GATEWAY_ATTEMPTS=0"}, "MAX_GATEWAY_ATTEMPTS must be between 1 and 10"},
		{"no margin after the gateway budget", []string{"PAYMENTS_TASK_TIMEOUT=25s", "PAYMENTS_TASK_LEASE=60s"}, "must exceed the worst gateway budget"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadConfig(append(baseEnv(), tt.env...))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

func TestLoadConfigMissingDatabaseURL(t *testing.T) {
	_, err := LoadConfig([]string{"PAYMENTS_ROLES=relay"})
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL is required") {
		t.Fatalf("err = %v", err)
	}
}

// Requirements depend on roles: a relay-only process needs neither the
// gateway URL nor the webhook secret.
func TestLoadConfigRoleSpecificRequirements(t *testing.T) {
	cfg, err := LoadConfig([]string{"PAYMENTS_DATABASE_URL=postgres://db/payments", "PAYMENTS_ROLES=relay"})
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Roles.API || cfg.Roles.Worker || !cfg.Roles.Relay {
		t.Fatalf("roles = %+v", cfg.Roles)
	}
}
