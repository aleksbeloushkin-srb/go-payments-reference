#!/usr/bin/env bash
# End-to-end check of the whole stack with docker compose and curl.
#
# It creates payments, follows each one to a final status through long
# polling, and drives the fake gateway's failure modes: 429 with
# Retry-After, 500, an executed-then-hung call, dropped and duplicated
# webhooks, and the decline / confirmation / cancel outcomes.
#
# Requires docker compose, curl and jq. KEEP=1 leaves the stack running.
# START_MODE=two_phase (default) or in_tx selects the v2 or the v1 start.
set -euo pipefail
cd "$(dirname "$0")/.."

API=${API:-http://localhost:8080}
ADMIN=${ADMIN:-http://localhost:9100}
PROM=${PROM:-http://localhost:9091}
GW=${GW:-http://localhost:9090}
KEEP=${KEEP:-0}
START_MODE=${START_MODE:-two_phase}
case $START_MODE in two_phase | in_tx) ;; *)
  echo "START_MODE must be two_phase or in_tx" >&2
  exit 2
  ;;
esac
export PAYMENTS_START_MODE=$START_MODE
# Run the duplicate check after an abandoned attempt within the test.
export PAYMENTS_DUPLICATE_CHECK_DELAY=${PAYMENTS_DUPLICATE_CHECK_DELAY:-5s}

pass=0
log() { printf '%s\n' "$*" >&2; }
ok() { pass=$((pass + 1)); log "  ok: $*"; }
fail() {
  log "FAIL: $*"
  docker compose logs --tail=80 payments fakegateway >&2 || true
  exit 1
}

body_file=$(mktemp)
cleanup() {
  rm -f "$body_file"
  if [ "$KEEP" != 1 ]; then
    docker compose down -v >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

sql() { docker compose exec -T postgres psql -U payments -d payments -tAc "$1"; }

# attempts_of ID -> the states of the payment's gateway attempts, in order
attempts_of() { sql "SELECT coalesce(string_agg(state, ',' ORDER BY attempt), '') FROM gateway_attempts WHERE payment_id='$1'"; }
two_phase() { [ "$START_MODE" = two_phase ]; }

# create KEY AMOUNT -> prints "HTTP_CODE BODY"
create() {
  local code
  code=$(curl -sS -o "$body_file" -w '%{http_code}' -X POST "$API/v1/payments" \
    -H "Idempotency-Key: $1" -H 'Content-Type: application/json' \
    -d "{\"merchant_id\":\"hay-and-oats-store\",\"amount_minor\":$2,\"currency\":\"EUR\"}")
  printf '%s %s\n' "$code" "$(cat "$body_file")"
}

# new_payment AMOUNT -> prints the id of a freshly created payment
new_payment() {
  local out code
  out=$(create "e2e-$RANDOM-$RANDOM-$SECONDS" "$1")
  code=${out%% *}
  [ "$code" = 201 ] || fail "create returned $out"
  jq -r .id <<<"${out#* }"
}

# follow ID -> long-polls until the payment is final, prints the final status
follow() {
  local id=$1 version=0 deadline=$((SECONDS + 90)) resp status modified
  while [ $SECONDS -lt $deadline ]; do
    resp=$(curl -sS --max-time 40 "$API/v1/payments/$id/wait?version=$version") || continue
    modified=$(jq -r .modified <<<"$resp")
    status=$(jq -r .status <<<"$resp")
    version=$(jq -r .version <<<"$resp")
    [ "$modified" = true ] && log "    $id: $status (v$version)"
    case $status in succeeded | failed | canceled)
      echo "$status"
      return 0
      ;;
    esac
  done
  fail "payment $id did not reach a final status in time"
}

gw_control() { curl -sS -X POST "$GW/_control?$1" >/dev/null; }
gw_stat() { curl -sS "$GW/_control" | jq -r ".stats.\"$1\" // 0"; }
expect() { [ "$2" = "$3" ] || fail "$1: got '$2', want '$3'"; ok "$1"; }

# metric SERIES -> the value of one exact series from /metrics, e.g.
# 'http_requests_total{code="201",method="POST",route="/v1/payments"}'
metric() { curl -sS "$ADMIN/metrics" | awk -v s="$1" '$1 == s { print $2 }'; }
at_least() {
  awk -v v="${2:-}" -v w="$3" 'BEGIN { exit !(v != "" && v + 0 >= w) }' || fail "$1: got '${2:-missing}', want >= $3"
  ok "$1"
}
prom_query() { curl -sS "$PROM/api/v1/query" --data-urlencode "query=$1" | jq -r '.data.result[0].value[1] // empty'; }

log "==> starting the stack"
# Both services run the same image: build it once, then start everything.
docker compose build payments
docker compose up -d --wait
docker compose logs payments | grep -q "\"start_mode\":\"$START_MODE\"" || fail "payments did not start in $START_MODE mode"
ok "running with PAYMENTS_START_MODE=$START_MODE"

log "==> service and web UI"
expect "readyz on the admin port" "$(curl -sS -o /dev/null -w '%{http_code}' "$ADMIN/readyz")" 200
curl -sS "$API/" | grep -q "Pinkponies Bank" || fail "web UI does not render"
ok "web UI page"
for asset in app.js style.css logo.svg; do
  expect "asset $asset" "$(curl -sS -o /dev/null -w '%{http_code}' "$API/$asset")" 200
done

log "==> idempotent create"
created_before=$(metric payments_created_total)
[ -n "$created_before" ] || fail "payments_created_total is not exported"
out=$(create e2e-key-1 1250)
expect "first create" "${out%% *}" 201
id=$(jq -r .id <<<"${out#* }")
out=$(create e2e-key-1 1250)
expect "replay with the same key" "${out%% *}" 200
expect "replay returns the same payment" "$(jq -r .id <<<"${out#* }")" "$id"
out=$(create e2e-key-1 9999)
expect "same key, different body" "${out%% *}" 422
expect "happy path" "$(follow "$id")" succeeded
if two_phase; then
  expect "one gateway attempt, done" "$(attempts_of "$id")" done
  expect "attempt 1 uses the v1 key, the payment id" "$(sql "SELECT idempotency_key FROM gateway_attempts WHERE payment_id='$id'")" "$id"
else
  expect "v1 records no attempts" "$(attempts_of "$id")" ""
fi
expect "payments_created_total grew by one, replays not counted" "$(metric payments_created_total)" "$((created_before + 1))"
at_least "transition to succeeded counted" "$(metric 'payment_transitions_total{from="processing",to="succeeded"}')" 1

log "==> gateway answers 429 twice, then accepts"
gw_control "faults=429,429"
before=$(gw_stat fault_429)
id=$(new_payment 2000)
expect "payment after rate limiting" "$(follow "$id")" succeeded
expect "429 answers served" "$(($(gw_stat fault_429) - before))" 2

log "==> gateway answers 500 (ambiguous)"
gw_control "faults=500"
id=$(new_payment 2100)
expect "payment after a 500" "$(follow "$id")" succeeded
if two_phase; then
  # Reconciliation finds nothing at the gateway, abandons attempt 1 and
  # starts attempt 2 with a new key.
  expect "attempt 1 abandoned, attempt 2 done" "$(attempts_of "$id")" "abandoned,done"
  expect "attempt 2 has a new key" "$(sql "SELECT idempotency_key FROM gateway_attempts WHERE payment_id='$id' AND attempt=2")" "$id:2"
  deadline=$((SECONDS + 30))
  until [ "$(sql "SELECT state FROM tasks WHERE kind='check_duplicates' AND payment_id='$id'")" = done ]; do
    [ $SECONDS -lt $deadline ] || fail "the duplicate check after the abandoned attempt did not finish"
    sleep 1
  done
  ok "duplicate check after the abandoned attempt found one gateway payment"
else
  expect "start task retried with the same key" "$(sql "SELECT attempts FROM tasks WHERE kind='start_payment' AND payment_id='$id'")" 1
fi
expect "the client was charged once after a 500" "$(curl -sS "$GW/v1/payments?reference=$id" | jq length)" 1

log "==> gateway answers 409 (same key in progress), then 401 (bad credentials)"
gw_control "faults=409,401"
id=$(new_payment 2150)
expect "payment after 409 and 401 is not failed" "$(follow "$id")" succeeded
# 409 means the gateway knows the key: both modes retry with the same key.
expect "start task retried twice" "$(sql "SELECT attempts FROM tasks WHERE kind='start_payment' AND payment_id='$id'")" 2
if two_phase; then
  expect "one attempt, never abandoned" "$(attempts_of "$id")" done
fi

log "==> gateway executes the payment, then hangs (ambiguous outcome)"
gw_control "faults=hang"
before=$(gw_stat fault_hang)
executions_before=$(gw_stat executions)
id=$(new_payment 2200)
expect "payment after a hung call" "$(follow "$id")" succeeded
expect "the hung call happened" "$(($(gw_stat fault_hang) - before))" 1
# Whichever path finished the payment (webhook, retry or reconciliation),
# the gateway must have executed it exactly once.
expect "exactly one gateway execution" "$(($(gw_stat executions) - executions_before))" 1
expect "the client was charged once" "$(curl -sS "$GW/v1/payments?reference=$id" | jq length)" 1
if two_phase; then
  deadline=$((SECONDS + 30))
  until [ "$(attempts_of "$id")" = done ]; do
    [ $SECONDS -lt $deadline ] || fail "the hung attempt was not closed: $(attempts_of "$id")"
    sleep 1
  done
  # The gateway's event arrives before the call times out and closes the
  # attempt; reconciliation is needed only if it does not.
  ok "the hung attempt was closed"
fi

log "==> webhooks dropped: polling finishes the job"
gw_control "webhook=drop"
id=$(new_payment 2300)
expect "payment without webhooks" "$(follow "$id")" succeeded
expect "no gateway event applied" "$(sql "SELECT count(*) FROM tasks WHERE kind='apply_gateway_event' AND payment_id='$id'")" 0
gw_control "webhook=normal"

log "==> webhooks duplicated: applied once"
gw_control "webhook=duplicate"
id=$(new_payment 2400)
expect "payment with duplicate webhooks" "$(follow "$id")" succeeded
gw_control "webhook=normal"
expect "one task per event despite two deliveries" \
  "$(sql "SELECT count(*) FROM tasks WHERE kind='apply_gateway_event' AND payment_id='$id'")" 1

log "==> outcomes chosen by amount"
expect "…99 is declined" "$(follow "$(new_payment 1299)")" failed
id=$(new_payment 1298)
expect "…98 needs a confirmation, then succeeds" "$(follow "$id")" succeeded
expect "requires_action was recorded" \
  "$(sql "SELECT count(*) FROM outbox WHERE payment_id='$id' AND payload->>'status'='requires_action'")" 1
expect "…97 is canceled" "$(follow "$(new_payment 1297)")" canceled

log "==> outbox relay publishes every event"
deadline=$((SECONDS + 20))
until [ "$(sql "SELECT count(*) FROM outbox WHERE published_at IS NULL")" = 0 ]; do
  [ $SECONDS -lt $deadline ] || fail "outbox events left unpublished"
  sleep 0.5
done
ok "outbox drained ($(sql "SELECT count(*) FROM outbox") events)"
expect "no dead tasks" "$(sql "SELECT count(*) FROM tasks WHERE state='dead'")" 0

log "==> metrics on the admin port"
at_least "POST 201 counted by route" "$(metric 'http_requests_total{code="201",method="POST",route="/v1/payments"}')" 1
at_least "replay 200 counted" "$(metric 'http_requests_total{code="200",method="POST",route="/v1/payments"}')" 1
at_least "conflict 422 counted" "$(metric 'http_requests_total{code="422",method="POST",route="/v1/payments"}')" 1
at_least "long polls in their own histogram" "$(metric http_long_poll_duration_seconds_count)" 1
at_least "start tasks succeeded" "$(metric 'task_attempts_total{kind="start_payment",outcome="ok"}')" 1
if two_phase; then
  # 500 and the hung call go to reconciliation; 409 and 401 are task retries.
  at_least "start tasks retried" "$(metric 'task_attempts_total{kind="start_payment",outcome="retry"}')" 2
  at_least "reconcile counted the not-found answers" "$(metric 'reconcile_total{outcome="not_found"}')" 2
  at_least "reconcile abandoned the unexecuted attempt" "$(metric 'reconcile_total{outcome="abandoned"}')" 1
  expect "no duplicate charge detected" "$(metric gateway_duplicates_detected_total)" 0
  expect "no charge for a failed payment detected" "$(metric gateway_charge_mismatches_total)" 0
  expect "no attempt left open" "$(metric 'gateway_attempts{state="unknown"}')" 0
  expect "no open attempt age" "$(metric gateway_attempt_oldest_open_seconds)" 0
else
  at_least "start tasks retried" "$(metric 'task_attempts_total{kind="start_payment",outcome="retry"}')" 4
fi
at_least "gateway creates ok" "$(metric 'gateway_requests_total{op="create",outcome="ok"}')" 1
at_least "gateway 429 counted" "$(metric 'gateway_requests_total{op="create",outcome="rate_limited"}')" 2
at_least "gateway 401 counted as misconfigured" "$(metric 'gateway_requests_total{op="create",outcome="misconfigured"}')" 1
at_least "gateway unknown outcomes counted" "$(metric 'gateway_requests_total{op="create",outcome="ambiguous"}')" 3
at_least "outbox events published" "$(metric outbox_published_total)" 1
at_least "time to final observed" "$(metric 'payment_time_to_final_seconds_count{status="succeeded"}')" 1
expect "no dead tasks in the gauge" "$(metric 'tasks_dead{kind="start_payment"}')" 0
for series in task_oldest_ready_seconds outbox_oldest_unpublished_seconds longpoll_waiters \
  db_pool_total_conns db_pool_acquire_wait_seconds_total go_goroutines process_resident_memory_bytes; do
  [ -n "$(metric "$series")" ] || fail "series $series is missing"
done
ok "database, pool, runtime and process series present"
expect "no ids in labels" "$(curl -sS "$ADMIN/metrics" | grep -cE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-' || true)" 0

log "==> Prometheus scrapes the admin port and evaluates the alert rules"
deadline=$((SECONDS + 60))
until [ "$(prom_query 'up{job="payments"}')" = 1 ]; do
  [ $SECONDS -lt $deadline ] || fail "Prometheus does not scrape the payments admin port"
  sleep 1
done
ok "scrape target is up"
expect "alert rule groups loaded" "$(curl -sS "$PROM/api/v1/rules" | jq '.data.groups | length')" 4
expect "alert rules loaded" "$(curl -sS "$PROM/api/v1/rules" | jq '[.data.groups[].rules[]] | length')" 12
deadline=$((SECONDS + 60))
until curl -sS "$PROM/api/v1/alerts" | jq -e '.data.alerts[] | select(.labels.alertname == "GatewayMisconfigured")' >/dev/null; do
  [ $SECONDS -lt $deadline ] || fail "GatewayMisconfigured did not fire after the 401 answer"
  sleep 1
done
ok "GatewayMisconfigured fires after the 401 answer"
expect "DeadTasks does not fire" "$(curl -sS "$PROM/api/v1/alerts" | jq '[.data.alerts[] | select(.labels.alertname == "DeadTasks")] | length')" 0
expect "GatewayDuplicateCharge does not fire" "$(curl -sS "$PROM/api/v1/alerts" | jq '[.data.alerts[] | select(.labels.alertname == "GatewayDuplicateCharge")] | length')" 0
expect "GatewayChargeMismatch does not fire" "$(curl -sS "$PROM/api/v1/alerts" | jq '[.data.alerts[] | select(.labels.alertname == "GatewayChargeMismatch")] | length')" 0

log "==> graceful stop"
docker compose stop payments >/dev/null
docker compose logs payments | grep -q '"msg":"payments stopped"' || fail "payments did not stop cleanly"
ok "payments stopped cleanly"

log "PASS: $pass checks"
