#!/usr/bin/env bash
# End-to-end check of the whole stack with docker compose and curl.
#
# It creates payments, follows each one to a final status through long
# polling, and drives the fake gateway's failure modes: 429 with
# Retry-After, 500, an executed-then-hung call, dropped and duplicated
# webhooks, and the decline / confirmation / cancel outcomes.
#
# Requires docker compose, curl and jq. KEEP=1 leaves the stack running.
set -euo pipefail
cd "$(dirname "$0")/.."

API=${API:-http://localhost:8080}
GW=${GW:-http://localhost:9090}
KEEP=${KEEP:-0}

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

log "==> starting the stack"
# Both services run the same image: build it once, then start everything.
docker compose build payments
docker compose up -d --wait

log "==> service and web UI"
expect "readyz" "$(curl -sS -o /dev/null -w '%{http_code}' "$API/readyz")" 200
curl -sS "$API/" | grep -q "Pinkponies Bank" || fail "web UI does not render"
ok "web UI page"
for asset in app.js style.css logo.svg; do
  expect "asset $asset" "$(curl -sS -o /dev/null -w '%{http_code}' "$API/$asset")" 200
done

log "==> idempotent create"
out=$(create e2e-key-1 1250)
expect "first create" "${out%% *}" 201
id=$(jq -r .id <<<"${out#* }")
out=$(create e2e-key-1 1250)
expect "replay with the same key" "${out%% *}" 200
expect "replay returns the same payment" "$(jq -r .id <<<"${out#* }")" "$id"
out=$(create e2e-key-1 9999)
expect "same key, different body" "${out%% *}" 422
expect "happy path" "$(follow "$id")" succeeded

log "==> gateway answers 429 twice, then accepts"
gw_control "faults=429,429"
before=$(gw_stat fault_429)
id=$(new_payment 2000)
expect "payment after rate limiting" "$(follow "$id")" succeeded
expect "429 answers served" "$(($(gw_stat fault_429) - before))" 2

log "==> gateway answers 500 (ambiguous): task retried with the same key"
gw_control "faults=500"
id=$(new_payment 2100)
expect "payment after a 500" "$(follow "$id")" succeeded
expect "start task needed a second attempt" "$(sql "SELECT attempts FROM tasks WHERE kind='start_payment' AND payment_id='$id'")" 1

log "==> gateway answers 409 (same key in progress), then 401 (bad credentials)"
gw_control "faults=409,401"
id=$(new_payment 2150)
expect "payment after 409 and 401 is not failed" "$(follow "$id")" succeeded
expect "start task needed two retries" "$(sql "SELECT attempts FROM tasks WHERE kind='start_payment' AND payment_id='$id'")" 2

log "==> gateway executes the payment, then hangs (ambiguous outcome)"
gw_control "faults=hang"
before=$(gw_stat fault_hang)
id=$(new_payment 2200)
expect "payment after a hung call" "$(follow "$id")" succeeded
expect "the hung call happened" "$(($(gw_stat fault_hang) - before))" 1
# Whether the retry or the webhook finished the payment depends on timing;
# either way the client must have been charged exactly once.
expect "the client was charged once" "$(curl -sS "$GW/v1/payments?reference=$id" | jq length)" 1

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

log "==> graceful stop"
docker compose stop payments >/dev/null
docker compose logs payments | grep -q '"msg":"payments stopped"' || fail "payments did not stop cleanly"
ok "payments stopped cleanly"

log "PASS: $pass checks"
