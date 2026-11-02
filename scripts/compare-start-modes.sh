#!/usr/bin/env bash
# Compares the v1 start (in_tx: gateway call inside the transaction) with the
# v2 start (two_phase: call outside any transaction) under a slow gateway.
#
# For each mode it starts the stack, sets the fake gateway's answer latency,
# drives N payments from P concurrent clients (create, then long-poll until
# final), and reads from /metrics:
#   - database pool: acquisitions that had to wait, and the total wait,
#   - p95 of create -> final status (payment_time_to_final_seconds),
#   - p95 latency of POST /v1/payments and of the gateway webhook
#     POST /v1/gateway/events (http_request_duration_seconds): the time the
#     client and the gateway wait for us,
#   - mean time from accepting a webhook to applying it: the sum of
#     task_lag_seconds and task_duration_seconds of apply_gateway_event.
#
# The gateway executes a payment, sends its webhook after SETTLE and answers
# the create call after LATENCY, so with SETTLE < LATENCY every webhook races
# the answer, as it does with a slow gateway.
#
# Requires docker compose, curl and jq. Numbers depend on the machine; run
# both modes on the same one.
set -euo pipefail
cd "$(dirname "$0")/.."

N=${N:-60}
P=${P:-20}
LATENCY=${LATENCY:-1s}
SETTLE=${SETTLE:-200ms}
WORKERS=${WORKERS:-9}
API=http://localhost:8080
ADMIN=http://localhost:9100
export API

trap 'docker compose down -v >/dev/null 2>&1 || true' EXIT

# drive_one creates one payment and long-polls it to a final status. Like a
# well-behaved client it repeats a failed create with the same key; this also
# rides out a Docker VM's port forwarder refusing connections right after
# the stack starts.
drive_one() {
  local key="cmp-$1-$RANDOM$RANDOM" id="" version=1 resp status code body
  body=$(mktemp)
  for try in 1 2 3 4 5; do
    code=$(curl -sS -o "$body" -w '%{http_code}' -X POST "$API/v1/payments" -H "Idempotency-Key: $key" \
      -H 'Content-Type: application/json' \
      -d '{"merchant_id":"hay-and-oats-store","amount_minor":1250,"currency":"EUR"}') || code=000
    if [ "$code" = 201 ] || [ "$code" = 200 ]; then
      id=$(jq -r .id "$body")
      break
    fi
    echo "create try $try: HTTP $code $(head -c 200 "$body")" >&2
    sleep 1
  done
  rm -f "$body"
  [ -n "$id" ] || { echo "create failed for key $key" >&2; return 0; }
  for _ in $(seq 1 120); do
    resp=$(curl -sS --max-time 30 "$API/v1/payments/$id/wait?version=$version") || continue
    status=$(jq -r .status <<<"$resp")
    version=$(jq -r .version <<<"$resp")
    case $status in succeeded | failed | canceled) return 0 ;; esac
  done
  echo "payment $id did not finish" >&2
}
export -f drive_one

# value SERIES -> the value of one exact series in a /metrics snapshot.
value() { awk -v s="$1" '$1 == s { print $2 }' "$metrics"; }

# quantile Q NAME FILTER -> the Q quantile of histogram NAME over the series
# whose labels contain FILTER, interpolated inside the bucket like
# Prometheus' histogram_quantile.
quantile() {
  grep "^$2_bucket{" "$metrics" | grep -F -- "$3" |
    sed -E 's/.*le="([^"]+)".* ([0-9.e+]+)$/\1 \2/' |
    awk '{ le = ($1 == "+Inf") ? 1e308 : $1; c[le] += $2 } END { for (l in c) print l, c[l] }' |
    sort -g |
    awk -v q="$1" '
      { le[NR] = $1; cum[NR] = $2 }
      END {
        total = cum[NR]; if (total == 0) { print "n/a"; exit }
        rank = q * total; prev_le = 0; prev_c = 0
        for (i = 1; i <= NR; i++) {
          if (cum[i] >= rank) {
            if (le[i] >= 1e308) { printf "%.3f\n", prev_le; exit }
            printf "%.3f\n", prev_le + (le[i] - prev_le) * (rank - prev_c) / (cum[i] - prev_c); exit
          }
          prev_le = le[i]; prev_c = cum[i]
        }
      }'
}

declare -a rows
for mode in in_tx two_phase; do
  echo "==> $mode: starting the stack" >&2
  export PAYMENTS_START_MODE=$mode PAYMENTS_WORKER_CONCURRENCY=$WORKERS GATEWAY_LATENCY=$LATENCY GATEWAY_SETTLE_DELAY=$SETTLE
  docker compose build payments >/dev/null
  docker compose up -d --wait payments >/dev/null

  echo "==> $mode: $N payments from $P clients, gateway answers after $LATENCY" >&2
  start=$SECONDS
  seq 1 "$N" | xargs -P "$P" -I{} bash -c 'drive_one {}'
  wall=$((SECONDS - start))

  metrics=$(mktemp)
  curl -sS "$ADMIN/metrics" >"$metrics"
  final=$(value 'payment_time_to_final_seconds_count{status="succeeded"}')
  wait_total=$(value db_pool_acquire_wait_seconds_total)
  empty=$(value db_pool_empty_acquires_total)
  apply=$(awk -v ls="$(value 'task_lag_seconds_sum{kind="apply_gateway_event"}')" \
    -v ds="$(value 'task_duration_seconds_sum{kind="apply_gateway_event"}')" \
    -v c="$(value 'task_duration_seconds_count{kind="apply_gateway_event"}')" \
    'BEGIN { print (c > 0) ? (ls + ds) / c : 0 }')
  rows+=("$(printf '%-10s %5s %4ss %7s %8.3f %9.3f %9.3f %9.3f %9.3f' \
    "$mode" "$final" "$wall" "$empty" "$wait_total" \
    "$(quantile 0.95 payment_time_to_final_seconds 'status="succeeded"')" \
    "$(quantile 0.95 http_request_duration_seconds 'route="/v1/payments"')" \
    "$(quantile 0.95 http_request_duration_seconds 'route="/v1/gateway/events"')" "$apply")")
  rm -f "$metrics"
  docker compose down -v >/dev/null 2>&1
done

echo
echo "N=$N payments, P=$P clients, gateway answer latency $LATENCY, webhook after $SETTLE, $WORKERS workers, pool of 12"
printf '%-10s %5s %5s %7s %8s %9s %9s %9s %9s\n' mode final wall waited pool_wait p95_final p95_create p95_hook hook_apply
printf '%-10s %5s %5s %7s %8s %9s %9s %9s %9s\n' "" "" "" acquires "s total" s s s "mean s"
printf '%s\n' "${rows[@]}"
