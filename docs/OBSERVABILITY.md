# Observability

The service exports Prometheus metrics on a separate admin listener, and
`deploy/prometheus/` holds a scrape config and alert rules that the local
stack runs. Tracing is planned, not built (IMPROVEMENTS.md, item 8).

```sh
curl localhost:9100/metrics      # the admin listener of the payments service
open http://localhost:9091       # Prometheus in docker compose; alerts at /alerts
make check-alerts                # promtool: config check and alert rule tests
```

## How it is wired

- **One registry, injected.** `cmd/payments/main.go` creates a
  `*prometheus.Registry` and passes it to each component's `NewMetrics`
  constructor. Nothing registers on the global default registry, so tests
  create as many independent instances as they need.
- **Each adapter owns its metrics.** HTTP, queue, outbox, gateway and long
  polling each define a small `Metrics` type next to the code that records
  it. Business metrics go through the `ports.Metrics` port, so the use cases
  stay free of Prometheus.
- **Business metrics count committed work only.** The use cases report a
  created payment or a status change after `Run` returns without error. A
  replay, a rolled-back start or a duplicate event never moves a counter.
- **Database state is read on scrape.** A custom collector runs one SQL
  query per scrape, with a 2 s timeout, for the queue and outbox gauges. A
  second collector reads `pgxpool.Stat()`. The Go runtime and process
  collectors are registered too.
- **Admin listener.** `/metrics`, `/healthz` and `/readyz` are served on
  `PAYMENTS_ADMIN_ADDR` (default `:9100`), not on the API port. The listener
  runs in every role, so worker-only and relay-only processes are observable,
  and it shuts down last, after the API and the background components.

## Metrics

| Name | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `http_requests_total` | counter | `route`, `method`, `code` | API requests. `route` is the ServeMux pattern without the method (`/v1/payments/{id}`), `unmatched` if none matched. |
| `http_request_duration_seconds` | histogram | `route`, `method` | API latency, long polls excluded. |
| `http_long_poll_duration_seconds` | histogram | none | Duration of `GET /v1/payments/{id}/wait`. |
| `payments_created_total` | counter | none | Payments accepted; replays of an Idempotency-Key are not counted. |
| `payment_transitions_total` | counter | `from`, `to` | Committed status changes. |
| `payment_time_to_final_seconds` | histogram | `status` | Time from creation to `succeeded`, `failed` or `canceled`. |
| `tasks_claimed_total` | counter | `kind` | Tasks claimed by this worker. |
| `task_attempts_total` | counter | `kind`, `outcome` | Settled runs: `ok` (done or postponed by polling), `retry`, `permanent` (dead at once), `dead` (out of attempts). |
| `task_lag_seconds` | histogram | `kind` | Claim time minus `run_after`: how long a due task waited. |
| `task_duration_seconds` | histogram | `kind` | Handler run time, including row-lock waits and gateway calls. For `apply_gateway_event` it is the time to apply a webhook. |
| `lease_lost_total` | counter | none | Results discarded because another worker took over the lease. |
| `task_panics_total` | counter | none | Recovered handler panics. |
| `tasks_ready` | gauge | `kind` | Tasks in the ready state, including those scheduled for later. |
| `tasks_dead` | gauge | `kind` | Tasks in the dead state, waiting for an operator. |
| `task_oldest_ready_seconds` | gauge | none | Age of the oldest due, unfinished task; 0 when none. |
| `outbox_oldest_unpublished_seconds` | gauge | none | Age of the oldest unpublished event; 0 when none. |
| `gateway_attempts` | gauge | `state` | Open gateway attempts of the two-phase start, `intent` and `unknown`. Done and abandoned attempts are terminal and only grow; abandons are counted by `reconcile_total`. |
| `gateway_attempt_oldest_open_seconds` | gauge | none | Age of the oldest `intent` or `unknown` attempt; 0 when none. |
| `reconcile_total` | counter | `outcome` | Reconciliations of ambiguous attempts: `found` (an answer applied), `not_found`, `abandoned`, `duplicate`, `error`. |
| `gateway_duplicates_detected_total` | counter | none | Times the gateway was found holding more than one payment for one bank payment, by reconciliation or by the duplicate check after an abandoned attempt: a double charge to refund. |
| `metrics_collect_errors_total` | counter | none | Scrapes in which the database gauges could not be read. |
| `outbox_published_total` | counter | none | Events published and marked. |
| `outbox_failed_total` | counter | none | Failed deliveries; the events are released and retried. |
| `gateway_requests_total` | counter | `op`, `outcome` | One per HTTP exchange with the gateway, in-line retries included. `op` is `create`, `read` or `lookup` (by reference, for reconciliation); `outcome` is `ok`, `declined`, `rate_limited`, `ambiguous`, `misconfigured` or `error`. |
| `gateway_request_duration_seconds` | histogram | `op` | Duration of one gateway exchange. |
| `longpoll_waiters` | gauge | none | Long polls waiting on this instance. |
| `longpoll_rejected_total` | counter | none | Long polls answered 429 because the instance cap was reached. |
| `longpoll_batch_query_duration_seconds` | histogram | none | Duration of the batcher's version query. |
| `db_pool_acquired_conns`, `db_pool_idle_conns`, `db_pool_total_conns`, `db_pool_max_conns` | gauge | none | Pool connections by state, and the pool size. |
| `db_pool_acquires_total` | counter | none | Successful acquisitions. |
| `db_pool_empty_acquires_total` | counter | none | Acquisitions that had to wait for a free connection. |
| `db_pool_acquire_wait_seconds_total` | counter | none | Cumulative time spent acquiring connections. |
| `go_*`, `process_*` | various | standard | Go runtime and process metrics from the standard collectors. |

## Choices worth knowing

- **Long polls have their own histogram.** A long poll lasts up to the soft
  timeout (15 s) by design, so its duration says nothing about how fast the
  service is. In `http_request_duration_seconds` it would dominate any p99
  taken across routes and fall outside the default buckets. Long polls are
  still counted, with their status codes, in `http_requests_total`.
- **When the database cannot be read, the gauges disappear.** The queue
  collector then increments `metrics_collect_errors_total` and emits no
  `tasks_*` or age gauges for that scrape. Keeping the previous values would
  be worse: a frozen `tasks_dead` of 0 looks healthy while the database is
  unreachable. Missing series and the error counter both alert.
- **Database gauges are the same on every instance.** They describe the
  whole database, so aggregate them with `max()`, never `sum()`.
- **Series that alerts depend on start at zero.** `increase()` and `rate()`
  ignore the first sample of a new series, so a counter that first appears
  with the value 1 never triggers its alert. Gateway outcomes, task kinds
  and outcomes, and reconcile outcomes are created at zero when the process
  starts.
- **A postponed poll counts as `ok`.** The handler ran and the task simply
  runs again later. Postponements are not failures and spend no attempt.

## Alerts

The rules live in `deploy/prometheus/alerts.yml` and are unit-tested in
`deploy/prometheus/alerts_test.yml` (`make check-alerts`, also run in CI).
Thresholds are starting points for this reference service; tune them from
real traffic.

| Alert | PromQL | For | Runbook |
| --- | --- | --- | --- |
| `TaskBacklogStale` | `max(task_oldest_ready_seconds) > 120` | 5m | Check that workers run and claim (`tasks_claimed_total`) and that the pool has free connections. |
| `DeadTasks` | `max by (kind) (tasks_dead) > 0` | 1m | Read `last_error` of the dead tasks. A dead `check_duplicates` task means a double charge (see `GatewayDuplicateCharge`); a dead `reconcile_payment` or `start_payment` task after the attempt cap means the payment stays `pending` and must be settled with the gateway by hand. Otherwise fix the cause and requeue. |
| `OutboxLag` | `max(outbox_oldest_unpublished_seconds) > 60` | 5m | Check that the relay runs and `outbox_failed_total`; a failing subscriber blocks the outbox. |
| `GatewayAmbiguousRatio` | `sum by (op) (rate(gateway_requests_total{outcome="ambiguous"}[10m])) / sum by (op) (rate(gateway_requests_total[10m])) > 0.05` | 10m | Check gateway latency and 5xx; payments stay pending and are retried with the same key. |
| `GatewayAttemptStuck` | `max(gateway_attempt_oldest_open_seconds) > 300` | 5m | Check `reconcile_total{outcome="error"}` and the gateway lookup. If the attempt's `reconcile_payment` task is dead, fix the cause and requeue that task (`UPDATE tasks SET state = 'ready', attempts = 0, run_after = now() WHERE id = ...`); re-running `start_payment` does not revive it, because its dedup key already exists. |
| `GatewayDuplicateCharge` | `sum(increase(gateway_duplicates_detected_total[15m])) > 0` | none | Find "gateway holds several payments" in the logs for the payment id and the gateway references, refund the extra gateway payment with the gateway's tools, then close the dead task. |
| `GatewayMisconfigured` | `sum(increase(gateway_requests_total{outcome="misconfigured"}[5m])) > 0` | none | Check `PAYMENTS_GATEWAY_URL` and the gateway credentials; affected payments wait in `pending`. |
| `HTTP5xxRatio` | `sum(rate(http_requests_total{code=~"5.."}[5m])) / sum(rate(http_requests_total[5m])) > 0.01` | 10m | Find "request failed" errors in the logs by route; then check the database. |
| `HTTPLatencyP99` | `histogram_quantile(0.99, sum by (le, route) (rate(http_request_duration_seconds_bucket[5m]))) > 1` | 10m | Long polls are excluded; check pool acquire wait and slow queries. |
| `DBPoolAcquireWait` | `sum(rate(db_pool_acquire_wait_seconds_total[5m])) / sum(rate(db_pool_acquires_total[5m])) > 0.05` | 10m | The pool is exhausted: check `PAYMENTS_START_MODE` (the `in_tx` start holds a connection during every gateway call), slow queries and `PAYMENTS_DB_MAX_CONNS`. |
| `MetricsCollectErrors` | `increase(metrics_collect_errors_total[10m]) > 0` | 10m | The queue gauges are missing, so queue alerts are blind; check the database. |

There is no Alertmanager in the local stack: firing alerts are visible in
the Prometheus UI. A deployment routes `page` alerts to on-call and
`ticket` alerts to a queue.

## Cardinality rules

1. **No identifiers in labels.** Payment ids, task ids, gateway references,
   idempotency keys, request ids and merchant ids go to logs, never to
   labels. The end-to-end script fails if a UUID appears in `/metrics`.
2. **Every label has a small, fixed value set**, written down above:
   - `route` is the ServeMux pattern, never the raw path, and all requests
     that matched nothing share `unmatched`;
   - `method` keeps the standard methods and folds anything else into
     `OTHER`, because clients can invent methods;
   - `code` is the status code;
   - `kind` comes from `ports.TaskKinds`, and the attempt `state` from the
     open states in `ports.AttemptStates`, which the collector also uses to
     export missing states at 0;
   - `outcome`, `op`, `from`, `to` and `status` come from constants in the
     code.
3. **Histograms multiply.** Each histogram series has 8 to 18 buckets, so
   histograms carry at most two labels.
4. **Budget per instance.** With the current routes and value sets an
   instance exports a few hundred series, most of them histogram buckets and
   the Go runtime. A change that adds a label must say how many values it
   can take.
5. **Adding a label value is a code change.** A new task kind, gateway
   outcome or route appears through constants and the ServeMux, never
   through data.
