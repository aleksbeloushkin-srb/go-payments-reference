# Review of version 1

An honest review of this repository as it stands: what it does well, where
it is weak, and how much each weakness matters. Every finding points to a
file and line in this repository. Numbers in the impact chains are the
configuration defaults from `internal/platform/config.go`.

This review describes v2. The `v1` tag shows the repository before the
two-phase start, with the gateway call inside the database transaction (W1);
in v2 that design is resolved and remains selectable only for comparison
(`PAYMENTS_START_MODE=in_tx`).

It also includes the findings of an independent code
review. Those that were fixed are listed under "Fixed in the review pass"
with the change that fixed them; the rest are open design trade-offs or
known limitations, each with its reason.

Severity: **High** means the design breaks under ordinary production
failures (a slow gateway); **Medium** means it costs throughput, money or
operator time under load; **Low** means a sharp edge with limited impact.
Fixes refer to [IMPROVEMENTS.md](IMPROVEMENTS.md).

## Strengths

| # | Strength | Where | Why it matters |
| --- | --- | --- | --- |
| S1 | Idempotent create: unique key, request hash, `ON CONFLICT DO NOTHING`, 200 for a replay and 422 for a different body | [create.go:58](../internal/app/create.go#L58), [create.go:64](../internal/app/create.go#L64), [payments.go:67](../internal/adapters/out/postgres/payments.go#L67) | a client that retries after a timeout never creates a second payment; the conflict never aborts the surrounding transaction; tested with 16 concurrent requests ([create_test.go:45](../test/integration/create_test.go#L45)) |
| S2 | Payment, start task and `payment.created` event commit together | [create.go:58](../internal/app/create.go#L58) | no payment that nobody starts, no event about a payment that does not exist |
| S3 | Explicit state machine with a transition table and table tests; "same status" is a no-op | [status.go:28](../internal/domain/payment/status.go#L28), [payment.go:67](../internal/domain/payment/payment.go#L67) | duplicate events are harmless and a final status can never be left |
| S4 | Rejected transitions are classified: stale reports are ignored, conflicting final statuses and mismatched gateway references go to the dead state | [status.go:70](../internal/app/status.go#L70) | out-of-order webhooks do not create noise, and a real disagreement with the gateway reaches an operator |
| S5 | Optimistic version check on every update, in addition to the row lock | [payments.go:122](../internal/adapters/out/postgres/payments.go#L122) | a code path that forgets the lock fails loudly instead of losing an update |
| S6 | Row locks only inside transactions, which run at READ COMMITTED explicitly | [payments.go:97](../internal/adapters/out/postgres/payments.go#L97), [txctx.go:88](../pkg/txctx/txctx.go#L88) | `SELECT ... FOR UPDATE` outside a transaction would silently lock nothing; the idempotent insert does not depend on a server default |
| S7 | Task queue: `FOR UPDATE SKIP LOCKED` for one statement, then a lease; the lease value is a fencing token for every settle statement | [tasks.go:81](../internal/adapters/out/postgres/tasks.go#L81), [tasks.go:122](../internal/adapters/out/postgres/tasks.go#L122) | no transaction is held while a handler runs, a dead worker's tasks come back, a late worker cannot overwrite the new owner; tested with 8 workers and 200 tasks ([queue_test.go:49](../test/integration/queue_test.go#L49)) and with lease expiry ([queue_test.go:98](../test/integration/queue_test.go#L98)) |
| S8 | One error contract for handlers: retry with backoff (honouring a downstream `Retry-After`), postpone, or give up | [worker.go:192](../internal/adapters/in/queue/worker.go#L192), [worker.go:195](../internal/adapters/in/queue/worker.go#L195) | handlers stay simple; polling does not burn attempts; poison tasks end in a visible dead state |
| S9 | Exponential backoff with equal jitter, capped | [backoff.go:20](../internal/adapters/in/queue/backoff.go#L20) | tasks that failed together during an outage do not come back as one wave |
| S10 | Polling is a delayed retry of the same task row, and the poll's gateway call is outside any transaction | [poll.go:22](../internal/app/poll.go#L22), [poll.go:53](../internal/app/poll.go#L53) | no sleeping goroutines, nothing lost on restart, no lock held during the call |
| S11 | Every payment that is not final has a poll task, whichever path made it non-final | [status.go:119](../internal/app/status.go#L119) | a lost webhook can delay a final status, never hide it |
| S12 | Gateway client: in-line retries only for requests that were not executed; only explicit decline codes fail a payment; per-call deadline from the caller's context | [client.go:239](../internal/adapters/out/gateway/client.go#L239), [client.go:294](../internal/adapters/out/gateway/client.go#L294), [client.go:256](../internal/adapters/out/gateway/client.go#L256) | an ambiguous outcome is never retried blindly, and a configuration mistake never turns into failed payments; the policy is tested by counting requests |
| S13 | Webhooks authenticated before parsing (HMAC over timestamp and body, constant-time compare, replay window) and deduplicated by event id | [events.go:45](../internal/adapters/in/httpapi/events.go#L45), [hmacsig.go:61](../pkg/hmacsig/hmacsig.go#L61), [event.go:44](../internal/app/event.go#L44) | forged or replayed events are rejected; redeliveries are applied once |
| S14 | Long polling: cap checked before any database read, one query per tick for all waiters, version markers, bounded reads, waiters released at shutdown | [query.go:36](../internal/app/query.go#L36), [batcher.go:58](../internal/longpoll/batcher.go#L58), [registry.go:100](../internal/longpoll/registry.go#L100), [main.go:220](../cmd/payments/main.go#L220) | database cost does not grow with the number of waiting clients; a full instance answers 429 cheaply; the drain does not wait for the soft timeout |
| S15 | Outbox relay claims with a lease and publishes with no transaction open | [outbox.go:61](../internal/adapters/out/postgres/outbox.go#L61), [relay.go:149](../internal/adapters/in/outbox/relay.go#L149) | a slow subscriber holds no connection; two relays never publish one event at the same time; tested in [outbox_test.go:111](../test/integration/outbox_test.go#L111) |
| S16 | Errors mapped to HTTP in one place; middleware order request id, logging, recover, tested | [errors.go:29](../internal/adapters/in/httpapi/errors.go#L29), [routes.go:75](../internal/adapters/in/httpapi/routes.go#L75), [httpapi_test.go:119](../internal/adapters/in/httpapi/httpapi_test.go#L119) | "not found" is never a 500; internals never leak; a panic is a logged 500 with the request id |
| S17 | Strict configuration with cross-checks between settings | [config.go:201](../internal/platform/config.go#L201), [config.go:379](../internal/platform/config.go#L379) | a typo, or a combination that would cut long polls short, double-claim tasks or starve the pool, does not start |
| S18 | Orderly shutdown with owned goroutines; the worker stops claiming at once and running tasks finish instead of being cut mid-call | [main.go:249](../cmd/payments/main.go#L249), [main.go:320](../cmd/payments/main.go#L320), [worker.go:90](../internal/adapters/in/queue/worker.go#L90), [lifecycle.go:53](../internal/platform/lifecycle.go#L53) | deploys do not drop requests or create ambiguous gateway outcomes |
| S19 | Database-side limits on every session | [db.go:42](../internal/adapters/out/postgres/db.go#L42) | a stuck statement or an abandoned transaction cannot hold locks forever |
| S20 | Logs use an allowlist of fields for payments | [payment.go:85](../internal/domain/payment/payment.go#L85) | idempotency keys and request hashes stay out of logs by construction |
| S22 | Two-phase start: the attempt and its idempotency key are committed before the gateway call, the call runs with no transaction, connection or row lock, and an ambiguous answer goes to reconciliation by lookup | [start_two_phase.go:34](../internal/app/start_two_phase.go#L34), [start_two_phase.go:137](../internal/app/start_two_phase.go#L137), [reconcile.go:41](../internal/app/reconcile.go#L41) | after any crash the database says which call may have happened; tested per crash point ([start_two_phase_test.go:116](../internal/app/start_two_phase_test.go#L116)) and with a connection probe during the call ([two_phase_test.go:114](../test/integration/two_phase_test.go#L114)) |
| S21 | Prometheus metrics with low-cardinality labels on an injected registry; business metrics counted only after commit; alert rules unit-tested with promtool | [main.go:109](../cmd/payments/main.go#L109), [status.go:59](../internal/app/status.go#L59), [metrics.go:79](../internal/adapters/in/httpapi/metrics.go#L79), [collector.go:109](../internal/adapters/out/postgres/collector.go#L109) | every alert in [OBSERVABILITY.md](OBSERVABILITY.md) is backed by a metric with a bounded set of series, and rolled-back work never shows up in the numbers |

## Open weaknesses at a glance

| # | Severity | Weakness | Where | Fix |
| --- | --- | --- | --- | --- |
| W2 | Medium | Attempts are counted only when a handler returns; a crashing task loops forever | [worker.go:195](../internal/adapters/in/queue/worker.go#L195) | item 4 |
| W3 | Medium | The worker waits for the whole batch before claiming again | [worker.go:146](../internal/adapters/in/queue/worker.go#L146) | item 4 |
| W4 | Low | Fixed lease with no heartbeat | [tasks.go:93](../internal/adapters/out/postgres/tasks.go#L93) | item 4 |
| W5 | Medium | Long-poll costs: a read per woken waiter, ticks without changes, synchronized reconnects | [query.go:66](../internal/app/query.go#L66), [batcher.go:66](../internal/longpoll/batcher.go#L66) | items 2, 3 |
| W6 | Low | Leases use the instance clock | [tasks.go:86](../internal/adapters/out/postgres/tasks.go#L86) | accept, or use `now()` with a time-travel helper in tests |
| W7 | Low | One failing outbox event stops the events of every other payment behind it | [relay.go:153](../internal/adapters/in/outbox/relay.go#L153) | item 6 |
| W8 | Low, grows | `done` tasks and published events are never deleted | [00001_init.sql:33](../migrations/00001_init.sql#L33), [00001_init.sql:50](../migrations/00001_init.sql#L50) | item 5 |
| W10 | Out of scope (High in production) | No authentication or object-level authorization | [routes.go:69](../internal/adapters/in/httpapi/routes.go#L69) | item 10 |
| W11 | Low | One webhook secret, no rotation | [events.go:45](../internal/adapters/in/httpapi/events.go#L45) | accept two secrets during rotation |
| W12 | Low | Non-final statuses can flap on out-of-order events | [status.go:30](../internal/domain/payment/status.go#L30) | item 7 |
| W13 | Low | A well-formed incoming `X-Request-ID` is kept | [middleware.go:34](../internal/adapters/in/httpapi/middleware.go#L34) | overwrite at the edge |
| W14 | Low | Commit-hook wake-ups are local to the process | [tasks.go:41](../internal/adapters/out/postgres/tasks.go#L41) | item 2 (NOTIFY) |
| W15 | Low | No tracing, no alert routing, no dashboards: metrics and alert rules exist, the rest of observability does not | [main.go:109](../cmd/payments/main.go#L109) | item 8 |
| W16 | Low | Readiness ignores the database | [admin.go:26](../internal/adapters/in/httpapi/admin.go#L26) | deliberate, see below |
| W17 | Low | Idempotency keys are global, never expire, and a replay returns the current state rather than the original response | [00001_init.sql:17](../migrations/00001_init.sql#L17), [create.go:67](../internal/app/create.go#L67) | item 10 |
| W18 | Low | A stub gateway is needed for processes without the worker role | [main.go:356](../cmd/payments/main.go#L356) | separate `app.Service` by role |
| W19 | Low | Several relays lose the global order of events | [outbox.go:69](../internal/adapters/out/postgres/outbox.go#L69) | one relay, or relays per payment range |
| W20 | Low | Gateway statuses are validated, not translated | [client.go:377](../internal/adapters/out/gateway/client.go#L377) | a mapping table per real gateway (EXTENDING.md) |
| W40 | Medium | Abandoning an attempt after a few certain "not found" lookups still charges the client twice if the gateway shows a payment later than that; it is now detected, not prevented | [reconcile.go:98](../internal/app/reconcile.go#L98), [reconcile.go:147](../internal/app/reconcile.go#L147) | set the reconcile window from the gateway's documented consistency; see below |
| W41 | Low | The worker slot is still held for the whole gateway call | [start_two_phase.go:40](../internal/app/start_two_phase.go#L40) | item 4 (continuous claiming), item 9 (bulkhead) |
| W42 | Low | Two start implementations to maintain while `in_tx` stays selectable (switching between them is safe since W44) | [start.go:22](../internal/app/start.go#L22) | remove `in_tx` after one release |
| W43 | Low | Attempts are visible only through SQL and metrics; there is no operator view | [00004_gateway_attempts.sql:7](../migrations/00004_gateway_attempts.sql#L7) | an admin endpoint listing open attempts |

## Fixed in the review pass and in v2

| # | Severity | Finding | What changed | Where |
| --- | --- | --- | --- | --- |
| W1 | High (deliberate in v1) | The gateway call ran inside the database transaction, under the payment's row lock (see the details below) | **Resolved in v2**: the two-phase start of [proposal 0001](proposals/0001-gateway-call-outside-transaction.md), the default; `PAYMENTS_START_MODE=in_tx` keeps v1 for comparison, and the `v1` tag shows it | [start_two_phase.go:34](../internal/app/start_two_phase.go#L34), [00004_gateway_attempts.sql:7](../migrations/00004_gateway_attempts.sql#L7) |
| W21 | High | Every 4xx on create was treated as a decline, so a 409 "same key in progress" or a 401 after a credential rotation failed a payment the gateway may still execute: the client sees "failed" and is charged | only 400, 402 and 422 decline; 409 and 425 are ambiguous and retried with the same key; 401, 403, 404, 405 and unknown 4xx are `ErrGatewayMisconfigured`: retried, logged at error level, counted in `gateway_requests_total{outcome="misconfigured"}` and alerted on | [client.go:294](../internal/adapters/out/gateway/client.go#L294), [start.go:94](../internal/app/start.go#L94), test [client_test.go:266](../internal/adapters/out/gateway/client_test.go#L266) |
| W22 | High | A non-final webhook applied between an ambiguous start and its retry left the payment `processing` with no poll task; a lost final webhook then left it there forever | invariant "non-final means a poll task exists", enforced on every non-final transition and on a start retry, idempotent through the dedup key | [status.go:119](../internal/app/status.go#L119), [start.go:75](../internal/app/start.go#L75), test [start_test.go:116](../internal/app/start_test.go#L116) |
| W23 | High | `build.network: host` in the compose file breaks the quickstart and CI on Compose versions that build through buildx bake | removed; a local, git-ignored `compose.override.yaml` is documented under Troubleshooting in the README | [docker-compose.yml:5](../docker-compose.yml#L5) |
| W24 | Medium | The relay published inside an open transaction; a slow subscriber ran into the idle-in-transaction limit and the batch was published again on every tick | claim with a lease (`claimed_until`), publish with no transaction, mark and release; a delivery starts only if it fits in the lease | [relay.go:134](../internal/adapters/in/outbox/relay.go#L134), [00002_outbox_claim.sql:6](../migrations/00002_outbox_claim.sql#L6) |
| W25 | Medium | The relay's SQL was never run by a Go test | integration tests for claim, lease expiry, mark, release, a failed delivery and two concurrent relays | [outbox_test.go:41](../test/integration/outbox_test.go#L41), [outbox_test.go:130](../test/integration/outbox_test.go#L130) |
| W26 | Medium | The web UI created a new Idempotency-Key per click, so retrying after a lost response could create a second payment | the key belongs to the form values; retries reuse it until a definite answer | [app.js:75](../web/app.js#L75), [app.js:52](../web/app.js#L52) |
| W27 | Low | A 429 with a long `Retry-After` was retried on the worker's own, shorter backoff | the client passes the delay on as `ports.RetryAfterError`; the worker waits at least that long | [worker.go:209](../internal/adapters/in/queue/worker.go#L209) |
| W28 | Low | Transactions used the server's default isolation, while the idempotent insert needs READ COMMITTED | set explicitly | [txctx.go:88](../pkg/txctx/txctx.go#L88) |
| W29 | Low | Webhooks were decoded strictly; a new field from the gateway would have turned every event into a 400 | lenient decoding for third-party payloads, strict for our own API | [events.go:54](../internal/adapters/in/httpapi/events.go#L54) |
| W30 | Low | A different gateway reference in an event overwrote the stored one silently | logged at error level and the task goes dead | [status.go:86](../internal/app/status.go#L86) |
| W31 | Low | The worker kept claiming during the drain, so shutdown could need about 58 s | it stops claiming when shutdown starts; about 35 s are needed | [main.go:253](../cmd/payments/main.go#L253) |
| W32 | Low | An abandoned long poll was logged as 200 | logged as 499 | [errors.go:54](../internal/adapters/in/httpapi/errors.go#L54) |
| W33 | Low | The write-timeout margin did not cover two slow reads around a long poll | each read is bounded by `PAYMENTS_WAIT_READ_TIMEOUT`, and the configuration checks the sum | [query.go:73](../internal/app/query.go#L73), [config.go:242](../internal/platform/config.go#L242) |
| W34 | Low | Test quality: `t.Fatal` from a goroutine could hang a test; two wake-path tests did not always exercise the wake | errors travel over a channel; the tests wait for the first claim or read before waking | [longpoll_test.go:56](../test/integration/longpoll_test.go#L56), [worker_test.go:307](../internal/adapters/in/queue/worker_test.go#L307), [query_test.go:46](../internal/app/query_test.go#L46) |
| W35 | Low | Documentation: CONTEXT.md claimed a context is never stored in a struct; the fx sketch used `fx.As` with a pointer to a struct; this review missed W21, W22 and W24 | CONTEXT.md names the exception (owners of background work); the DI snippets are labelled as sketches and use `fx.As(fx.Self())`; this table | [CONTEXT.md](CONTEXT.md), [IMPROVEMENTS.md](IMPROVEMENTS.md) |
| W36 | Low | CI ran integration tests on one Go version only, used older action versions, had no timeouts, and built an image nobody used | integration matrix on 1.24 and stable, `checkout@v5`, `setup-go@v6`, timeouts, the e2e job builds the image | [ci.yml:35](../.github/workflows/ci.yml#L35) |
| W38 | Low | Operator endpoints (`/debug/vars`, probes) were served on the public API port | a separate admin listener, `PAYMENTS_ADMIN_ADDR`, serves `/metrics`, `/healthz` and `/readyz` in every role and stops last | [admin.go:16](../internal/adapters/in/httpapi/admin.go#L16), [main.go:268](../cmd/payments/main.go#L268) |
| W39 | Medium | Observability was package-level `expvar` counters: no latency, no queue lag, no pool waits, no alerts, and globals that tests could not isolate | Prometheus metrics registered on a registry injected from the composition root, database and pool collectors, and alert rules with promtool tests; see [OBSERVABILITY.md](OBSERVABILITY.md) | [collector.go:27](../internal/adapters/out/postgres/collector.go#L27), [metrics.go:20](../internal/adapters/in/queue/metrics.go#L20) |
| W44 | High | Switching the start mode on live data charged twice: `in_tx` sent the payment id, `two_phase` sent `<id>:<n>`, and neither looked at the other's state, so a v1 call that executed without committing was retried under a new key after the upgrade | attempt 1 sends the payment id, the v1 key, and `<id>:<n>` is used from attempt 2; `in_tx` hands any payment with attempts to the two-phase start, decided under the row lock | [attempt.go:59](../internal/ports/attempt.go#L59), [start.go:69](../internal/app/start.go#L69), tests [start_safety_test.go:22](../internal/app/start_safety_test.go#L22), [start_safety_test.go:47](../internal/app/start_safety_test.go#L47) |
| W45 | High | A crash after the gateway executed, followed by the gateway's own event, left the attempt `intent` forever: `GatewayAttemptStuck` paged without end and hid real stuck attempts | applying a gateway event, and a start retry, close the open attempt of a payment that has left `pending` with a gateway reference (without a reference, it goes to reconciliation) | [start_two_phase.go:194](../internal/app/start_two_phase.go#L194), tests [start_safety_test.go:71](../internal/app/start_safety_test.go#L71), [reconcile_test.go:128](../test/integration/reconcile_test.go#L128) |
| W46 | High | After an abandoned attempt, a late duplicate was never looked up again, while W40 claimed it was | a `check_duplicates` task after any attempt that follows an abandoned one, `gateway_duplicates_detected_total` and the `GatewayDuplicateCharge` alert; W40 corrected | [start_two_phase.go:174](../internal/app/start_two_phase.go#L174), [reconcile.go:171](../internal/app/reconcile.go#L171), test [start_safety_test.go:150](../internal/app/start_safety_test.go#L150) |
| W47 | Medium | The abandon chain had no bound, and the stuck-attempt alert could not see it | `PAYMENTS_MAX_GATEWAY_ATTEMPTS` (3); past it the task goes dead and `DeadTasks` pages | [start_two_phase.go:119](../internal/app/start_two_phase.go#L119), test [start_safety_test.go:250](../internal/app/start_safety_test.go#L250) |
| W48 | Medium | 409/425 "this key is in progress" led to an abandon and a new key | the attempt keeps its key and the task retries; the e2e asserts a single attempt | [start_two_phase.go:54](../internal/app/start_two_phase.go#L54), test [start_safety_test.go:295](../internal/app/start_safety_test.go#L295) |
| W49 | Medium | The documented precondition said the gateway needs deduplication *or* lookup | both are required, and the lookup must show new payments promptly for abandoning to be safe | [EXTENDING.md](EXTENDING.md), [proposal 0001](proposals/0001-gateway-call-outside-transaction.md) |
| W50 | Low | A definite answer for an abandoned attempt was dropped at Info level | logged at Error and followed by an immediate duplicate check | [start_two_phase.go:149](../internal/app/start_two_phase.go#L149) |
| W51 | Low | Timing relations were unchecked: the gateway budget could equal the task timeout, and the abandon window could be shorter than a call | the configuration requires a 5 s margin and an abandon window that covers the worst gateway budget; the reconcile interval default is 10 s | [config.go:249](../internal/platform/config.go#L249) |
| W52 | Low | The `GatewayAttemptStuck` runbook did not say how to settle an attempt; abandoned attempts were counted on every scrape; reconciliation had no PostgreSQL test; the proposal's schema block was stale; the "v1 to v2" numbers overstated the gain | runbook names the requeue; the gauge and its index cover open attempts only; integration tests for found, abandon and duplicates; the proposal links the migration; the README reports repeated runs | [collector.go:96](../internal/adapters/out/postgres/collector.go#L96), [reconcile_test.go:68](../test/integration/reconcile_test.go#L68) |
| W53 | Nit | Smaller items from the v2 review | separate payload errors; `found` counted only when an answer was applied; "outcome unknown" logged only when something changed; states zero-filled from `ports.AttemptStates`; `NewService` rejects an invalid configuration, including an unknown start mode | [service.go:120](../internal/app/service.go#L120), test [start_safety_test.go:340](../internal/app/start_safety_test.go#L340) |
| W37 | Nit | Naming and small simplifications | `TaskSpec` instead of `NewTask`; config structs instead of adjacent durations; error prefixes said once; a currency set; a doubling loop in backoff; a result struct instead of a `-1` sentinel; `Fanout` next to the `Publisher` interface it needs; one tuned `http.Client` constructor; the publisher test copies the request inside the handler | [task.go:27](../internal/ports/task.go#L27), [batcher.go:18](../internal/longpoll/batcher.go#L18), [backoff.go:22](../internal/adapters/in/queue/backoff.go#L22), [client.go:122](../internal/adapters/out/gateway/client.go#L122), [relay.go:62](../internal/adapters/in/outbox/relay.go#L62), [httpclient.go:13](../internal/platform/httpclient.go#L13) |

## Known limitations

These were found and deliberately left as they are.

- **The Dockerfile has no BuildKit cache mounts and pins base images by tag,
  not by digest** ([Dockerfile:10](../Dockerfile#L10)). Cache mounts would stop every
  source change from downloading modules again, but they require BuildKit,
  and the Dockerfile stays buildable by the classic builder. Digest pins
  make builds reproducible but need an update bot to stay current; add both
  together when the repository gets one.
- **Database gauges are exported by every instance.** `tasks_ready`,
  `tasks_dead` and the two age gauges describe the whole database, so each
  instance runs the same query and reports the same values
  ([collector.go:109](../internal/adapters/out/postgres/collector.go#L109)). The query
  is cheap and the alert rules aggregate with `max()`; running the collector
  in one process only would save the queries but needs an election.
- **Counters with open label values start at their first event.**
  `http_requests_total{code="503"}` appears with the first 503, and
  `increase()` ignores that first sample. The labels alerts depend on most
  (gateway outcomes, task kinds and outcomes) are created at zero on start
  ([client.go:100](../internal/adapters/out/gateway/client.go#L100)); status codes cannot all be
  pre-created, so the 5xx ratio misses the very first 5xx of an instance.
- **The `payment_time_to_final_seconds` buckets changed in v2** (from 11 to
  18, for finer measurements in the first seconds). Dashboards and rules that
  use `le` values across the upgrade see a discontinuity; the README's
  upgrade notes say so.
- **No automatic refund.** A detected double charge is counted, alerted and
  left as a dead task with the gateway references; refunding it is an
  operator action with the gateway's tools.
- **An answer for an abandoned attempt is not applied.** If the abandoned key
  executed late, the payment still waits for its next attempt, which then
  executes too; the duplicate check catches that for an operator. Applying
  the late answer could prevent the second charge but races with a next
  attempt already in flight.
- **Everything in "Open weaknesses" above** is either the deliberate
  version 1 trade-off (W1) or planned in IMPROVEMENTS.md.

## Details

### W1. Gateway call inside the transaction, under a row lock (High, deliberate in v1, resolved in v2)

**Resolved in v2** by [proposal 0001](proposals/0001-gateway-call-outside-transaction.md):
the two-phase start is the default. The analysis below describes the v1
start, which the `v1` tag shows and `PAYMENTS_START_MODE=in_tx` still runs;
the README section "v1 to v2" measures the difference.

`startInTx` (the v1 `StartPayment`) opens a transaction ([start.go:56](../internal/app/start.go#L56)),
locks the payment row ([start.go:57](../internal/app/start.go#L57)), calls the
gateway ([start.go:78](../internal/app/start.go#L78)) and commits only after
the answer is stored. Version 1 uses this design on purpose, as a teaching
example of the trade-off: one executor per payment, and the status change,
the event and the poll task commit together. Its costs show up exactly when
the gateway is slow.

**Connection held across the network.** A start task holds a pooled
connection for the whole call. With the defaults, one call can last 4 s
(`PAYMENTS_GATEWAY_TIMEOUT`), and with two in-line retries and two
`Retry-After` waits up to 22 s. The client waits for `Retry-After` while the
transaction is open ([client.go:242](../internal/adapters/out/gateway/client.go#L242)).
When the gateway degrades:

1. every running start task pins one of the 12 connections for up to 22 s;
2. with the default 4 workers, 4 connections are pinned and starts drop to
   about one per second per instance, so the start backlog grows;
3. if the worker concurrency is raised to the maximum the configuration
   allows (9), only 3 connections remain for the API, the batcher and the
   relay; creates and reads queue for a connection, latency rises and
   clients time out;
4. clients retry their creates (safe, thanks to S1), which adds load to the
   same pool.

**Row lock held across the network.** Everything else that touches the
payment waits for the lock: a webhook for the same payment, a poll. With
`PAYMENTS_DB_LOCK_TIMEOUT=5s` such a task fails after 5 s and goes back to
the queue with backoff. The end-to-end script shows this when run with
`START_MODE=in_tx`: in the "hang" scenario the webhook's apply task waits on the lock until the start
transaction rolls back after the 4 s timeout, and only then applies the
result.

**Ambiguous outcome between the gateway's success and our commit.** If the
gateway executes the payment and then the commit does not happen (the
connection drops, the process is killed, `idle_in_transaction_session_timeout`
fires, the task timeout cancels the call after the gateway acted), the
database still says `pending` and holds no trace that a call was made. The
task is retried and calls the gateway again with the same idempotency key
(the payment id). That is safe only because the gateway deduplicates by key
and returns the original result, or answers 409 while the first request is
still running (W21). With a gateway that does neither, the client is
charged twice. The configuration even has to work around this design:
`PAYMENTS_DB_IDLE_IN_TX_TIMEOUT` must be longer than a task
([config.go:253](../internal/platform/config.go#L253)), because the transaction
sits idle during the call.

**Fix, done in v2.** Record the intent in a short transaction, call the
gateway outside any transaction, apply the result in a second short
transaction, and settle ambiguous outcomes by looking the payment up instead
of creating it again.

### W2. Attempts are counted only when a handler returns (Medium)

The attempt counter grows in the settle step
([worker.go:195](../internal/adapters/in/queue/worker.go#L195),
[tasks.go:129](../internal/adapters/out/postgres/tasks.go#L129)). A handler
that never returns because the process dies (killed for memory, a fatal
runtime error such as concurrent map writes, a forced kill after the grace
period) leaves the task with its old count. The lease expires after 60 s and
another instance claims it. If the crash is deterministic, every instance
that picks the task up dies, once a minute, and the task never reaches the
dead state, so no alert fires. Handler panics are recovered
([worker.go:165](../internal/adapters/in/queue/worker.go#L165)) and do count;
only process-level crashes slip through. Fix: count attempts at claim time.

### W3. Batch barrier in the worker (Medium)

`RunOnce` claims up to 4 tasks, runs them concurrently and waits for all of
them ([worker.go:146](../internal/adapters/in/queue/worker.go#L146)) before
claiming again. One task stuck on a slow gateway for 22 s leaves three slots
idle for 22 s. Throughput falls exactly when the backlog grows. Fix: claim
into free slots continuously with a semaphore.

### W4. Fixed lease, no heartbeat (Low)

The lease is set once at claim time
([tasks.go:93](../internal/adapters/out/postgres/tasks.go#L93)). It is safe
because the configuration demands that the task timeout is at least 10 s
shorter than the lease, and the handler context enforces the task timeout.
A task type that legitimately runs long would need a longer lease for all
tasks, which delays recovery after a crash. Fix: extend the lease while the
handler runs.

### W5. Long-poll costs (Medium)

- A woken waiter reads the payment again to build its answer
  ([query.go:66](../internal/app/query.go#L66)). A change watched by N clients
  costs N reads, on the primary.
- The batcher queries every 200 ms while anyone is waiting
  ([batcher.go:66](../internal/longpoll/batcher.go#L66)), changed or not, and
  a change is noticed up to one tick late.
- After the soft timeout the answer carries the state read at registration
  ([query.go:60](../internal/app/query.go#L60)), which can be up to one tick old.
- The soft timeout has no jitter, so clients that started together reconnect
  together.
- The registry is per instance; the cap (2000) protects the instance, not
  the fleet, and a load balancer that is not least-connections can fill one
  instance while others are idle.

Fixes: an event-driven wake-up with the batcher as a fallback, answers built
from the batch query, and jitter (IMPROVEMENTS.md, items 2 and 3).

### W6. Leases use the instance clock (Low)

`Claim` compares `run_after` and `lease_until` with a time passed by the
caller ([tasks.go:86](../internal/adapters/out/postgres/tasks.go#L86)), not
with the database's `now()`. This makes lease expiry testable without
sleeping, and the integration tests rely on it. The cost: two instances whose
clocks differ by more than a small fraction of the lease can claim a task
early or late. NTP keeps this negligible; a deployment without it would
not. The outbox claim works the same way.

### W7. One failing event blocks the outbox (Low)

Since the review pass the relay claims with a lease and publishes with no
transaction open (W24), so a slow subscriber no longer pins a connection.
What remains: publishing stops at the first failure
([relay.go:153](../internal/adapters/in/outbox/relay.go#L153)) to keep each
payment's events in order, which also stops every other payment's events
behind it. One permanently broken event halts the outbox until it is fixed
or skipped by hand.

### W8. No retention (Low, grows)

Tasks end in `done` or `dead` and stay
([00001_init.sql:33](../migrations/00001_init.sql#L33)); published events keep
their rows ([00001_init.sql:50](../migrations/00001_init.sql#L50)). The partial
indexes keep the hot queries small, but the tables, their primary keys and
vacuum work grow with every payment. Deleting rows in bulk later is itself
a heavy operation; partitioning by time is the durable fix (item 5).

### W10. No authentication (out of scope)

Operator endpoints moved to the admin listener (W38). The API has no authentication and no check that the
caller may see a payment ([routes.go:69](../internal/adapters/in/httpapi/routes.go#L69)).
Payment ids are UUIDv7 with 74 random bits, so they are not guessable, but
an id that leaks (in a log, a URL, a screenshot) is enough to read the
payment. Authentication belongs in front of or inside any real deployment.

### W11 to W14. Sharp edges (Low)

- **One webhook secret** ([events.go:45](../internal/adapters/in/httpapi/events.go#L45)):
  rotating it requires a coordinated switch; accepting the old and new
  secrets for a while removes that.
- **Status flapping** ([status.go:30](../internal/domain/payment/status.go#L30)):
  `processing` and `requires_action` can follow each other in both
  directions, which is legitimate, but a delayed `processing` event can move
  a payment back from a newer `requires_action`. The client then sees the
  status go back and forth until the next report.
- **Incoming request ids** ([middleware.go:34](../internal/adapters/in/httpapi/middleware.go#L34)):
  a client can choose the id that appears in our logs. Harmless for
  security, confusing for correlation behind a public edge.
- **Local wake-ups** ([tasks.go:41](../internal/adapters/out/postgres/tasks.go#L41)):
  a task created by another instance waits up to the idle interval (1 s)
  before a worker here looks for it.

### W15. Tracing and the rest of observability (Low)

Metrics and alert rules exist since the review pass (W39,
[main.go:109](../cmd/payments/main.go#L109)). What is still missing: tracing
across the HTTP request, the tasks and the gateway calls of one payment, an
Alertmanager with routing (alerts are only visible in Prometheus), and
dashboards. IMPROVEMENTS.md, item 8, keeps tracing planned.

### W16 to W20

- **Readiness ignores the database** ([admin.go:26](../internal/adapters/in/httpapi/admin.go#L26)).
  Deliberate: if readiness failed on a database blip, every instance would
  leave the load balancer at once and clients would get connection errors
  instead of fast 5xx answers they can retry. The flag covers what readiness
  is for here: draining before shutdown.
- **Idempotency keys** ([00001_init.sql:17](../migrations/00001_init.sql#L17)) are
  unique across all clients and kept forever, so two clients that happen to
  use the same key collide (422), and the index grows. A replay returns the
  payment as it is now ([create.go:67](../internal/app/create.go#L67)), not
  the response of the first call; for this API that is usually what a client
  wants, but it is not "exactly the same response".
- **Stub gateway for non-worker roles** ([main.go:356](../cmd/payments/main.go#L356)).
  `app.Service` bundles the API use cases and the task handlers, so an
  API-only process still needs something that satisfies `ports.Gateway`.
  Separating the service by role would remove the stub.
- **Relay ordering** ([outbox.go:69](../internal/adapters/out/postgres/outbox.go#L69)).
  With several relays, each claims a different batch, and two events of one
  payment can be published in either order. Consumers can cope by comparing
  versions; downstream systems that need strict order need one relay per
  payment range.
- **Status translation** ([client.go:377](../internal/adapters/out/gateway/client.go#L377)).
  The fake gateway speaks the domain's vocabulary, so the client only
  validates statuses. A real gateway needs an explicit mapping table.

### W40 to W43. What v2 leaves open

- **Abandoning too early** ([reconcile.go:98](../internal/app/reconcile.go#L98)).
  After an ambiguous call, reconciliation looks the payment up by
  reference. Three certain "not found" answers, 10 s apart, abandon the
  attempt and start attempt `n+1` with a new key. If the gateway executed
  the first call but shows it to the lookup only later than that, the client
  is charged twice. The window is configuration
  (`PAYMENTS_RECONCILE_INTERVAL`, `PAYMENTS_RECONCILE_NOT_FOUND_LIMIT`), must
  come from the gateway's documented consistency, and is validated to cover
  at least the worst gateway budget. The double charge is detected, not
  prevented: when attempt `n+1` settles, a duplicate check runs after
  `PAYMENTS_DUPLICATE_CHECK_DELAY` ([reconcile.go:147](../internal/app/reconcile.go#L147));
  two gateway payments count `gateway_duplicates_detected_total`, page
  `GatewayDuplicateCharge` and leave a dead task for an operator, who
  refunds the extra one. A payment that shows up even later than the check
  is missed; the gateway's settlement reports are the last line.
- **The worker slot during the call** ([start_two_phase.go:40](../internal/app/start_two_phase.go#L40)).
  v2 holds no database resource during the call, but the worker's slot is
  busy until the gateway answers. Worker throughput is still
  `concurrency / latency`, and the batch barrier (W3) still makes the
  slowest call set the pace.
- **Two start implementations** ([start.go:22](../internal/app/start.go#L22)).
  `in_tx` stays selectable to compare the two; both paths are tested, and
  `in_tx` should go after one release. Switching between them on live data
  is safe since W44: attempt 1 sends the v1 key, and `in_tx` leaves payments
  that have attempts to the two-phase start.
- **No operator view of attempts.** An `unknown` attempt is visible in
  `gateway_attempts`, in the `gateway_attempts{state}` gauge and through the
  `GatewayAttemptStuck` alert, but no endpoint lists open attempts with
  their payment and key.

## Choices that look wrong but are deliberate

- **Task handlers ignore shutdown cancellation**
  ([worker.go:157](../internal/adapters/in/queue/worker.go#L157)).
  Cancelling a handler in the middle of a gateway call would create the
  ambiguous outcome the rest of the design tries to avoid. The handler is
  still bounded by the task timeout, the worker claims nothing new once
  shutdown starts, and the shutdown waits for running tasks.
- **A payment stays `pending` while its attempt is `unknown`**
  ([start_two_phase.go:216](../internal/app/start_two_phase.go#L216)). Showing
  `processing` would promise progress that may not exist; the long poll
  simply has nothing new to report until reconciliation decides.
- **A gateway configuration error never fails a payment**
  ([client.go:294](../internal/adapters/out/gateway/client.go#L294)). A
  wrong key or URL is retried until someone fixes it, and payments wait in
  `pending` meanwhile. The `GatewayMisconfigured` alert fires on the first
  such answer; without it the backlog would grow silently until tasks
  reach the dead state.
- **The webhook answers 404 for an unknown payment**
  ([event.go:34](../internal/app/event.go#L34)). A non-2xx
  answer makes the gateway redeliver a few times, which is wasted work for a
  truly unknown reference, but answering 2xx would hide a misrouted or
  forged reference.
- **`AfterCommit` runs immediately outside a transaction**
  ([txctx.go:150](../pkg/txctx/txctx.go#L150)). The write it follows has
  already been autocommitted, so running the hook is correct; refusing would
  force every caller to know whether it is inside a transaction.
- **A nested `Run` joins instead of opening a savepoint**
  ([txctx.go:84](../pkg/txctx/txctx.go#L84)). PostgreSQL aborts the whole
  transaction on an error anyway; savepoints would allow partial rollback but
  make "after commit" ambiguous for inner hooks.
- **`NewService` panics on a missing dependency or an invalid configuration**
  ([service.go:102](../internal/app/service.go#L102)). It runs once at start; a
  wiring bug must stop the process, not surface as a nil pointer on the
  first request.

## Test coverage gaps

- `cmd/payments` has no test of `run` itself; the shutdown order is covered
  piecewise (worker, group, registry, end-to-end stop) but not as a whole.
- The PostgreSQL adapters are tested only through the integration package,
  so `go test ./...` without Docker does not exercise any SQL.
- No fuzz tests for the JSON decoders and the signature parser.
- No load test; the impact chains above are reasoned from the configuration,
  not measured.
