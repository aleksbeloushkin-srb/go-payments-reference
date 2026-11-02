# Production Go service checklist

A checklist for reviewing a Go service that moves money, or anything else
that must not lose or duplicate work. Each item is marked against this
repository: `[x]` done, `[ ]` not done (with the reason or the plan), `[~]`
partly. The open items are deliberate and listed in REVIEW.md or
IMPROVEMENTS.md.

## Errors

- [x] Errors are wrapped with `%w` and checked with `errors.Is` / `errors.As`, never by comparing strings.
- [x] Domain errors are mapped to HTTP status codes in exactly one function (`httpapi.writeError`).
- [x] Every expected error has a mapping, so "not found" or "conflict" never turns into a 500.
- [x] Clients get a stable machine-readable code and a safe message; internal details are logged, not returned.
- [x] An error is either returned or logged, not both at every layer: unexpected errors are logged once, in `writeError` or in the worker's `settle`.
- [x] `recover` sits in the middleware chain and writes a 500 body; `http.ErrAbortHandler` is re-panicked.
- [x] Background goroutines recover too and turn a panic into an error (`queue.Worker.handle`, `platform.Group.Go`).

## Configuration

- [x] Parsing is strict: an unknown `PAYMENTS_` variable or an unparsable value stops the start.
- [x] Defaults live in one place, each with its reason (`platform.LoadConfig`).
- [x] A zero duration is rejected: it would mean "already expired" or "no limit".
- [x] Relations between settings are validated (write timeout vs soft timeout, task timeout vs lease, pool size vs worker concurrency, gateway budget vs task timeout).
- [x] Configuration is read in `main` and passed down explicitly: no `init()`, no globals, no `ctx.Value`. Metrics are registered on a registry created in `main`, not on the global default.
- [ ] Hot reload. Not needed here; when added, swap an immutable snapshot (`atomic.Pointer`) and use one snapshot per request or task.

## Lifecycle and shutdown

- [x] Signals are caught before anything else starts; the second signal kills the process.
- [x] Order: readiness off, pause, HTTP drain, background components, pool, logs.
- [x] `http.Server.Shutdown` gets a fresh deadline, not the cancelled root context.
- [x] Long polls are released when shutdown starts instead of holding the drain for the soft timeout.
- [x] Every goroutine has an owner that waits for it, and every wait can be cancelled.
- [x] `WaitGroup.Add` never races with `Wait` (`fakegateway.goBackground` takes the same lock as `close`).
- [x] A failed start or stop exits with code 1.
- [x] The worker stops claiming when shutdown starts, so only running tasks have to finish.
- [~] Drain pause, shutdown timeout and the longest task must fit the orchestrator's grace period: about 35 s with the defaults. Compose sets `stop_grace_period: 60s`; a deployment must set its own, at least 40 s.

## HTTP server

- [x] `ReadHeaderTimeout`, `ReadTimeout`, `WriteTimeout` and `IdleTimeout` are all set; `WriteTimeout` exceeds the long-poll soft timeout.
- [x] Request bodies are size-limited with `http.MaxBytesReader` before decoding.
- [x] Our own API rejects unknown JSON fields and trailing data; third-party webhooks are decoded leniently, so a new field from the gateway does not stop them.
- [x] Readiness reflects shutdown; liveness checks only the process.
- [x] Admin endpoints on a separate port: `/metrics`, `/healthz` and `/readyz` on `PAYMENTS_ADMIN_ADDR`, in every role, stopped last.
- [ ] Authentication and authorization of API callers. Out of scope for this reference (REVIEW.md, W10).
- [~] Request ids: generated with `crypto/rand`; a well-formed incoming id is kept, which is only safe behind your own edge proxy.

## Outgoing calls

- [x] Every call has a per-call deadline derived from the caller's context; `http.Client.Timeout` is only a backstop.
- [x] One tuned `http.Client` per upstream (`MaxIdleConnsPerHost` raised from the default of 2); response bodies are drained and closed.
- [x] Only requests that were definitely not executed are retried in-line (429, dial errors). Ambiguous outcomes are not retried blindly.
- [x] `Retry-After` is honoured, capped, and never waited for past the caller's deadline; a longer one is passed to the task queue as the minimum retry delay.
- [x] Retries live on one layer per concern: the client retries "not executed", the task queue retries everything else with backoff.
- [x] Errors are classified: unavailable (retry), declined (do not retry; only explicit decline codes), ambiguous (retry only with the same idempotency key, including "same key in progress"), misconfigured (retry and alert, never fail the payment), not ready yet (postpone without spending an attempt).
- [ ] Circuit breaker or bulkhead per upstream. Planned with the version 2 start flow (IMPROVEMENTS.md).

## Idempotency

- [x] Creating a payment requires an `Idempotency-Key`; a replay returns the same payment, a different body with the same key gets 422.
- [x] A unique index and `INSERT ... ON CONFLICT DO NOTHING` enforce it; the conflict never aborts the surrounding transaction.
- [x] The key sent to the gateway is deterministic (the payment id), stable across retries.
- [x] Webhooks are deduplicated by the gateway's event id in the same insert that stores them.
- [x] The state machine refuses to leave a final status, so a late event cannot undo a result.
- [ ] Keys are scoped per client and expire. Today they are global and kept forever (REVIEW.md, W17).

## Database and transactions

- [ ] No network calls between `BEGIN` and `COMMIT`. Violated on purpose by `StartPayment` in version 1 (REVIEW.md, W1). The outbox relay claims with a lease and publishes outside any transaction.
- [x] The `Commit` error is checked; a failed commit is treated as ambiguous in the documentation of `pkg/txctx`.
- [x] The pool is bounded and larger than the concurrency that can hold connections; validated at start.
- [x] Server-side limits are set: `statement_timeout`, `lock_timeout`, `idle_in_transaction_session_timeout`.
- [x] Row locks are only taken inside a transaction (`Payments.Lock` refuses otherwise).
- [x] Lost updates are impossible: the row lock gives exclusivity, and `UPDATE ... WHERE version = $expected` turns a missing lock into an error.

## Queues and workers

- [x] A task is marked done only after the handler succeeded; failures are retried with exponential backoff and jitter.
- [x] A dead state with a log line at error level, the `tasks_dead` gauge and the `DeadTasks` alert.
- [x] The lease is longer than the task timeout (validated), and settling is fenced by the lease token.
- [x] Concurrency is bounded and below the database pool size.
- [x] On shutdown, running tasks finish instead of being cancelled mid-call.
- [~] Handlers tolerate redelivery: they re-check the status under a row lock. A crash after the handler but before settling runs it again, which is safe for every handler here.
- [x] Every payment that is not final has a poll task, whichever path made it non-final, so a lost webhook cannot leave it stuck.
- [ ] Attempts counted at claim time. Today a process crash in the middle of a task does not count as an attempt (REVIEW.md, W2).
- [x] Alerts on dead tasks and on the age of the oldest due task (`DeadTasks`, `TaskBacklogStale`), with promtool tests. Routing them to people is the deployment's job.

## Outbox

- [x] Events are written in the same transaction as the change they describe.
- [x] The relay is at least once; consumers deduplicate by event id and order by version.
- [x] A publish failure stops the batch, so later events of a payment do not overtake earlier ones.
- [ ] Published rows are cleaned up. They are kept today (REVIEW.md, W8).

## Observability

- [x] Structured JSON logs with a request id on every line written in a request.
- [x] Logged entities use an allowlist of fields (`Payment.LogValue`); idempotency keys and request hashes stay out of logs.
- [x] Access logs record the route pattern, never the query string or the body.
- [x] RED metrics per route pattern, with long polls in their own histogram; queue, outbox, gateway, long-poll and pool metrics; business metrics counted only after commit (OBSERVABILITY.md).
- [x] Labels have small fixed value sets: route patterns, folded methods, task kinds, outcomes. No ids; the e2e script fails if one appears.
- [x] Counters that alerts depend on start at zero, so `increase()` sees the first event.
- [x] A failing metrics query increments a counter and drops the gauges instead of reporting stale values.
- [ ] Tracing with context propagated through tasks and events (IMPROVEMENTS.md).

## Security

- [x] Webhooks are authenticated before the body is parsed: HMAC-SHA256 over a timestamp and the body, compared in constant time, with a replay window.
- [x] Secrets come from the environment and are validated for minimum length; none is logged.
- [x] The container runs as non-root on a distroless base image.
- [ ] TLS termination, API authentication, rate limiting. Expected from the platform in front of the service; not part of this repository.

## Testing

- [x] Unit tests use in-memory fakes of the ports; the fakes implement real rollback semantics.
- [x] Transactions, locks, unique keys and the queue are tested on a real PostgreSQL (testcontainers, `-tags integration`).
- [x] Concurrency is tested for real: 16 concurrent creates with one key, 8 workers draining 200 tasks.
- [x] Failure scenarios: gateway timeout after execution, 429, 500, connection refused, dropped and duplicated webhooks, lease expiry, dead after max attempts, shutdown with a task running.
- [x] Invariant tests: every task kind has a handler; every status in the transition table is valid.
- [x] No `time.Sleep` for synchronization in tests; clocks and waits are injected.
- [x] Everything runs with `-race` on the oldest and newest supported Go versions.
- [x] An end-to-end script drives the real stack with curl.

## Delivery

- [x] `gofmt`, `go vet` and `staticcheck -checks all` are clean and run in CI.
- [x] CI runs unit and integration tests on Go 1.24 and the latest stable release, staticcheck, and the end-to-end script, which builds the image.
- [ ] `govulncheck` in CI and on every dependency bump.
- [x] Dependencies are few and each one is justified in the README.
