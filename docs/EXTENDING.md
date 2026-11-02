# Extending the service

How to add the things a real payment service grows into, and what to check
each time. Each section names the files to touch.

## A new payment gateway

The application talks to "the gateway" through one port,
`ports.Gateway` (`internal/ports/ports.go`). A second gateway is a second
adapter that implements the same two methods.

1. **Create the adapter** in `internal/adapters/out/<name>/`. Keep the wire
   types private to the package and translate into `ports.GatewayPayment`.
2. **Map statuses explicitly.** The fake gateway uses the same status names
   as the domain, so `gateway.decode` only validates. A real gateway has its
   own vocabulary (`authorized`, `captured`, `declined`, `expired`...). Write
   a table from its statuses to `payment.Status`, and treat an unknown
   status as an error, never as a default.
3. **Classify every failure** into the `ports` error classes. This is the
   most important decision per gateway:

   | Gateway answer | Class | Why |
   | --- | --- | --- |
   | 2xx with a known status | success | |
   | 429, or 503 that the gateway documents as "not processed" | `ErrGatewayUnavailable`, retry after `Retry-After` (in-line if short, otherwise as a `RetryAfterError` hint for the worker) | the request was refused before execution |
   | dial error, DNS failure, 408 | `ErrGatewayUnavailable`, retry | nothing was executed |
   | an explicit decline: 400, 402, 422 with a decline body | `ErrGatewayRejected` | retrying will not help; the payment fails |
   | 409 or 425 "a request with this key is in progress" | `ErrGatewayAmbiguous`, retry later with the same key | the first request may still succeed |
   | 401, 403, 404 on a create, 405, any other 4xx | `ErrGatewayUnavailable` and `ErrGatewayMisconfigured`, retry and alert | our credentials or URL are wrong, not the payment; failing it would turn a configuration mistake into lost sales |
   | 5xx, timeout after sending, reset connection, unreadable 2xx | `ErrGatewayAmbiguous` | the payment may have gone through |

   Only an answer that clearly speaks about the payment may fail it. Read
   the gateway's documentation for its decline codes and for 5xx: some
   promise that a 5xx means "not executed", most do not. When in doubt, it
   is ambiguous.
4. **Check idempotency support.** This service retries an ambiguous start
   with the same `Idempotency-Key` (the payment id) and relies on the gateway
   to return the original result. If a gateway has no idempotency keys, a
   blind retry can charge a client twice. Then the start must look up the
   payment by our reference before creating it again (the gateway usually
   offers a "find by merchant reference" call), or move to the version 2
   design in IMPROVEMENTS.md, where an ambiguous outcome triggers a lookup
   instead of a retry.
5. **Webhooks.** Each gateway signs webhooks its own way. Verify the
   signature before reading the body as JSON, reject old timestamps, and
   deduplicate by the gateway's event id. `pkg/hmacsig` shows one scheme;
   a second gateway gets its own route (for example
   `POST /v1/gateways/<name>/events`) and its own verifier.
6. **Choose the gateway per payment** if there are several: store the choice
   on the payment before the first call, so retries and polls go to the
   same gateway even if the routing rules change in between.
7. **Metrics.** Reuse the `gateway_requests_total{op,outcome}` vocabulary
   for the new adapter, add a `gateway` label only if several gateways run
   at once (it has a handful of values), and create the series at zero so
   `GatewayMisconfigured` and `GatewayAmbiguousRatio` work from the start.
8. **Tests.** Copy `internal/adapters/out/gateway/client_test.go`: an
   `httptest.Server` per failure class, and assertions on the number of
   calls, not only on the returned error. A retry policy is defined by how
   many requests it sends.

## A new operation (for example, refunds)

1. **Domain.** Decide whether a refund is a new aggregate (`Refund` with its
   own status machine) or new statuses of `Payment`. A separate aggregate
   is usually cleaner: a payment can have several partial refunds. Add the
   status table and its tests next to `status.go`; the table-driven test that
   walks every pair of statuses is the safety net.
2. **Schema.** Add a goose migration in `migrations/` (`00002_refunds.sql`).
   Expand first (new table, nullable columns), deploy, then contract in a
   later migration. Migrations run at start under an advisory lock, so
   several instances can start at once.
3. **Use case.** `CreateRefund` in `internal/app`, idempotent the same way
   as `CreatePayment`: a unique idempotency key, a request hash, and the row,
   its first task and its event in one transaction.
4. **Task kind.** Add `TaskStartRefund` to `internal/ports/task.go`, append
   it to `ports.TaskKinds`, and handle it in `Service.HandleTask`. The test
   `TestHandleTaskKnowsEveryKind` fails if the kind has no handler. The
   queue metrics and gauges pick the new kind up from `ports.TaskKinds`.
5. **Dedup keys.** Choose them so that a retried request cannot create a
   second task: `"start"` for once-per-entity work, the gateway's event id
   for webhooks.
6. **Events.** `refund.created`, `refund.status_changed`, with the same
   fields as payment events (id, status, version, occurred_at).
7. **HTTP.** Routes in `internal/adapters/in/httpapi/routes.go`, a new
   consumer interface for the handler, and the new errors in
   `writeError`, the only place that maps errors to status codes.

## A new notification channel

Clients learn about status changes through long polling today. Other
channels plug in at two points.

- **Pushed to the client: Server-Sent Events or WebSocket.** Reuse the
  registry and batcher in `internal/longpoll`: an SSE handler registers a
  waiter, writes an event when it is woken, and registers again with the new
  version. Keep the marker (`version`) in the protocol so a reconnecting
  client resumes without gaps (`Last-Event-ID` carries it in SSE).
- **Pushed to other systems: merchant webhooks, a message broker.**
  Implement `outbox.Publisher` (`internal/adapters/in/outbox/relay.go`) and
  add it to the `outbox.Fanout` in `cmd/payments/main.go`. For a broker,
  use the payment id as the message key so one payment's events stay in
  order within a partition, and put the outbox id in a header for
  deduplication. For merchant webhooks, sign the body (as `pkg/hmacsig`
  does) and retry with backoff; slow merchants belong in their own queue so
  they do not hold up the relay for everyone.
- **Faster wake-up.** See "LISTEN/NOTIFY" in IMPROVEMENTS.md: the batcher
  stays as the safety net and a notification only makes it run earlier.

## Scaling

| Pressure | First step | Next step |
| --- | --- | --- |
| HTTP load | more instances with `PAYMENTS_ROLES=api` behind a load balancer | separate read path for `GET` (replica, cache) |
| Long-poll connections | raise `PAYMENTS_WAIT_MAX_WAITERS` per instance, add API instances | SSE with the same registry; event-driven wake-up |
| Task throughput | more `worker` instances; `PAYMENTS_WORKER_CONCURRENCY` | version 2 start flow so a worker does not hold a connection during the gateway call; partition the task table by kind or hash |
| Gateway latency | nothing in version 1 scales around it: every slow call holds a connection | version 2 start flow, then a bulkhead (separate concurrency limit) per gateway |
| Table growth | delete or archive `done` tasks and published outbox rows | time-based partitions, dropped instead of deleted |
| Event volume | relay batch size, several relays | a broker publisher; one relay per partition of payment ids to keep per-payment order |

Connection budget: with version 1, every running start task holds one
database connection for the whole gateway call. Keep
the sum of `PAYMENTS_WORKER_CONCURRENCY`, the API's peak concurrency, the
batcher and the relay below `PAYMENTS_DB_MAX_CONNS`, and the sum over all
instances below the database's `max_connections`. The configuration loader
checks the per-instance part.

Several relays process the outbox concurrently with `SKIP LOCKED`, which
breaks global ordering: two events of one payment can be published by two
relays in either order. Consumers already have what they need to cope
(the version), but if order matters to a downstream system, run one relay
or give each relay a disjoint set of payments.

Read replicas: long polling stays correct with a lagging replica because it
compares versions (see ARCHITECTURE.md). Anything that must read its own
writes (the answer right after a create, the follow-up read after a wake-up)
must go to the primary.

## Versioning

- **HTTP API.** The version is in the path (`/v1/...`). Additive changes
  (new optional fields, new endpoints) stay in `v1`; clients must ignore
  unknown fields. The request decoder rejects unknown fields, so new request
  fields have to be optional and added to the server before any client
  sends them. A breaking change gets `/v2` routes served side by side until
  `v1` clients are gone. Describe the API in OpenAPI and check in CI that the
  description and the handlers agree.
- **Events.** The event `type` names the schema (`payment.status_changed`).
  Add fields, never rename or remove them; for a breaking change publish a
  new type (`payment.status_changed.v2`) alongside the old one for a
  migration period. Consumers deduplicate by event id and order by version.
- **Database.** Expand and contract: add the new column or table, deploy
  code that writes both shapes, backfill, switch reads, then drop the old
  shape in a later release. Never rename a column in place under a running
  service. Number migrations sequentially and resolve conflicts at merge
  time, not when the branch is created.
- **Task payloads.** Tasks may wait in the table for hours (retries,
  polling) and be picked up by a newer binary. Treat payloads like an API:
  add optional fields, keep the old ones readable, and give an incompatible
  payload a new task kind.
- **Go module.** The module path has no `/v2`, because nothing imports this
  service as a library. If `pkg/` packages were ever published, a breaking
  change would need a new major version in the import path.
