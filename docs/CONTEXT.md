# context.Context in this service

`context.Context` carries two things and nothing else: the lifetime of an
operation (cancellation and deadline) and a small amount of request-scoped
data for cross-cutting concerns (here, the request id). This document walks
through how the service uses it, where each rule shows up in the code, and
the mistakes it is designed to avoid.

## Rules

| # | Rule | Where to look |
| --- | --- | --- |
| 1 | `ctx` is the first parameter of every function that does I/O or may block. Request code never stores it in a struct. The one exception is an owner of background work, which keeps the context it cancels and exposes `Stop` or `close` to end it. | every port in `internal/ports/ports.go`; the exceptions are `platform.Group` and the fake gateway |
| 2 | Each outbound call gets its own deadline, derived from the caller's: `context.WithTimeout(ctx, callTimeout)`. The effective deadline is the earlier of the two. | `internal/adapters/out/gateway/client.go` (`once`) |
| 3 | Do not start a wait that cannot finish before the caller's deadline. | `client.go` (`fits`), used before honouring `Retry-After` |
| 4 | The process root context comes from `signal.NotifyContext`. Shutdown work uses a fresh deadline, because the root is already cancelled at that point. | `cmd/payments/main.go` (`main`, `shutdown`) |
| 5 | Only request-scoped data goes into `Value`: the request id, under an unexported key type, with a `WithRequestID`/`RequestID` pair. The logger is an explicit dependency, not a context value. | `internal/platform/logger.go` |
| 6 | Work that must outlive its caller uses `context.WithoutCancel(ctx)`, always paired with its own timeout and an owner that waits for it. | `internal/adapters/in/queue/worker.go` (`process`, `settle`), `pkg/txctx/txctx.go` (rollback, commit hooks) |
| 7 | Every wait is a `select` on `ctx.Done()` and a timer or channel. There is no `time.Sleep` in production code or tests. | `client.go` (`sleepCtx`), `longpoll/batcher.go` (`Run`), `queue/worker.go` (`Run`) |
| 8 | Two different "done" causes are kept apart: our own soft timeout is a normal answer, the client's cancellation is not. | `internal/app/query.go` (`WaitPayment`) |
| 9 | Every goroutine has an owner and an exit path. | `internal/platform/lifecycle.go` (`Group`), `cmd/fakegateway/gateway.go` (`goBackground`, `close`) |
| 10 | A zero timeout is never intended: configuration rejects it at start. | `internal/platform/config.go` (`validate`) |
| 11 | Where an API gives no context, create one with a timeout instead of running unbounded. `prometheus.Collector.Collect` receives no context, so the queue collector bounds its query with its own 2 s timeout. | `internal/adapters/out/postgres/collector.go` (`Collect`) |

## Deadlines and budgets

Budgets nest. The HTTP server's `WriteTimeout` must be longer than the
long-poll soft timeout and its two database reads, each bounded by
`PAYMENTS_WAIT_READ_TIMEOUT`; the task timeout must cover the gateway's worst
case (three calls and two `Retry-After` waits); the task lease must be longer
than the task timeout; the database's idle-in-transaction timeout must be
longer than a task, because the `in_tx` start (version 1) holds a
transaction open during the gateway call. `platform.Config.validate` checks each of these relations, so a
configuration that would cut a long poll short or let a slow task be claimed
twice does not start.

The gateway client sets a per-call timeout with `context.WithTimeout(ctx,
CallTimeout)`. If the caller's context already has a shorter deadline, that
one wins, which is what a downstream budget should be: never longer than
what the caller has left. `http.Client.Timeout` is set too, but only as a
backstop. The outbox relay works the same way: each delivery gets
`PAYMENTS_EVENTS_WEBHOOK_TIMEOUT`, and a delivery starts only if it can
finish before the relay's claim on the event expires.

## Values: what goes in, what stays out

The request id goes in. The middleware puts it there and a small `slog`
handler copies it into every log record written with `InfoContext` and
friends, so log lines from the use cases and adapters carry the id without
any of them knowing about HTTP.

Dependencies stay out: the logger, the database pool, the gateway client.
They are constructor parameters, so a missing one is a compile error or a
start-up panic, not a nil value discovered on the first request. Inputs that
decide correctness (amount, currency, idempotency key) are function
parameters for the same reason.

The transaction is the one deliberate exception. `pkg/txctx` keeps it in the
context so use cases stay free of SQL types and nested calls join one unit
of work. The cost is that the dependency is invisible: a repository method
called outside `Run` silently autocommits. `Payments.Lock` refuses to run
without a transaction, because a row lock outside a transaction is
meaningless. Goroutines must not be started inside a transaction at all:
work that follows a write is started from an `AfterCommit` hook.

## Work that outlives its caller

Three places detach from cancellation on purpose:

- **Task handlers.** When the worker's context is cancelled at shutdown, it
  stops claiming, but a task already running keeps going on
  `context.WithoutCancel(ctx)` bounded by `PAYMENTS_TASK_TIMEOUT`. Cancelling
  it mid-way through a gateway call would turn a clean result into an
  ambiguous one. The shutdown waits for these tasks.
- **Settling a task.** Recording the outcome uses its own short timeout on a
  detached context, so a cancelled worker still writes "done" for work it
  has finished.
- **Rollback and commit hooks.** A rollback after the caller gave up must
  still reach the database; commit hooks run after the commit happened and
  must not be cut short by a client that has left.

Each of these has a bound, either `WithTimeout` or the task timeout, and an
owner that waits for it. `context.WithoutCancel` without both is how
goroutines leak.

## Long polling: whose Done fired?

`WaitPayment` waits on three things:

```go
select {
case _, ok := <-changes: // the batcher saw a newer version, or shutdown closed the channel
case <-timer.C:           // our soft timeout: answer "not modified"
case <-ctx.Done():        // the client went away: nobody reads the answer
}
```

Using a timer instead of a derived `context.WithTimeout` keeps the two exits
apart without inspecting which context fired. The deregistration is a plain
`defer cancel()`. `context.AfterFunc` would work as well and avoids a
goroutine per waiter in designs where the wait is not already a `select`.

## Shutdown

`main` creates the root context with `signal.NotifyContext`. When it is
cancelled, `run` calls the `stop` function at once, which restores the
default signal behaviour, so a second Ctrl-C kills the process. Then:

```go
sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
defer cancel()
err := srv.Shutdown(sctx)
```

Passing the already cancelled root context to `Shutdown` would return
immediately and drain nothing. `WithoutCancel` keeps any values (none today)
while dropping the cancellation. Background components run on a context
that is not derived from the signal context at all (`platform.Group`), so
they keep working while HTTP drains and stop only when `Group.Stop` cancels
them. The worker is the exception in one respect: it stops claiming new
tasks the moment shutdown starts (`Worker.StopClaiming`), and only its
running tasks continue. `context.Cause` is logged so the reason for the
shutdown is visible.

## Pitfalls this code avoids

| Pitfall | Consequence | What the code does instead |
| --- | --- | --- |
| `ctx` as a struct field | the struct outlives the request; cancellation goes to the wrong operation | `ctx` is a parameter; only owners of background work keep the context they cancel |
| `context.Background()` in the middle of a call chain | the request id and the deadline are lost | derive from the incoming `ctx`; detach with `WithoutCancel` when needed |
| `srv.Shutdown(rootCtx)` | drains nothing, in-flight requests are cut on every deploy | fresh deadline from `WithoutCancel` |
| `time.Sleep` in a retry or poll loop | ignores cancellation, delays shutdown | `select` with a timer and `ctx.Done()` |
| a retry loop that ignores `ctx` | keeps retrying for a client that has gone | the gateway client checks the deadline before each wait |
| lost `cancel` (`_ = cancel`) | the timer lives until it fires | `defer cancel()` right after every `WithTimeout` |
| `WithTimeout(ctx, cfg.X)` with an unset field | every call fails at once | configuration rejects zero durations |
| goroutines started inside a transaction | they use a connection that is about to be returned | start them from an `AfterCommit` hook |
| dependencies in `ctx.Value` | invisible requirements, nil at run time | constructor parameters |
