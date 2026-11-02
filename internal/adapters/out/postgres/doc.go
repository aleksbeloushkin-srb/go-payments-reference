// Package postgres implements the storage ports on PostgreSQL with pgx:
// payments, the task queue and the outbox.
//
// Every query goes through txctx.Manager.DB(ctx), so a repository call made
// inside txctx.Manager.Run joins the caller's transaction, and a call made
// outside runs on the pool. Methods that are only meaningful inside a
// transaction (row locks) refuse to run without one.
package postgres
