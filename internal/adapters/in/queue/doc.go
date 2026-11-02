// Package queue runs background tasks from the task table.
//
// A Worker claims a batch of due tasks with a lease, runs each through the
// application's handler concurrently, and settles the outcome: done, retried
// with exponential backoff and jitter, postponed without counting an attempt
// (polling), or moved to the dead state after too many attempts or a
// permanent error. Dead tasks are the dead-letter queue: they are logged at
// error level, counted in task_attempts_total and visible as the tasks_dead
// gauge.
package queue
