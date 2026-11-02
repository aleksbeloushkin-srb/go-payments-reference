// Package longpoll implements change notification for GET /wait.
//
// Waiters register in a per-instance Registry with the version they have
// already seen. One Batcher goroutine per instance asks the database for the
// current versions of all awaited payments in a single query and wakes every
// waiter whose payment moved past its marker. Database cost is therefore one
// query per tick per instance, not one per waiting client.
//
// Markers make the scheme robust: a waiter is woken only when it observes a
// version strictly greater than the one it has seen, so a duplicate wake-up,
// a late registration or a read from a lagging source can delay an answer
// but never produce a wrong one.
package longpoll
