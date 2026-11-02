// Package ports declares what the application layer needs from the outside
// world: payment storage, a task queue, an outbox, the payment gateway, a
// clock, a transaction runner, a change watcher and business metrics.
//
// The interfaces belong to the consumer (internal/app), not to the adapters
// that implement them, and they are deliberately small: a use case depends on
// a handful of methods, so a test fake is a few lines. Adapters satisfy them
// implicitly; cmd/payments wires everything together.
//
// The package also holds the plain data types and error values that cross
// the boundary (tasks, gateway results, retry hints), so both sides can share
// them without importing each other.
package ports
