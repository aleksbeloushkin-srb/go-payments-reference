// Package payment is the domain model: a payment from a bank client to a
// merchant, executed through an external payment gateway.
//
// It is the innermost layer. It imports only the standard library and
// google/uuid, knows nothing about HTTP, SQL or queues, and is the single
// place that decides which status transitions are legal. Every other layer
// asks this package instead of re-implementing the rules.
package payment
