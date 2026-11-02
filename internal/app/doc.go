// Package app contains the use cases of the payment service.
//
// Synchronous use cases serve the HTTP API: CreatePayment, Payment and
// WaitPayment. Task handlers run in the background worker: StartPayment,
// PollPayment and ApplyGatewayEvent, dispatched by HandleTask.
//
// The package depends only on internal/domain/payment and internal/ports.
// It never imports an adapter, so every use case can be tested with
// in-memory fakes, and the database or the gateway can be replaced without
// touching business rules.
package app
