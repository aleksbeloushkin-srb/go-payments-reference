// Package httpapi is the HTTP adapter: routes, middleware, JSON encoding,
// the long-poll endpoint, the gateway webhook and the embedded web UI.
//
// Handlers translate HTTP to use-case calls and back, nothing more. Errors
// are mapped to status codes in exactly one place, writeError. Middleware
// order is request id, logging, metrics, then recovery: the logger sees the
// id, and a recovered panic is logged and counted as the 500 it became.
//
// The package is called httpapi rather than http so it does not shadow
// net/http.
package httpapi
