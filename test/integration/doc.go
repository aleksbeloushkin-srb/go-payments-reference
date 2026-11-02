// Package integration runs the adapters and use cases against a real
// PostgreSQL started with testcontainers. The tests carry the integration
// build tag, so a plain go test ./... needs no Docker:
//
//	go test -race -tags integration ./test/integration/...
package integration
