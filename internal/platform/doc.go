// Package platform holds process-level plumbing shared by the binaries:
// strict configuration from the environment, the structured logger, a
// readiness flag, a clock and an owner for background goroutines.
//
// It contains no business logic and imports no other internal package.
package platform
