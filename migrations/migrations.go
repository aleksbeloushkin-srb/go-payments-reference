// Package migrations embeds the SQL schema. The payments binary applies it
// at start with goose, so a fresh database needs no manual step.
package migrations

import "embed"

// FS holds the migration files in goose format.
//
//go:embed *.sql
var FS embed.FS
