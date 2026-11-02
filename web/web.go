// Package web embeds the browser UI that the payments binary serves at "/".
// It is plain HTML, CSS and JavaScript with no build step.
package web

import "embed"

// FS holds the UI files.
//
//go:embed index.html app.js style.css logo.svg
var FS embed.FS
