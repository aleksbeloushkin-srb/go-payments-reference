// Package hmacsig signs and verifies webhook bodies with HMAC-SHA256.
//
// The header value has the form "t=<unix seconds>,v1=<hex digest>", where the
// digest covers "<t>.<body>". Including the timestamp lets the receiver
// reject old messages, so a captured request cannot be replayed later.
package hmacsig

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"
)

// Header is the HTTP header that carries the signature.
const Header = "Webhook-Signature"

// Errors returned by Verify.
var (
	ErrMalformed = errors.New("hmacsig: malformed signature header")
	ErrMismatch  = errors.New("hmacsig: signature mismatch")
	ErrExpired   = errors.New("hmacsig: signature timestamp outside tolerance")
)

// Sign returns the header value for body signed at ts.
func Sign(secret []byte, ts time.Time, body []byte) string {
	t := strconv.FormatInt(ts.Unix(), 10)
	return "t=" + t + ",v1=" + digest(secret, t, body)
}

// Verify checks header against body. The timestamp must be within tolerance
// of now in either direction.
func Verify(secret []byte, header string, body []byte, now time.Time, tolerance time.Duration) error {
	var t, v1 string
	for _, part := range strings.FieldsFunc(header, func(r rune) bool { return r == ',' }) {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return ErrMalformed
		}
		switch key {
		case "t":
			t = value
		case "v1":
			v1 = value
		}
	}
	if t == "" || v1 == "" {
		return ErrMalformed
	}
	unix, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return ErrMalformed
	}
	if age := now.Sub(time.Unix(unix, 0)); age > tolerance || age < -tolerance {
		return ErrExpired
	}
	want := digest(secret, t, body)
	if !hmac.Equal([]byte(v1), []byte(want)) {
		return ErrMismatch
	}
	return nil
}

func digest(secret []byte, t string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(t))
	mac.Write([]byte("."))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
