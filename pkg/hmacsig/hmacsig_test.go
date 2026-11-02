package hmacsig

import (
	"errors"
	"testing"
	"time"
)

func TestSignVerify(t *testing.T) {
	secret := []byte("s3cret-s3cret-s3cret")
	body := []byte(`{"id":"evt_1"}`)
	signedAt := time.Unix(1_800_000_000, 0)
	header := Sign(secret, signedAt, body)

	tests := []struct {
		name   string
		secret []byte
		header string
		body   []byte
		now    time.Time
		want   error
	}{
		{name: "valid", secret: secret, header: header, body: body, now: signedAt.Add(time.Minute)},
		{name: "tampered body", secret: secret, header: header, body: []byte(`{"id":"evt_2"}`), now: signedAt, want: ErrMismatch},
		{name: "wrong secret", secret: []byte("other-secret-value"), header: header, body: body, now: signedAt, want: ErrMismatch},
		{name: "replayed later", secret: secret, header: header, body: body, now: signedAt.Add(10 * time.Minute), want: ErrExpired},
		{name: "from the future", secret: secret, header: header, body: body, now: signedAt.Add(-10 * time.Minute), want: ErrExpired},
		{name: "missing digest", secret: secret, header: "t=1800000000", body: body, now: signedAt, want: ErrMalformed},
		{name: "garbage", secret: secret, header: "nonsense", body: body, now: signedAt, want: ErrMalformed},
		{name: "empty", secret: secret, header: "", body: body, now: signedAt, want: ErrMalformed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Verify(tt.secret, tt.header, tt.body, tt.now, 5*time.Minute)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Verify = %v, want %v", err, tt.want)
			}
		})
	}
}
