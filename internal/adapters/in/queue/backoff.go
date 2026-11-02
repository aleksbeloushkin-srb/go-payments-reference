package queue

import (
	"math/rand/v2"
	"time"
)

// Backoff computes retry delays: exponential growth from Base, capped at
// Max, with equal jitter (half of the delay is fixed, half is random). The
// jitter spreads retries of tasks that failed together, for example during a
// gateway outage, so they do not return as one synchronized wave.
type Backoff struct {
	Base time.Duration
	Max  time.Duration
	// Rand returns a number in [0, n). Nil means math/rand/v2.
	Rand func(n int64) int64
}

// Delay returns the wait before the given attempt (1 for the first retry).
func (b Backoff) Delay(attempt int) time.Duration {
	d := b.Base
	for i := 1; i < attempt && d < b.Max; i++ {
		d *= 2
	}
	d = min(d, b.Max)
	half := d / 2
	randN := b.Rand
	if randN == nil {
		randN = rand.Int64N
	}
	return half + time.Duration(randN(int64(half)+1))
}
