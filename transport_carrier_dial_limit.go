package connect

import (
	"context"
	"math"
	"sync"
	"time"
)

const (
	defaultCarrierDialRate  = 8.0
	defaultCarrierDialBurst = 16
)

// carrierDialLimiter implements a token bucket rate limiter for carrier dials.
// It uses an injectable clock and an injectable-timer-free design: caller paths
// compute the wait duration and wait on a timer created at the wait site.
// Wait does not hold a lock while sleeping, and every admitted reservation
// is granted a finite delay so no goroutine starves forever.
type carrierDialLimiter struct {
	mu          sync.Mutex
	rate        float64
	burst       float64
	tokens      float64
	lastTime    time.Time
	clock       func() time.Time
	disabled    bool
	waitForTest func(ctx context.Context) bool
}

// defaultCarrierDialLimiter is the default process-wide carrier dial limiter
// configured for 8 dials/second with a burst of 16.
var defaultCarrierDialLimiter = newCarrierDialLimiter(defaultCarrierDialRate, defaultCarrierDialBurst)

func newCarrierDialLimiter(rate float64, burst int) *carrierDialLimiter {
	return newCarrierDialLimiterWithClock(rate, burst, time.Now)
}

func newCarrierDialLimiterWithClock(rate float64, burst int, clock func() time.Time) *carrierDialLimiter {
	if clock == nil {
		clock = time.Now
	}
	if rate == -1 {
		return &carrierDialLimiter{
			rate:     -1,
			disabled: true,
			clock:    clock,
		}
	}
	if rate <= 0 {
		rate = defaultCarrierDialRate
	}
	if burst <= 0 {
		burst = defaultCarrierDialBurst
	}
	return &carrierDialLimiter{
		rate:   rate,
		burst:  float64(burst),
		tokens: float64(burst),
		clock:  clock,
	}
}

func (l *carrierDialLimiter) refillLocked(now time.Time) {
	if l.lastTime.IsZero() {
		l.lastTime = now
		return
	}
	if now.After(l.lastTime) {
		elapsed := now.Sub(l.lastTime)
		l.tokens = min(l.burst, l.tokens+elapsed.Seconds()*l.rate)
		l.lastTime = now
	}
}

// computeWait calculates how long a caller must wait for 1 token at now
// without consuming any tokens.
func (l *carrierDialLimiter) computeWait(now time.Time) time.Duration {
	if l == nil || l.disabled {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	tokens := l.tokens
	if l.lastTime.IsZero() {
		tokens = l.burst
	} else if now.After(l.lastTime) {
		elapsed := now.Sub(l.lastTime)
		tokens = min(l.burst, tokens+elapsed.Seconds()*l.rate)
	}
	if 1.0 <= tokens {
		return 0
	}
	deficit := 1.0 - tokens
	return time.Duration(math.Ceil(deficit * float64(time.Second) / l.rate))
}

// reserve attempts to allocate 1 token at now. It returns the required wait
// duration and an idempotent cancellation callback that refunds the reserved
// token if the caller context ends before the wait completes.
func (l *carrierDialLimiter) reserve(now time.Time) (time.Duration, func()) {
	if l == nil || l.disabled {
		return 0, func() {}
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	l.refillLocked(now)

	if 1.0 <= l.tokens {
		l.tokens -= 1.0
		return 0, func() {}
	}

	deficit := 1.0 - l.tokens
	wait := time.Duration(math.Ceil(deficit * float64(time.Second) / l.rate))
	l.tokens -= 1.0

	var canceled bool
	cancel := func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if canceled {
			return
		}
		canceled = true
		cNow := l.clock()
		l.refillLocked(cNow)
		l.tokens = min(l.burst, l.tokens+1.0)
	}
	return wait, cancel
}

// Wait blocks until a carrier dial token is available or ctx ends. It returns
// true if a token was acquired and false if ctx ends first.
func (l *carrierDialLimiter) Wait(ctx context.Context) bool {
	if l == nil || l.disabled {
		return true
	}
	if l.waitForTest != nil {
		return l.waitForTest(ctx)
	}
	if ctx.Err() != nil {
		return false
	}
	wait, cancel := l.reserve(l.clock())
	if wait <= 0 {
		return true
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		cancel()
		return false
	case <-timer.C:
		return true
	}
}

// waitCarrierDialSlot acquires a carrier dial token from the transport's
// configured limiter (or the default process-wide limiter). It returns false
// if ctx ends first.
func (self *PlatformTransport) waitCarrierDialSlot(ctx context.Context) bool {
	if self == nil || self.carrierDialLimiter == nil {
		return true
	}
	return self.carrierDialLimiter.Wait(ctx)
}

// reconnectFastPathAllowed reports whether a carrier may take the reconnect
// fast path. If the runner parked in standDown while a strictly better mode
// was active, it must not take the cheap fast path and instead fall back to
// the serialized staircase.
func reconnectFastPathAllowed(hadConnection, parked bool) bool {
	return hadConnection && !parked
}
