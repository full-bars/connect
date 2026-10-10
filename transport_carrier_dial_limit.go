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
// It uses an injectable clock; callers block in Wait, which computes the wait
// and creates its own timer without holding the lock while sleeping, and every
// admitted reservation is granted a finite delay so no goroutine starves
// forever.
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

// carrierDialLimiterKey identifies a limiter by its effective settings. Two
// transports configured with the same pair share one bucket.
type carrierDialLimiterKey struct {
	rate  float64
	burst int
}

// carrierDialLimiterRegistry maps a settings pair to the single limiter that
// enforces it process-wide.
//
// WHY THIS EXISTS: the settings are documented as a PROCESS-WIDE cap on carrier
// dials, but each PlatformTransport used to build its own limiter from the same
// settings. The multi-client window copies one settings value into every
// transport (up to 3 per client in a family group), so an operator who set the
// default explicitly multiplied the cap by the transport count: rate=4 with 16
// transports admitted 64/s. Keying the limiter by its effective settings makes
// the cap what it says it is, and makes an explicit default share the same
// bucket as leaving it unset.
var carrierDialLimiterRegistry sync.Map // carrierDialLimiterKey -> *carrierDialLimiter

// sharedCarrierDialLimiter returns the process-wide limiter for these
// settings, creating it on first use.
func sharedCarrierDialLimiter(rate float64, burst int) *carrierDialLimiter {
	key := carrierDialLimiterKey{rate: rate, burst: burst}
	if l, ok := carrierDialLimiterRegistry.Load(key); ok {
		return l.(*carrierDialLimiter)
	}
	actual, _ := carrierDialLimiterRegistry.LoadOrStore(key, newCarrierDialLimiter(rate, burst))
	return actual.(*carrierDialLimiter)
}

func newCarrierDialLimiter(rate float64, burst int) *carrierDialLimiter {
	return newCarrierDialLimiterWithClock(rate, burst, time.Now)
}

func newCarrierDialLimiterWithClock(rate float64, burst int, clock func() time.Time) *carrierDialLimiter {
	if clock == nil {
		clock = time.Now
	}
	// ANY negative rate disables the limit, not just an exact -1. Matching -1
	// exactly meant -2 silently fell through to the default limiter, which is
	// the opposite of what an operator who typed a negative number asked for.
	if rate < 0 {
		return &carrierDialLimiter{
			rate:     rate,
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

// maxCarrierDialWait bounds a single computed wait. Without it, a tiny rate
// makes the float-to-Duration conversion overflow: on amd64 an out-of-range
// float64 to int64 conversion yields the minimum int64, so the wait goes
// NEGATIVE and Wait admits immediately — the limiter silently stops limiting at
// exactly the rates where it matters most. A minute is far longer than any
// legitimate pacing wait.
const maxCarrierDialWait = time.Minute

// waitDuration converts a token deficit into a wait, clamped so an extreme rate
// cannot overflow the Duration conversion into a negative (i.e. instant) wait.
func waitDuration(deficit, rate float64) time.Duration {
	if deficit <= 0 || rate <= 0 {
		return 0
	}
	d := time.Duration(math.Ceil(deficit * float64(time.Second) / rate))
	if d < 0 {
		// Overflow landed in the negative range: the true wait is enormous.
		return maxCarrierDialWait
	}
	if d > maxCarrierDialWait {
		return maxCarrierDialWait
	}
	return d
}

// computeWaitForTest calculates how long a caller must wait for 1 token at
// now without consuming any tokens. Test-only: production gating goes through
// reserve/Wait, which compute and reserve atomically.
func (l *carrierDialLimiter) computeWaitForTest(now time.Time) time.Duration {
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
	return waitDuration(1.0-tokens, l.rate)
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

	// waitDuration, not a raw conversion: at a tiny rate the float-to-Duration
	// conversion overflows to the minimum int64, the wait goes negative, and
	// Wait treats `wait <= 0` as "admit immediately" — the limiter silently
	// stops limiting at exactly the rates where it matters.
	wait := waitDuration(1.0-l.tokens, l.rate)
	l.tokens -= 1.0

	var canceled bool
	cancel := func() {
		// Sample the clock before taking the lock: a caller-supplied clock
		// may synchronize with its own mutex, and holding l.mu across the
		// clock call would invert the order against Wait's unlocked read.
		cNow := l.clock()
		l.mu.Lock()
		defer l.mu.Unlock()
		if canceled {
			return
		}
		canceled = true
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
