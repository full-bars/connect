package connect

// Coverage for the second review round on the carrier dial limiter: the
// process-wide limiter registry, the negative-rate sentinel, the clamped wait
// conversion, and the reordering that moved the slot acquisition after the
// standDown recheck.
//
// Every test here is written against the CONSTRUCTOR rather than a struct
// literal. The previous round's tests built a PlatformTransport by hand and set
// carrierDialLimiter directly, so settings.carrierDialLimiterForTest was dead
// code in that test and none of the constructor wiring ran anywhere.

import (
	"context"
	"strings"
	"testing"
	"time"
)

// transportWithDialSettings builds a REAL transport through the constructor so
// the settings-to-limiter wiring actually runs. Constructing a
// PlatformTransport by struct literal skips that wiring entirely, which is how
// the previous round's tests left carrierDialLimiterForTest dead and the
// per-transport limiter untested.
func transportWithDialSettings(t *testing.T, rate float64, burst int) *PlatformTransport {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	settings := DefaultPlatformTransportSettings()
	settings.CarrierDialRate = rate
	settings.CarrierDialBurst = burst

	tr := NewPlatformTransportWithTargetMode(
		ctx,
		NewClientStrategyWithDefaults(ctx),
		NewRouteManager(ctx, "carrier-dial-settings"),
		"https://example.invalid",
		&ClientAuth{ByJwt: "testing", InstanceId: NewId(), AppVersion: "testing"},
		TransportModeH3,
		settings,
	)
	t.Cleanup(tr.Close)
	return tr
}

// --- O1: the limiter is process-wide, shared by equal settings ---------------

// TestCarrierDialLimiterSettingsAreProcessWide pins the documented contract:
// CarrierDialRate caps carrier dials PROCESS-WIDE. Building a limiter per
// transport meant the cap was really per-transport, and the multi-client window
// copies one settings value into up to 3 transports per client — so rate=4 with
// 16 transports admitted 64/s.
func TestCarrierDialLimiterSettingsAreProcessWide(t *testing.T) {
	// Two transports built with the SAME explicit settings share one bucket.
	a, b := transportWithDialSettings(t, 4, 8), transportWithDialSettings(t, 4, 8)
	if a.carrierDialLimiter != b.carrierDialLimiter {
		t.Error("two transports with identical dial settings must share one limiter; " +
			"a per-transport limiter multiplies the documented process-wide cap by the transport count")
	}

	// An explicit default and an unset default share the SAME bucket, so
	// spelling out the documented default cannot silently change behaviour.
	explicit := transportWithDialSettings(t, defaultCarrierDialRate, defaultCarrierDialBurst)
	unset := transportWithDialSettings(t, 0, 0)
	if explicit.carrierDialLimiter != defaultCarrierDialLimiter {
		t.Error("an explicitly configured default must resolve to the shared default limiter, " +
			"not build a second bucket with the same numbers")
	}
	if unset.carrierDialLimiter != defaultCarrierDialLimiter {
		t.Error("unset dial settings must resolve to the shared default limiter")
	}

	// Different settings get their own bucket.
	other := transportWithDialSettings(t, 2, 8)
	if other.carrierDialLimiter == a.carrierDialLimiter {
		t.Error("distinct settings must not share a bucket")
	}
}

// TestSharedCarrierDialLimiterIsStable pins that repeated lookups of the same
// pair return the identical pointer, including under concurrent construction —
// the registry must not hand two limiters to two transports racing at startup.
func TestSharedCarrierDialLimiterIsStable(t *testing.T) {
	const n = 16
	got := make([]*carrierDialLimiter, n)
	done := make(chan struct{})
	for i := range got {
		go func(i int) {
			defer func() { done <- struct{}{} }()
			got[i] = sharedCarrierDialLimiter(3, 7)
		}(i)
	}
	for range n {
		<-done
	}
	for i := 1; i < n; i++ {
		if got[i] != got[0] {
			t.Fatalf("concurrent sharedCarrierDialLimiter returned %d distinct limiters", i+1)
		}
	}
}

// --- O4: any negative rate disables, not just -1 ----------------------------

// TestAnyNegativeDialRateDisables pins that every negative value disables the
// limiter. The old code matched -1 exactly, so -2 fell through to the DEFAULT
// limiter — the opposite of what an operator who typed a negative number asked
// for, and silent about it.
func TestAnyNegativeDialRateDisables(t *testing.T) {
	for _, rate := range []float64{-1, -2, -0.5, -1e9} {
		tr := transportWithDialSettings(t, rate, 8)
		if tr.carrierDialLimiter == nil || !tr.carrierDialLimiter.disabled {
			t.Errorf("CarrierDialRate=%v must disable the limiter, got %+v", rate, tr.carrierDialLimiter)
		}
	}
}

// TestZeroDialRateUsesTheDefault pins the other half of the rule: 0 is not a
// negative and must mean "use the default", not "disable".
func TestZeroDialRateUsesTheDefault(t *testing.T) {
	tr := transportWithDialSettings(t, 0, 0)
	if tr.carrierDialLimiter == nil || tr.carrierDialLimiter.disabled {
		t.Errorf("CarrierDialRate=0 must use the default limiter, got %+v", tr.carrierDialLimiter)
	}
	if tr.carrierDialLimiter != defaultCarrierDialLimiter {
		t.Error("CarrierDialRate=0 must resolve to the shared default limiter")
	}
}

// --- O5: the wait conversion cannot overflow into an instant admit ----------

// TestWaitDurationClampsExtremeRates pins the overflow fix. A float64 to
// time.Duration conversion out of range yields the minimum int64 on amd64, so
// the wait went NEGATIVE and Wait admitted immediately — the limiter stopped
// limiting at exactly the rates where it matters. Opus measured rate=1e-11
// producing a wait of -2562047h47m16.85s.
func TestWaitDurationClampsExtremeRates(t *testing.T) {
	for _, rate := range []float64{1e-11, 1e-300, 1e-300 * 1e-300} {
		got := waitDuration(1, rate)
		if got < 0 {
			t.Errorf("waitDuration(1, %v) = %v, which is negative and would admit immediately", rate, got)
		}
		if got > maxCarrierDialWait {
			t.Errorf("waitDuration(1, %v) = %v, want at most the %v clamp", rate, got, maxCarrierDialWait)
		}
	}
}

// TestWaitDurationNormalRatesUnchanged pins that the clamp does not distort an
// ordinary rate. A 4/s limiter with a full token deficit waits ~250ms.
func TestWaitDurationNormalRatesUnchanged(t *testing.T) {
	if got := waitDuration(1, 4); got != 250*time.Millisecond {
		t.Errorf("waitDuration(1, 4) = %v, want 250ms", got)
	}
	if got := waitDuration(0, 4); got != 0 {
		t.Errorf("a zero deficit needs no wait, got %v", got)
	}
	if got := waitDuration(1, 0); got != 0 {
		t.Errorf("a zero rate needs no wait, got %v", got)
	}
}

// TestTinyRateLimiterStillRefusesAfterWait pins the end-to-end consequence: a
// limiter built with an absurd rate must return false from Wait within a
// bounded time rather than admitting the caller instantly.
func TestTinyRateLimiterStillRefusesAfterWait(t *testing.T) {
	l := newCarrierDialLimiter(1e-11, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	// The bucket holds one token, so the first reservation is granted.
	if !l.Wait(ctx) {
		t.Fatal("the first reservation within the burst must be granted")
	}
	// The second must block, and the context ends the wait.
	if l.Wait(ctx) {
		t.Error("the second reservation must not be granted at an absurdly small rate")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Wait blocked for %v; the clamp should bound it near the context deadline", elapsed)
	}
}

// --- O3: the slot is taken after the standDown recheck ----------------------

// TestRunH3TakesSlotAfterStandDownRecheck pins the ORDER, which is the whole
// point of the reorder. A token spent before the recheck is a token burned on a
// dial that never happens, and a better mode elected during the gate wait is
// normal in auto mode rather than an edge case.
func TestRunH3TakesSlotAfterStandDownRecheck(t *testing.T) {
	src, err := readSource("transport.go")
	if err != nil {
		t.Fatalf("read transport.go: %v", err)
	}
	start := strings.Index(src, "attemptCtx, releaseAttempt, acquired := self.h3Gate.Acquire(")
	if start < 0 {
		t.Fatal("could not find the h3Gate.Acquire call in runH3")
	}
	end := strings.Index(src[start:], "var connStream")
	if end < 0 {
		t.Fatal("could not find the end of the runH3 preamble")
	}
	preamble := src[start : start+end]

	acquire := strings.Index(preamble, "self.h3Gate.Acquire(")
	standDown := strings.Index(preamble, "self.standDown(")
	slot := strings.Index(preamble, "self.waitCarrierDialSlot(")

	switch {
	case acquire < 0:
		t.Fatal("runH3 preamble has no gate Acquire")
	case standDown < 0:
		t.Fatal("runH3 preamble has no standDown recheck")
	case slot < 0:
		t.Fatal("runH3 preamble has no carrier dial slot wait")
	case slot < standDown:
		t.Error("the carrier dial slot must be taken AFTER the standDown recheck, or a parked " +
			"attempt spends a token on a dial that never happens")
	case slot < acquire:
		t.Error("the carrier dial slot must be taken AFTER the gate Acquire, or a token can be " +
			"granted seconds before the dial runs and the real dial instants bunch past the burst")
	}
}

// TestRunH3RefundsAbandonedReservation pins that a slot wait cancelled by the
// ATTEMPT context continues the loop rather than returning, and that it uses
// attemptCtx so a better mode's election refunds the token.
func TestRunH3RefundsAbandonedReservation(t *testing.T) {
	src, err := readSource("transport.go")
	if err != nil {
		t.Fatalf("read transport.go: %v", err)
	}
	start := strings.Index(src, "attemptCtx, releaseAttempt, acquired := self.h3Gate.Acquire(")
	if start < 0 {
		t.Fatal("could not find the h3Gate.Acquire call in runH3")
	}
	end := strings.Index(src[start:], "var connStream")
	preamble := src[start : start+end]

	if !strings.Contains(preamble, "self.waitCarrierDialSlot(attemptCtx)") {
		t.Error("the slot wait must use attemptCtx, not ctx: attemptCtx is cancelled when a better " +
			"mode wins, which is what refunds the abandoned reservation")
	}
	// The abandon path must release the gate and continue, not return, unless
	// the caller's own context is done.
	for _, want := range []string{"releaseAttempt()", "cancelConnect()", "if ctx.Err() != nil {", "continue"} {
		if !strings.Contains(preamble, want) {
			t.Errorf("the abandoned-slot path is missing %q", want)
		}
	}
}
