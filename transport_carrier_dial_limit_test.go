package connect

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCarrierDialLimiter_BurstAdmitsImmediately verifies that a burst of 16
// admits immediately (wait duration 0) with a fake clock.
func TestCarrierDialLimiter_BurstAdmitsImmediately(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	limiter := newCarrierDialLimiterWithClock(8, 16, clock)

	for i := 0; i < 16; i++ {
		wait, _ := limiter.reserve(now)
		if wait != 0 {
			t.Fatalf("call %d: expected immediate admission (wait 0), got %v", i+1, wait)
		}
	}
}

// TestCarrierDialLimiter_17thWaitsComputedDuration verifies that after consuming
// the 16-token burst, the 17th call waits ~125ms (1/8 s) by the computed wait.
func TestCarrierDialLimiter_17thWaitsComputedDuration(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	limiter := newCarrierDialLimiterWithClock(8, 16, clock)

	// Consume burst of 16
	for i := 0; i < 16; i++ {
		if wait, _ := limiter.reserve(now); wait != 0 {
			t.Fatalf("call %d: expected wait 0, got %v", i+1, wait)
		}
	}

	// 17th call computed wait
	computed := limiter.computeWaitForTest(now)
	wantWait := 125 * time.Millisecond // 1/8 second
	if computed != wantWait {
		t.Fatalf("computeWait for 17th call = %v, want %v", computed, wantWait)
	}

	wait, _ := limiter.reserve(now)
	if wait != wantWait {
		t.Fatalf("reserve for 17th call = %v, want %v", wait, wantWait)
	}
}

// TestCarrierDialLimiter_RefillRateAndCap verifies that tokens refill at the
// configured rate (8/s) and never exceed the burst cap (16).
func TestCarrierDialLimiter_RefillRateAndCap(t *testing.T) {
	var mu sync.Mutex
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	limiter := newCarrierDialLimiterWithClock(8, 16, clock)

	// Consume all 16 tokens
	for i := 0; i < 16; i++ {
		limiter.reserve(clock())
	}

	// Advance clock by 500ms: should refill 0.5s * 8 = 4 tokens
	mu.Lock()
	now = now.Add(500 * time.Millisecond)
	mu.Unlock()

	for i := 0; i < 4; i++ {
		wait, _ := limiter.reserve(clock())
		if wait != 0 {
			t.Fatalf("refilled token %d: expected wait 0, got %v", i+1, wait)
		}
	}
	// 5th call should wait
	if wait, _ := limiter.reserve(clock()); wait == 0 {
		t.Fatalf("expected 5th call to wait, but got wait 0")
	}

	// Advance clock by 10 seconds: refilled tokens capped at burst (16)
	mu.Lock()
	now = now.Add(10 * time.Second)
	mu.Unlock()

	for i := 0; i < 16; i++ {
		wait, _ := limiter.reserve(clock())
		if wait != 0 {
			t.Fatalf("capped token %d: expected wait 0, got %v", i+1, wait)
		}
	}
	// 17th call should wait
	if wait, _ := limiter.reserve(clock()); wait == 0 {
		t.Fatalf("expected 17th call after cap to wait, but got wait 0")
	}
}

// TestCarrierDialLimiter_ContextCancelNoTokenConsumed verifies that a cancelled
// context returns false without consuming a token.
func TestCarrierDialLimiter_ContextCancelNoTokenConsumed(t *testing.T) {
	var mu sync.Mutex
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	limiter := newCarrierDialLimiterWithClock(8, 16, clock)

	// Consume burst
	for i := 0; i < 16; i++ {
		limiter.reserve(clock())
	}

	// Wait with an already canceled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	if admitted := limiter.Wait(canceledCtx); admitted {
		t.Fatalf("Wait with canceled context should return false")
	}

	// Test that reservation cancellation refunds the token
	wait, refund := limiter.reserve(clock())
	if wait != 125*time.Millisecond {
		t.Fatalf("expected wait 125ms, got %v", wait)
	}
	refund()

	// Advance clock by 125ms
	mu.Lock()
	now = now.Add(125 * time.Millisecond)
	mu.Unlock()

	// If token was not refunded, tokens would be 0 (1 refilled - 1 consumed = 0).
	// Because refund occurred, tokens is 1 (0 + 1 refilled = 1).
	waitNext, _ := limiter.reserve(clock())
	if waitNext != 0 {
		t.Fatalf("expected wait 0 after refund and 125ms refill, got %v", waitNext)
	}
}

// TestCarrierDialLimiter_DisabledNeverWaits verifies that a disabled limiter
// (rate -1) never waits and admits immediately.
func TestCarrierDialLimiter_DisabledNeverWaits(t *testing.T) {
	limiter := newCarrierDialLimiter(-1, 0)
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		if !limiter.Wait(ctx) {
			t.Fatalf("disabled limiter call %d should admit immediately", i)
		}
		if wait := limiter.computeWaitForTest(time.Now()); wait != 0 {
			t.Fatalf("disabled limiter computeWait should be 0, got %v", wait)
		}
	}
}

// TestCarrierDialLimiter_ConcurrentWindowAdmissions verifies that concurrent
// callers using sync primitives never admit more than burst + elapsed*rate in
// any window.
func TestCarrierDialLimiter_ConcurrentWindowAdmissions(t *testing.T) {
	var mu sync.Mutex
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}

	const (
		rate       = 8.0
		burst      = 16
		numCallers = 64
	)
	limiter := newCarrierDialLimiterWithClock(rate, burst, clock)

	type callerResult struct {
		wait time.Duration
	}
	results := make([]callerResult, numCallers)
	var wg sync.WaitGroup
	wg.Add(numCallers)

	startGate := make(chan struct{})
	for i := 0; i < numCallers; i++ {
		go func(idx int) {
			defer wg.Done()
			<-startGate
			wait, _ := limiter.reserve(clock())
			results[idx] = callerResult{wait: wait}
		}(i)
	}
	close(startGate)
	wg.Wait()

	// In any elapsed window [0, T], callers with wait <= T cannot exceed burst + T * rate
	testWindows := []time.Duration{
		0,
		250 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		2 * time.Second,
		3 * time.Second,
	}

	for _, w := range testWindows {
		admitted := 0
		for _, r := range results {
			if r.wait <= w {
				admitted++
			}
		}
		maxAllowed := burst + int(w.Seconds()*rate)
		if admitted > maxAllowed {
			t.Fatalf("in window %v: admitted %d, max allowed %d (burst %d + %v * %v)",
				w, admitted, maxAllowed, burst, w, rate)
		}
	}
}

// TestReconnectFastPathAllowed_TruthTable tests the truth table of the pure
// helper reconnectFastPathAllowed.
func TestReconnectFastPathAllowed_TruthTable(t *testing.T) {
	cases := []struct {
		hadConnection bool
		parked        bool
		want          bool
	}{
		{hadConnection: true, parked: false, want: true},
		{hadConnection: true, parked: true, want: false},
		{hadConnection: false, parked: false, want: false},
		{hadConnection: false, parked: true, want: false},
	}
	for _, tc := range cases {
		got := reconnectFastPathAllowed(tc.hadConnection, tc.parked)
		if got != tc.want {
			t.Errorf("reconnectFastPathAllowed(%v, %v) = %v, want %v",
				tc.hadConnection, tc.parked, got, tc.want)
		}
	}
}

// TestRunH3_SourceAnchor verifies that runH3 gates dial attempts via
// waitCarrierDialSlot before h3Gate.Acquire, and resets hadConnection when parked.
func TestRunH3_SourceAnchor(t *testing.T) {
	source, err := readSource("transport.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *PlatformTransport) runH3(")
	if !ok {
		t.Fatalf("could not find func (self *PlatformTransport) runH3(")
	}

	// Task A check: waitCarrierDialSlot immediately precedes h3Gate.Acquire
	slotCall := "if !self.waitCarrierDialSlot(ctx) {"
	gateAcquire := "self.h3Gate.Acquire(ctx, ptMode)"
	slotIdx := strings.Index(body, slotCall)
	gateIdx := strings.Index(body, gateAcquire)
	if slotIdx < 0 {
		t.Fatalf("runH3 missing waitCarrierDialSlot call: %s", slotCall)
	}
	if gateIdx < 0 {
		t.Fatalf("runH3 missing h3Gate.Acquire call")
	}
	if slotIdx > gateIdx {
		t.Fatalf("waitCarrierDialSlot must precede h3Gate.Acquire in runH3")
	}

	// Task B check: runH3 updates hadConnection after standDown
	if !strings.Contains(body, "hadConnection = reconnectFastPathAllowed(hadConnection, parked)") {
		t.Fatalf("runH3 does not call reconnectFastPathAllowed(hadConnection, parked)")
	}
}

// TestRunH3_CarrierDialSlot_Integration verifies that waitCarrierDialSlot is
// consulted in runH3, and when it returns false, runH3 returns promptly.
func TestRunH3_CarrierDialSlot_Integration(t *testing.T) {
	var limiterCalls atomic.Int32
	limiter := newCarrierDialLimiter(8, 16)
	limiter.waitForTest = func(ctx context.Context) bool {
		limiterCalls.Add(1)
		return false
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultPlatformTransportSettings()
	settings.carrierDialLimiterForTest = limiter

	strategy := NewClientStrategy(ctx, DefaultClientStrategySettings())
	transport := &PlatformTransport{
		log:                NewNoopLogger(),
		settings:           settings,
		auth:               &ClientAuth{},
		carrierDialLimiter: limiter,
		clientStrategy:     strategy,
		held:               NewMonitorValue(false),
		mode:               NewMonitorValue(TransportModeNone),
		kickMonitor:        NewMonitor(),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		transport.runH3(ctx, TransportModeH3, 0, 1)
	}()

	select {
	case <-done:
		// Succeeded in returning promptly
	case <-time.After(2 * time.Second):
		t.Fatalf("runH3 did not return promptly when waitCarrierDialSlot rejected")
	}

	if limiterCalls.Load() != 1 {
		t.Fatalf("expected waitCarrierDialSlot to be called exactly once, got %d", limiterCalls.Load())
	}
}

// TestCarrierDialLimiter_DefaultLimiterIsSharedAcrossTransports: two
// transports built with default settings share the one process-wide limiter,
// and draining it through one paces the other. The shared limiter's state is
// saved and restored so the drain cannot leak into other tests.
func TestCarrierDialLimiter_DefaultLimiterIsSharedAcrossTransports(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	newDefaultTransport := func() *PlatformTransport {
		transport := NewPlatformTransportWithTargetMode(
			ctx,
			NewClientStrategyWithDefaults(ctx),
			NewRouteManager(ctx, "carrier-dial"),
			"https://example.invalid",
			&ClientAuth{ByJwt: "testing", InstanceId: NewId(), AppVersion: "testing"},
			TransportModeH3,
			DefaultPlatformTransportSettings(),
		)
		t.Cleanup(transport.Close)
		return transport
	}
	t1 := newDefaultTransport()
	t2 := newDefaultTransport()
	if t1.carrierDialLimiter != defaultCarrierDialLimiter || t2.carrierDialLimiter != defaultCarrierDialLimiter {
		t.Fatalf("default transports must share defaultCarrierDialLimiter (got %p / %p, want %p)",
			t1.carrierDialLimiter, t2.carrierDialLimiter, defaultCarrierDialLimiter)
	}

	def := defaultCarrierDialLimiter
	def.mu.Lock()
	origTokens, origLast := def.tokens, def.lastTime
	def.mu.Unlock()
	t.Cleanup(func() {
		def.mu.Lock()
		def.tokens, def.lastTime = origTokens, origLast
		def.mu.Unlock()
	})

	// Exhaust the shared bucket through t1 (background transport activity may
	// have consumed some tokens already, so drain until the limiter reports a
	// wait), then confirm the same object paces t2. A past-dated now keeps
	// the refill out of the arithmetic.
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	exhausted := false
	for i := 0; i < int(def.burst)+2; i++ {
		if wait, _ := t1.carrierDialLimiter.reserve(now); wait > 0 {
			exhausted = true
			break
		}
	}
	if !exhausted {
		t.Fatal("the shared limiter never reported a wait after a full burst of reservations")
	}
	if wait, _ := t2.carrierDialLimiter.reserve(now); wait == 0 {
		t.Fatal("t2 was not paced after t1 exhausted the shared limiter: the default limiter is not shared")
	}
}

// TestCarrierDialLimiter_SettingsMinusOneDisables: CarrierDialRate=-1 wires a
// disabled limiter that admits immediately.
func TestCarrierDialLimiter_SettingsMinusOneDisables(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultPlatformTransportSettings()
	settings.CarrierDialRate = -1
	transport := NewPlatformTransportWithTargetMode(
		ctx,
		NewClientStrategyWithDefaults(ctx),
		NewRouteManager(ctx, "carrier-dial-off"),
		"https://example.invalid",
		&ClientAuth{ByJwt: "testing", InstanceId: NewId(), AppVersion: "testing"},
		TransportModeH3,
		settings,
	)
	t.Cleanup(transport.Close)
	if transport.carrierDialLimiter == nil || !transport.carrierDialLimiter.disabled {
		t.Fatalf("CarrierDialRate=-1 must wire a disabled limiter, got %+v", transport.carrierDialLimiter)
	}
	if !transport.carrierDialLimiter.Wait(ctx) {
		t.Fatal("a disabled limiter must admit immediately")
	}
}

// TestCarrierDialLimiter_CancelWhileWaitingRefundsOnce exercises the
// in-flight cancellation arm: a waiter sleeping on its timer returns false
// and refunds its token exactly once; a second refund is a no-op.
func TestCarrierDialLimiter_CancelWhileWaitingRefundsOnce(t *testing.T) {
	var mu sync.Mutex
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	limiter := newCarrierDialLimiterWithClock(8, 16, clock)
	for i := 0; i < 16; i++ {
		limiter.reserve(clock())
	}

	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()
	done := make(chan bool, 1)
	go func() { done <- limiter.Wait(ctx) }()

	// Wait until the goroutine has reserved its token (tokens go to -1) and
	// is sleeping on the real timer; the fake clock never advances, so only
	// the ctx arm can end the wait early.
	deadline := time.Now().Add(5 * time.Second)
	for {
		limiter.mu.Lock()
		tokens := limiter.tokens
		limiter.mu.Unlock()
		if tokens == -1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter never reserved a token")
		}
		time.Sleep(time.Millisecond)
	}
	cancelCtx()
	select {
	case admitted := <-done:
		if admitted {
			t.Fatal("Wait returned true after cancellation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after cancellation")
	}
	limiter.mu.Lock()
	tokens := limiter.tokens
	limiter.mu.Unlock()
	if tokens != 0 {
		t.Fatalf("tokens after the cancel refund = %v, want 0 (exactly one refund)", tokens)
	}

	// A second refund must be a no-op.
	wait, refund := limiter.reserve(clock())
	if wait != 125*time.Millisecond {
		t.Fatalf("probe wait = %v, want 125ms", wait)
	}
	refund()
	refund()
	limiter.mu.Lock()
	tokens = limiter.tokens
	limiter.mu.Unlock()
	if tokens != 0 {
		t.Fatalf("tokens after a double refund = %v, want 0 (no double refund)", tokens)
	}
}
