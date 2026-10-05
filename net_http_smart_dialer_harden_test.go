package connect

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// A transport the probes cannot reach must not keep every transport probed.
// Before the fix a never-succeeding dialer stayed under the sample floor, set
// "needed" on every round, and all dialers were probed again each tick; a dead
// proxy paid the full request timeout on all of them forever.
func TestProbeBlockedTransportDoesNotTriggerRoundsForTheOthers(t *testing.T) {
	withSmartDialer(t, true)

	fragment := &clientDialer{description: "fragment", priority: 0, minimumWeight: 0.25}
	normal := &clientDialer{description: "normal", priority: 25, minimumWeight: 0.5}
	reorder := &clientDialer{description: "reorder", priority: 30, minimumWeight: 0.5}
	strategy := probeStrategy(fragment, normal, reorder)
	costs := map[string]time.Duration{"fragment": 100 * time.Millisecond, "normal": 90 * time.Millisecond}

	// two rounds blocks "reorder" (it never connects)
	var calls atomic.Int32
	strategy.probeDialers(context.Background(), fixedProbe(costs, &calls))
	strategy.probeDialers(context.Background(), fixedProbe(costs, &calls))
	if !reorder.probeIsBlocked() {
		t.Fatalf("reorder failed two probes and should be blocked")
	}

	// everything else is measured and fresh: nothing may be probed now, even
	// though the blocked transport still has no samples
	before := calls.Load()
	if probed := strategy.probeDialers(context.Background(), fixedProbe(costs, &calls)); probed != 0 {
		t.Fatalf("probed %d dialers with only a blocked transport unmeasured, want 0", probed)
	}
	if calls.Load() != before {
		t.Fatalf("made %d probe calls for a blocked transport, want 0", calls.Load()-before)
	}
}

// A blocked transport is retried alone once its own window passes.
func TestProbeBlockedTransportIsRetriedAloneAfterItsWindow(t *testing.T) {
	withSmartDialer(t, true)

	fragment := testDialer("fragment", 0, 0.25, 3, 100*time.Millisecond, 0)
	reorder := &clientDialer{description: "reorder", priority: 30, minimumWeight: 0.5}
	strategy := probeStrategy(fragment, reorder)
	fragment.probeSuccesses = 3
	reorder.probeFailStreak = smartDialerBlockedAfter
	reorder.probeBlockedCount = 1
	reorder.probeBlockedUntil = time.Now().Add(-time.Minute)

	var probedNames []string
	probe := func(ctx context.Context, dialer *clientDialer) (bool, time.Duration, error) {
		probedNames = append(probedNames, dialer.description)
		return true, 70 * time.Millisecond, nil
	}
	if probed := strategy.probeDialers(context.Background(), probe); probed != 1 {
		t.Fatalf("probed %d, want only the blocked transport", probed)
	}
	if len(probedNames) == 0 || probedNames[0] != "reorder" {
		t.Fatalf("probed %v, want reorder", probedNames)
	}
	if reorder.probeIsBlocked() {
		t.Fatalf("a successful retry must clear the blocked verdict")
	}
}

// The penalty window doubles up to its cap.
func TestProbeBlockedPenaltyDoublesToACap(t *testing.T) {
	dialer := &clientDialer{description: "reorder", minimumWeight: 0.5}
	ctx := context.Background()
	var windows []time.Duration
	for i := 0; i < 8; i++ {
		dialer.probeFailed(ctx, errors.New("blocked"))
		if smartDialerBlockedAfter <= dialer.probeFailStreak {
			windows = append(windows, time.Until(dialer.probeBlockedUntil).Round(time.Hour))
		}
	}
	if windows[0] != smartDialerBlockedBaseAge {
		t.Fatalf("first window %v, want %v", windows[0], smartDialerBlockedBaseAge)
	}
	if last := windows[len(windows)-1]; last != smartDialerBlockedMaxAge {
		t.Fatalf("last window %v, want the cap %v", last, smartDialerBlockedMaxAge)
	}
}

// A probe that reaches the api must not put a transport that fails live
// traffic back in front.
func TestProbeSuccessDoesNotRestoreATransportThatFailsLiveTraffic(t *testing.T) {
	withSmartDialer(t, true)

	fragment := testDialer("fragment", 0, 0.25, 5, 200*time.Millisecond, 0)
	normal := testDialer("normal", 25, 0.5, 5, 80*time.Millisecond, 0)
	// normal then fails a live dial
	normal.Update(context.Background(), errors.New("reset"))
	if normal.hasMeasuredLatency() {
		t.Fatalf("a transport whose latest live attempt failed must not be measured")
	}
	normal.probeSucceeded(true, 60*time.Millisecond)
	if normal.hasMeasuredLatency() {
		t.Fatalf("a probe success restored a transport that failed live traffic")
	}
	if got := dialerOrder([]*clientDialer{fragment, normal}); got[0] != "fragment" {
		t.Fatalf("order = %v, want fragment first while normal is failing live", got)
	}
	// a live success is what restores it
	normal.Update(context.Background(), nil)
	if !normal.hasMeasuredLatency() {
		t.Fatalf("a live success should restore the transport")
	}
}

// A probe is proof a never-used transport connects, and does not count as a
// live success.
func TestProbeSuccessIsNotALiveSuccess(t *testing.T) {
	normal := &clientDialer{description: "normal", priority: 25, minimumWeight: 0.5}
	normal.probeSucceeded(true, 90*time.Millisecond)
	normal.probeSucceeded(true, 90*time.Millisecond)
	if stats := normal.Stats(); stats.successCount != 0 {
		t.Fatalf("probes recorded %d live successes, want 0", stats.successCount)
	}
	if !normal.hasMeasuredLatency() {
		t.Fatalf("two probe samples should make a never-used transport measured")
	}
}

// Connect cost jitters by far more than a handshake split costs, so a
// challenger inside the margin does not displace the first choice.
func TestOrderKeepsTheFirstChoiceInsideTheSwitchingMargin(t *testing.T) {
	withSmartDialer(t, true)

	fragment := testDialer("fragment", 0, 0.25, 4, 130*time.Millisecond, 0)
	normal := testDialer("normal", 25, 0.5, 4, 123*time.Millisecond, 0)
	if got := dialerOrder([]*clientDialer{fragment, normal}); got[0] != "fragment" {
		t.Fatalf("order = %v, want fragment kept first (7ms is inside the margin)", got)
	}

	// clearly faster (more than a fifth and more than 50ms) does displace it
	fast := testDialer("normal", 25, 0.5, 4, 60*time.Millisecond, 0)
	if got := dialerOrder([]*clientDialer{fragment, fast}); got[0] != "normal" {
		t.Fatalf("order = %v, want normal first when it is clearly faster", got)
	}

	// 25% faster but under the absolute gap is still inside the margin
	small := testDialer("fragment", 0, 0.25, 4, 40*time.Millisecond, 0)
	quick := testDialer("normal", 25, 0.5, 4, 28*time.Millisecond, 0)
	if got := dialerOrder([]*clientDialer{small, quick}); got[0] != "fragment" {
		t.Fatalf("order = %v, want fragment kept first (12ms gap is under the absolute floor)", got)
	}
}

func TestWeightsTreatACostInsideTheMarginAsEqual(t *testing.T) {
	withSmartDialer(t, true)

	fragment := testDialer("fragment", 0, 0.25, 4, 130*time.Millisecond, 0)
	normal := testDialer("normal", 25, 0.25, 4, 123*time.Millisecond, 0)
	weights := map[*clientDialer]float32{fragment: 1, normal: 1}
	applySmartDialerWeights(weights)
	if weights[fragment] != 1 || weights[normal] != 1 {
		t.Fatalf("weights = fragment %v normal %v, want both untouched inside the margin", weights[fragment], weights[normal])
	}
}

// While the ranking is stable the refresh age doubles up to a cap; any change
// puts it back to the base.
func TestProbeIntervalBacksOffWhileStableAndResetsOnChange(t *testing.T) {
	var state smartProbeState
	fragment := &clientDialer{description: "fragment"}
	normal := &clientDialer{description: "normal"}
	avgs := map[*clientDialer]time.Duration{fragment: 100 * time.Millisecond, normal: 120 * time.Millisecond}

	state.settle(fragment, avgs) // first round: nothing to compare to
	if state.interval != smartDialerProbeRefreshAge {
		t.Fatalf("first interval %v, want the base", state.interval)
	}
	want := smartDialerProbeRefreshAge
	for want < smartDialerProbeMaxAge {
		state.settle(fragment, map[*clientDialer]time.Duration{fragment: 105 * time.Millisecond, normal: 118 * time.Millisecond})
		want = min(want*2, smartDialerProbeMaxAge)
		if state.interval != want {
			t.Fatalf("interval %v, want %v", state.interval, want)
		}
	}
	// the cap holds
	state.settle(fragment, avgs)
	if state.interval != smartDialerProbeMaxAge {
		t.Fatalf("interval %v, want the cap %v", state.interval, smartDialerProbeMaxAge)
	}
	// a cost moving by more than the drift resets it
	state.settle(fragment, map[*clientDialer]time.Duration{fragment: 400 * time.Millisecond, normal: 118 * time.Millisecond})
	if state.interval != smartDialerProbeRefreshAge {
		t.Fatalf("interval %v after a large drift, want the base", state.interval)
	}
	// a leader change resets it too
	state.settle(fragment, map[*clientDialer]time.Duration{fragment: 400 * time.Millisecond, normal: 118 * time.Millisecond})
	state.settle(fragment, map[*clientDialer]time.Duration{fragment: 400 * time.Millisecond, normal: 118 * time.Millisecond})
	state.settle(normal, map[*clientDialer]time.Duration{fragment: 400 * time.Millisecond, normal: 118 * time.Millisecond})
	if state.interval != smartDialerProbeRefreshAge {
		t.Fatalf("interval %v after a leader change, want the base", state.interval)
	}
}

// Each strategy's refresh age is jittered once, within bounds, and stays put.
func TestProbeRefreshAgeIsJitteredOnceWithinBounds(t *testing.T) {
	var state smartProbeState
	first := state.refreshAge()
	low := time.Duration(float64(smartDialerProbeRefreshAge) * (1 - smartDialerProbeJitter))
	high := time.Duration(float64(smartDialerProbeRefreshAge) * (1 + smartDialerProbeJitter))
	if first < low || high < first {
		t.Fatalf("refresh age %v outside [%v, %v]", first, low, high)
	}
	if again := state.refreshAge(); again != first {
		t.Fatalf("refresh age changed from %v to %v; the jitter is fixed per strategy", first, again)
	}
}

// A leader that failed live traffic brings the next round forward even though
// its measurement is fresh.
func TestProbeLeaderFailureForcesTheNextRound(t *testing.T) {
	withSmartDialer(t, true)

	fragment := &clientDialer{description: "fragment", priority: 0, minimumWeight: 0.25}
	normal := &clientDialer{description: "normal", priority: 25, minimumWeight: 0.5}
	strategy := probeStrategy(fragment, normal)
	costs := map[string]time.Duration{"fragment": 100 * time.Millisecond, "normal": 60 * time.Millisecond}

	var calls atomic.Int32
	strategy.probeDialers(context.Background(), fixedProbe(costs, &calls))
	calls.Store(0)
	if probed := strategy.probeDialers(context.Background(), fixedProbe(costs, &calls)); probed != 0 {
		t.Fatalf("a fresh, stable picture was probed again (%d)", probed)
	}
	leader := strategy.smartProbe.leader
	if leader == nil {
		t.Fatalf("no leader recorded")
	}
	time.Sleep(2 * time.Millisecond)
	leader.Update(context.Background(), errors.New("reset"))
	if probed := strategy.probeDialers(context.Background(), fixedProbe(costs, &calls)); probed == 0 {
		t.Fatalf("a leader that failed live traffic did not bring the next round forward")
	}
}

// Behind SOCKS the reorder transports behave like their twins and are not
// worth measuring.
func TestProbeCollapsesSocksTwins(t *testing.T) {
	withSmartDialer(t, true)

	var dialers []*clientDialer
	for i, name := range []string{"fragment+reorder", "fragment", "normal", "reorder"} {
		dialers = append(dialers, &clientDialer{description: name, priority: i * 10, minimumWeight: 0.25})
	}
	strategy := probeStrategy(dialers...)
	var probedNames []string
	probe := func(ctx context.Context, dialer *clientDialer) (bool, time.Duration, error) {
		probedNames = append(probedNames, dialer.description)
		return true, 50 * time.Millisecond, nil
	}
	probed := strategy.probeDialersWith(context.Background(), probe, SmartDialerProbeOptions{CollapseSocksTwins: true})
	if probed != 2 {
		t.Fatalf("probed %d (%v), want fragment and normal only", probed, probedNames)
	}
	for _, name := range probedNames {
		if socksTwin(name) {
			t.Fatalf("probed twin %s", name)
		}
	}
}

// The api answering 429 or 5xx is overload, not a verdict on a transport: the
// round stops, nothing is marked failed, and probing stands down.
func TestProbeOverloadStopsTheRoundAndPausesWithoutBlaming(t *testing.T) {
	withSmartDialer(t, true)

	fragment := &clientDialer{description: "fragment", priority: 0, minimumWeight: 0.25}
	normal := &clientDialer{description: "normal", priority: 25, minimumWeight: 0.5}
	strategy := probeStrategy(fragment, normal)
	var calls atomic.Int32
	probe := func(ctx context.Context, dialer *clientDialer) (bool, time.Duration, error) {
		calls.Add(1)
		return false, 0, errProbeOverloaded
	}
	strategy.probeDialers(context.Background(), probe)
	if calls.Load() != 1 {
		t.Fatalf("made %d calls after the first overload answer, want 1", calls.Load())
	}
	if fragment.probeFailStreak != 0 || normal.probeFailStreak != 0 {
		t.Fatalf("overload was held against a transport")
	}
	if fragment.Stats().errorCount != 0 {
		t.Fatalf("overload was recorded as a dial error")
	}
	if probed := strategy.probeDialers(context.Background(), fixedProbe(map[string]time.Duration{
		"fragment": time.Millisecond, "normal": time.Millisecond,
	}, &calls)); probed != 0 {
		t.Fatalf("probing continued during the overload pause (%d)", probed)
	}
}

// The gate runs before every connect and a refusal ends the round without
// being held against the transport.
func TestProbeGateIsConsultedPerConnectAndEndsTheRound(t *testing.T) {
	withSmartDialer(t, true)

	fragment := &clientDialer{description: "fragment", priority: 0, minimumWeight: 0.25}
	normal := &clientDialer{description: "normal", priority: 25, minimumWeight: 0.5}
	strategy := probeStrategy(fragment, normal)
	costs := map[string]time.Duration{"fragment": 100 * time.Millisecond, "normal": 60 * time.Millisecond}

	var gated, released, probes atomic.Int32
	gate := func(ctx context.Context) (func(), error) {
		if 2 <= gated.Add(1) {
			return nil, errors.New("auth is busy")
		}
		return func() { released.Add(1) }, nil
	}
	strategy.probeDialersWith(context.Background(), func(ctx context.Context, dialer *clientDialer) (bool, time.Duration, error) {
		probes.Add(1)
		return fixedProbe(costs, nil)(ctx, dialer)
	}, SmartDialerProbeOptions{Gate: gate})
	if probes.Load() != 1 {
		t.Fatalf("made %d connects, want 1 (the gate refused the second)", probes.Load())
	}
	if released.Load() != 1 {
		t.Fatalf("released %d, want every granted slot released", released.Load())
	}
	if fragment.probeFailStreak != 0 || normal.probeFailStreak != 0 {
		t.Fatalf("a gate refusal was held against a transport")
	}
}

// The round report carries each transport's cost and status and the leader
// change, which is what the provider logs.
func TestProbeReportsTheRound(t *testing.T) {
	withSmartDialer(t, true)

	fragment := &clientDialer{description: "fragment", priority: 0, minimumWeight: 0.25}
	normal := &clientDialer{description: "normal", priority: 25, minimumWeight: 0.5}
	reorder := &clientDialer{description: "reorder", priority: 30, minimumWeight: 0.5}
	strategy := probeStrategy(fragment, normal, reorder)
	var report ProbeRound
	var reports int
	strategy.probeDialersWith(context.Background(), fixedProbe(map[string]time.Duration{
		"fragment": 300 * time.Millisecond, "normal": 100 * time.Millisecond,
	}, nil), SmartDialerProbeOptions{OnRound: func(round ProbeRound) { report = round; reports += 1 }})
	if reports != 1 {
		t.Fatalf("OnRound called %d times, want 1", reports)
	}
	if report.Leader != "normal" || report.PreviousLeader != "" {
		t.Fatalf("leader %q previous %q, want normal and none", report.Leader, report.PreviousLeader)
	}
	status := map[string]string{}
	for _, result := range report.Results {
		status[result.Transport] = result.Status
	}
	if status["fragment"] != "ok" || status["normal"] != "ok" || status["reorder"] != "fail" {
		t.Fatalf("statuses = %v", status)
	}
}

// A probe borrows the dialer's shared client only to clone its transport; a
// dialer never used for live traffic must not keep one afterwards.
func TestProbeDoesNotLeaveASharedClientBehindOnAnUnusedDialer(t *testing.T) {
	withSmartDialer(t, true)

	normal := &clientDialer{description: "normal", priority: 25, minimumWeight: 0.5, settings: DefaultClientStrategySettings()}
	strategy := probeStrategy(normal)
	strategy.probeDialers(context.Background(), func(ctx context.Context, dialer *clientDialer) (bool, time.Duration, error) {
		dialer.HttpClient() // what connectProbe does
		return true, 50 * time.Millisecond, nil
	})
	if normal.hasHttpClient() {
		t.Fatalf("the probe left a shared http client on a dialer with no live use")
	}
}
