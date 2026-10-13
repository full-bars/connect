package connect

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func identityJitter(d time.Duration) time.Duration { return d }

func newTestFlapGuard(stableAfter time.Duration, base time.Duration) *flapGuard {
	g := newFlapGuard(stableAfter, base)
	g.jitter = identityJitter
	return g
}

func TestFlapGuard_StableSessionNeedsNoExtraWait(t *testing.T) {
	g := newTestFlapGuard(60*time.Second, 5*time.Second)
	if got := g.sessionEnded(2 * time.Hour); got != 0 {
		t.Fatalf("a session that lived 2h must reconnect without extra wait, got %s", got)
	}
}

func TestFlapGuard_YoungSessionsEscalate(t *testing.T) {
	g := newTestFlapGuard(60*time.Second, 5*time.Second)
	want := []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second, 60 * time.Second, 60 * time.Second}
	for i, w := range want {
		if got := g.sessionEnded(3 * time.Second); got != w {
			t.Fatalf("young session %d: wait %s, want %s", i, got, w)
		}
	}
}

func TestFlapGuard_StableSessionClearsStreak(t *testing.T) {
	g := newTestFlapGuard(60*time.Second, 5*time.Second)
	g.sessionEnded(time.Second)
	g.sessionEnded(time.Second)
	if got := g.sessionEnded(61 * time.Second); got != 0 {
		t.Fatalf("stable session must not wait, got %s", got)
	}
	if got := g.sessionEnded(time.Second); got != 5*time.Second {
		t.Fatalf("after a stable session the streak starts over at the base, got %s", got)
	}
}

func TestFlapGuard_SessionJustUnderThresholdCounts(t *testing.T) {
	g := newTestFlapGuard(60*time.Second, 5*time.Second)
	if got := g.sessionEnded(60*time.Second - time.Nanosecond); got == 0 {
		t.Fatal("a session shorter than the stable threshold is a flap")
	}
	if got := g.sessionEnded(60 * time.Second); got != 0 {
		t.Fatalf("a session exactly at the threshold is stable, got %s", got)
	}
}

func TestFlapGuard_UnsetBaseFallsBackToOneSecond(t *testing.T) {
	g := newTestFlapGuard(60*time.Second, 0)
	if got := g.sessionEnded(time.Second); got != time.Second {
		t.Fatalf("unset base must not make a tight loop, got %s", got)
	}
}

func TestFlapGuard_NegativeStableAfterDisables(t *testing.T) {
	g := newTestFlapGuard(-1, 5*time.Second)
	for i := 0; i < 5; i++ {
		if got := g.sessionEnded(time.Millisecond); got != 0 {
			t.Fatalf("disabled guard must never wait, got %s", got)
		}
	}
}

func TestFlapGuard_JitterStaysWithinBand(t *testing.T) {
	g := newFlapGuard(60*time.Second, 10*time.Second)
	for i := 0; i < 200; i++ {
		g2 := newFlapGuard(60*time.Second, 10*time.Second)
		got := g2.sessionEnded(time.Second)
		if got < 8*time.Second || got > 12*time.Second {
			t.Fatalf("first wait %s outside the +-20%% band around 10s", got)
		}
	}
	_ = g
}

func TestSessionEnd_CountsEngagedBackoffs(t *testing.T) {
	defer setGovernModeForTest(governEnforce)()
	before := TransportFlapBackoffs()
	g := newTestFlapGuard(60*time.Second, time.Second)
	log := loggerOrDefault(nil)
	sessionEndWait(context.Background(), g, time.Hour, false, "c", log, true)
	sessionEndWait(context.Background(), g, time.Second, false, "c", log, true)
	sessionEndWait(context.Background(), g, time.Second, false, "c", log, true)
	if got := TransportFlapBackoffs() - before; got != 2 {
		t.Fatalf("expected 2 engaged backoffs counted, got %d", got)
	}
}

// flapServer upgrades every websocket and holds it for hold before closing, so
// a client sees a connect that succeeds and then dies.
func flapServer(hold time.Duration, accepts *atomic.Int64, stamps *flapStamps) *httptest.Server {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// only a completed websocket upgrade is a connect: the transport may first
		// offer a framed upgrade, which a plain websocket server refuses
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		accepts.Add(1)
		stamps.add(time.Now())
		if 0 < hold {
			time.Sleep(hold)
		}
		ws.Close()
	}))
}

type flapStamps struct {
	mu sync.Mutex
	at []time.Time
}

func (s *flapStamps) add(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.at = append(s.at, t)
}

func (s *flapStamps) snapshot() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.at...)
}

func newFlapTestTransport(t *testing.T, ctx context.Context, serverUrl string, settings *PlatformTransportSettings) *PlatformTransport {
	t.Helper()
	// the default strategy spaces every connect 100-1000ms apart, which would
	// mask the loop's own behavior; these tests are about the loop, not that pacing
	strategySettings := DefaultClientStrategySettings()
	strategySettings.MinNextConnectDelay = 0
	strategySettings.MaxNextConnectDelay = 0
	// a failed dial otherwise holds its attempt open for the full 15s request
	// timeout; a local server answers in microseconds
	strategySettings.RequestTimeout = 500 * time.Millisecond
	strategy := NewClientStrategy(ctx, strategySettings)
	routeManager := NewRouteManager(ctx, "test-flap")
	auth := &ClientAuth{
		ByJwt:      "jwt",
		InstanceId: NewId(),
		AppVersion: "1.0.0",
	}
	return NewPlatformTransportWithTargetMode(ctx, strategy, routeManager, "ws"+strings.TrimPrefix(serverUrl, "http"), auth, TransportModeH1, settings)
}

// The incident shape: the backend accepts the websocket and the session dies
// seconds later. Before the guard each death reconnected as fast as the
// ReconnectTimeout jitter allowed, forever.
func TestRunH1_FlappingSessionBacksOff(t *testing.T) {
	defer setGovernModeForTest(governEnforce)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var accepts atomic.Int64
	var stamps flapStamps
	server := flapServer(0, &accepts, &stamps)
	defer server.Close()

	settings := DefaultPlatformTransportSettings()
	settings.ReconnectTimeout = 20 * time.Millisecond
	settings.FlapStableAfter = 10 * time.Second

	transport := newFlapTestTransport(t, ctx, server.URL, settings)
	defer transport.Close()

	time.Sleep(2 * time.Second)

	// waits of ~20, 40, 80, 160, 320, 640, 1280 ms: at most 8 connects in 2s.
	// an unguarded loop reconnects every few ms and lands in the hundreds.
	if n := accepts.Load(); n > 12 {
		t.Fatalf("flapping sessions were not backed off: %d connects in 2s", n)
	}
	if n := accepts.Load(); n < 3 {
		t.Fatalf("guard over-throttled, only %d connects in 2s", n)
	}
}

// A healthy session ends rarely and long after it began. The guard must not add
// any wait there: this is the path every hot restart and platform deploy takes.
// The transport paces its own reconnects and races its dialers, so the absolute
// gap between connects is not the guard's to promise; what it must not do is
// make stable sessions reconnect slower than the same loop with the guard off.
func TestRunH1_StableSessionReconnectsPromptly(t *testing.T) {
	defer setGovernModeForTest(governEnforce)()

	connectsWith := func(stableAfter time.Duration) int {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var accepts atomic.Int64
		var stamps flapStamps
		server := flapServer(300*time.Millisecond, &accepts, &stamps)
		defer server.Close()

		settings := DefaultPlatformTransportSettings()
		settings.ReconnectTimeout = 20 * time.Millisecond
		settings.FlapStableAfter = stableAfter

		transport := newFlapTestTransport(t, ctx, server.URL, settings)
		defer transport.Close()
		time.Sleep(3 * time.Second)
		return len(stamps.snapshot())
	}

	// every session lives 300ms, which is stable by a 100ms threshold; a negative
	// threshold turns the guard off
	guarded, unguarded := connectsWith(100*time.Millisecond), connectsWith(-1)
	if unguarded < 4 {
		t.Fatalf("only %d connects with the guard off, test setup is not exercising the loop", unguarded)
	}
	if guarded < unguarded*3/4 {
		t.Fatalf("stable sessions reconnected slower with the guard on: %d connects against %d with it off", guarded, unguarded)
	}
}

// The loop feeds the process-wide governor: every attempt is classified, and a
// connect whose session dies young is recorded as an early death.
func TestRunH1_FeedsTheGovernor(t *testing.T) {
	defer setGovernModeForTest(governEnforce)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var accepts atomic.Int64
	var stamps flapStamps
	server := flapServer(0, &accepts, &stamps)
	defer server.Close()

	before := connectGov.snapshot()

	settings := DefaultPlatformTransportSettings()
	settings.ReconnectTimeout = 20 * time.Millisecond
	settings.FlapStableAfter = 10 * time.Second

	transport := newFlapTestTransport(t, ctx, server.URL, settings)
	defer transport.Close()

	time.Sleep(1500 * time.Millisecond)

	after := connectGov.snapshot()
	connects := accepts.Load()
	if connects < 3 {
		t.Fatalf("only %d connects, test setup is not exercising the loop", connects)
	}
	// the dial may race several dialers and discard the losers, whose upgrades the
	// server still counts, so the server accepts are an upper bound on connects
	ok := after.OK - before.OK
	if ok < 3 || connects < ok {
		t.Fatalf("governor saw %d connected attempts for %d server accepts", ok, connects)
	}
	if got := after.Early - before.Early; got < ok-2 {
		t.Fatalf("governor saw %d early deaths for %d young sessions", got, ok)
	}
}

// An attempt that fails is classified, not silently dropped.
func TestRunH1_FailedAttemptsReachTheGovernor(t *testing.T) {
	defer setGovernModeForTest(governEnforce)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var accepts atomic.Int64
	var stamps flapStamps
	server := flapServer(0, &accepts, &stamps)
	url := server.URL
	server.Close() // nothing listens any more: every dial is refused

	before := connectGov.snapshot()

	settings := DefaultPlatformTransportSettings()
	settings.ReconnectTimeout = 20 * time.Millisecond

	transport := newFlapTestTransport(t, ctx, url, settings)
	defer transport.Close()

	deadline := time.After(10 * time.Second)
	for {
		after := connectGov.snapshot()
		if 2 <= after.Attempts-before.Attempts {
			if after.OK != before.OK {
				t.Fatal("refused dials must not count as connected")
			}
			return
		}
		select {
		case <-deadline:
			t.Fatalf("governor saw %d attempts from a refusing server", after.Attempts-before.Attempts)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// A session cut short by our own teardown (a reload, the reaper, shutdown) says
// nothing about the backend and must not count as one that died young.
func TestRunH1_LocalTeardownIsNotAnEarlyDeath(t *testing.T) {
	defer setGovernModeForTest(governEnforce)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var accepts atomic.Int64
	var stamps flapStamps
	server := flapServer(time.Hour, &accepts, &stamps) // holds the session open
	defer server.Close()

	settings := DefaultPlatformTransportSettings()
	settings.ReconnectTimeout = 20 * time.Millisecond
	settings.FlapStableAfter = time.Hour // any session counts as young

	before := connectGov.snapshot()
	transport := newFlapTestTransport(t, ctx, server.URL, settings)

	deadline := time.After(10 * time.Second)
	for accepts.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("the transport never connected")
		case <-time.After(10 * time.Millisecond):
		}
	}
	time.Sleep(100 * time.Millisecond)
	transport.Close() // local teardown of a live session
	time.Sleep(500 * time.Millisecond)

	if got := connectGov.snapshot().Early - before.Early; got != 0 {
		t.Fatalf("a locally cancelled session was counted as dying young (%d)", got)
	}
}

// The end-of-session rules are shared by the H1 and H3 loops through one helper,
// so that the two cannot drift apart.
func TestSessionEnd_YoungSessionIsHeldAndCountedOnce(t *testing.T) {
	defer setGovernModeForTest(governEnforce)()
	flaps := newFlapGuard(time.Minute, time.Second)
	flaps.jitter = func(d time.Duration) time.Duration { return d }
	ctx := context.Background()
	before := connectGov.snapshot()
	wait := sessionEndWait(ctx, flaps, 3*time.Second, false, "c", loggerOrDefault(nil), true)
	if wait != time.Second {
		t.Fatalf("first young session wait = %v, want the base", wait)
	}
	if got := connectGov.snapshot().Early - before.Early; got != 1 {
		t.Fatalf("early deaths counted = %d, want 1", got)
	}
}

func TestSessionEnd_StableSessionHasNoWaitAndNoEarlyDeath(t *testing.T) {
	defer setGovernModeForTest(governEnforce)()
	flaps := newFlapGuard(time.Minute, time.Second)
	before := connectGov.snapshot()
	if wait := sessionEndWait(context.Background(), flaps, 5*time.Minute, false, "c", loggerOrDefault(nil), true); wait != 0 {
		t.Fatalf("stable session wait = %v, want 0", wait)
	}
	if got := connectGov.snapshot().Early - before.Early; got != 0 {
		t.Fatalf("a stable session counted as an early death (%d)", got)
	}
}

func TestSessionEnd_LocalTeardownAndAuxiliaryAreNotEarlyDeaths(t *testing.T) {
	defer setGovernModeForTest(governEnforce)()
	flaps := newFlapGuard(time.Minute, time.Second)
	flaps.jitter = func(d time.Duration) time.Duration { return d }
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	before := connectGov.snapshot()
	sessionEndWait(cancelled, flaps, time.Second, false, "c", loggerOrDefault(nil), true)
	// an auxiliary H3 beside H1 is never a health signal, but is still held back
	if wait := sessionEndWait(context.Background(), flaps, time.Second, false, "c", loggerOrDefault(nil), false); wait == 0 {
		t.Fatal("an auxiliary transport still must not redial in a tight loop")
	}
	if got := connectGov.snapshot().Early - before.Early; got != 0 {
		t.Fatalf("teardown or an auxiliary transport counted as an early death (%d)", got)
	}
}

// A session our own teardown cut short (a network change, the owner's hold) is
// not the backend dropping connections: no early death, no wait.
func TestSessionEnd_CutShortSessionIsNotAnEarlyDeath(t *testing.T) {
	defer setGovernModeForTest(governEnforce)()
	flaps := newFlapGuard(time.Minute, time.Second)
	flaps.jitter = func(d time.Duration) time.Duration { return d }
	before := connectGov.snapshot()
	if wait := sessionEndWait(context.Background(), flaps, time.Second, true, "c", loggerOrDefault(nil), true); wait != 0 {
		t.Fatalf("a cut short session was held %v", wait)
	}
	if got := connectGov.snapshot().Early - before.Early; got != 0 {
		t.Fatalf("a cut short session counted as an early death (%d)", got)
	}
}

// Shadow observes and reports: the early death is counted and the wait that
// enforce would take is counted, but nothing is held.
func TestSessionEnd_ShadowCountsButNeverHolds(t *testing.T) {
	defer setGovernModeForTest(governEnforce)()
	defer setGovernModeForTest(governShadow)()
	flaps := newFlapGuard(time.Minute, time.Second)
	flaps.jitter = func(d time.Duration) time.Duration { return d }
	before := connectGov.snapshot()
	backoffs, wouldHold := TransportFlapBackoffs(), TransportFlapWouldHold()
	if wait := sessionEndWait(context.Background(), flaps, 3*time.Second, false, "c", loggerOrDefault(nil), true); wait != 0 {
		t.Fatalf("shadow held a reconnect %v", wait)
	}
	if got := connectGov.snapshot().Early - before.Early; got != 1 {
		t.Fatalf("shadow must still count the early death, got %d", got)
	}
	if TransportFlapWouldHold()-wouldHold != 1 || TransportFlapBackoffs() != backoffs {
		t.Fatal("shadow must count a would-hold and no engaged backoff")
	}
}

// Off disables the guard and the governor's observation alike.
func TestSessionEnd_OffDoesNothing(t *testing.T) {
	defer setGovernModeForTest(governEnforce)()
	defer setGovernModeForTest(governOff)()
	flaps := newFlapGuard(time.Minute, time.Second)
	before := connectGov.snapshot()
	if wait := sessionEndWait(context.Background(), flaps, time.Second, false, "c", loggerOrDefault(nil), true); wait != 0 {
		t.Fatalf("off held a reconnect %v", wait)
	}
	if got := connectGov.snapshot().Early - before.Early; got != 0 {
		t.Fatalf("off counted an early death (%d)", got)
	}
}
