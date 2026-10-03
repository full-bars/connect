package connect

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	mathrand "math/rand"
	"net"
	"net/http"
	"net/http/httptrace"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// Smart dialer: make the measured dial cost a signal in transport choice.
//
// The dialer set exists because networks differ. Fragmented and reordered TLS
// survive paths that block or fingerprint a plain handshake, and they cost time
// where no such path exists. This file adds an opt-in preference that reacts to
// the cost measured on the network this process is actually on.
//
// It is deliberately a preference, never a filter:
//
//   - Off by default. With the setting off, ordering and weights are exactly
//     what they were before this file existed.
//   - Only dialers with MEASURED successes are considered, and failures keep
//     their existing authority, so a transport that is blocked here still loses
//     to one that works however slow the working one is.
//   - The reference is the fastest dialer that works here, so a client with a
//     single working transport is never penalised for being the only option.
//   - Nothing is ever excluded: the fallback order for every other dialer is
//     preserved and weights stay at or above each dialer's minimum weight.
//   - Costs are exponential moving averages, so a path that recovers is
//     believed again without an operator doing anything.
var smartDialerEnabled atomic.Bool

const (
	// smartDialerMinSamples is how many connection establishments a transport
	// needs before its measured cost may influence anything. Below this the
	// existing order stands, so one cold or unlucky dial cannot reorder
	// anything. The count is deliberately small: a sample is a whole
	// connection establishment (a reused keep-alive request contributes
	// none), so a healthy client may produce only one sample per connection
	// lifetime, and a higher floor would leave the preference dormant on
	// exactly the healthy networks it exists to simplify.
	smartDialerMinSamples = 2
	// smartDialerLatencyFloor bounds how far a slower working transport's
	// weight can fall relative to a faster one. It is a preference, not a ban.
	smartDialerLatencyFloor = 0.25
)

// SetSmartDialer turns the measured-cost preference on or off and returns the
// previous value. Measurement is always running; this only decides whether
// selection consults it. Safe to call while requests are in flight.
func SetSmartDialer(enabled bool) bool {
	return smartDialerEnabled.Swap(enabled)
}

// SmartDialerEnabled reports whether the measured-cost preference is on.
func SmartDialerEnabled() bool {
	return smartDialerEnabled.Load()
}

// measuredLatency returns the dialer's average connection-establishment cost
// and how many establishments that average is built from.
func (self *clientDialer) measuredLatency() (time.Duration, int) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return time.Duration(self.connectLatencyNanos), self.connectSamples
}

// hasMeasuredLatency reports whether this dialer has both worked here and
// produced enough connection-establishment samples for its cost to mean
// anything. It also requires current health: a dialer whose most recent
// attempt failed does not get to lead the measured order or set the
// fastest-working reference, no matter how fast its older samples were.
// Failures always outrank latency.
//
// A transport the probes have found unreachable (blocked) never counts.
// Probe evidence and live evidence are kept apart: once live traffic has
// succeeded through the transport, only live traffic can restore it after a
// live failure, so a probe that happens to reach the api host cannot put a
// transport that fails real dials back at the front.
func (self *clientDialer) hasMeasuredLatency() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if self.connectSamples < smartDialerMinSamples || self.probeBlockedLocked(time.Now()) {
		return false
	}
	if 0 < self.successCount {
		return !self.lastSuccessTime.Before(self.lastErrorTime)
	}
	return 0 < self.probeSuccesses && !self.probeLastSuccess.Before(self.lastErrorTime)
}

// probeBlockedLocked reports whether the probes gave up on this transport and
// it is still inside its penalty window. The caller holds the dialer mutex.
func (self *clientDialer) probeBlockedLocked(now time.Time) bool {
	return smartDialerBlockedAfter <= self.probeFailStreak && now.Before(self.probeBlockedUntil)
}

// weightWithoutLatency is the dialer's score with latency left out. Upstream's
// Weight() is already exactly that (its success ratio floored at the dialer's
// minimum weight), so the measured-cost preference builds on it: a slow path
// is compared against the fastest path that works here rather than against an
// assumed constant.
func (self *clientDialer) weightWithoutLatency() float32 {
	return self.Weight()
}

// measuredSnapshot is one dialer's measured cost read once, so a sort or a
// scale never sees a value change under it.
type measuredSnapshot struct {
	dialer *clientDialer
	avg    time.Duration
}

func snapshotMeasured(dialers []*clientDialer) []measuredSnapshot {
	snapshots := make([]measuredSnapshot, 0, len(dialers))
	for _, dialer := range dialers {
		if !dialer.hasMeasuredLatency() {
			continue
		}
		avg, _ := dialer.measuredLatency()
		snapshots = append(snapshots, measuredSnapshot{dialer: dialer, avg: avg})
	}
	return snapshots
}

// beatsIncumbent reports whether a challenger's cost is low enough to displace
// the incumbent. Connect cost on a proxy path jitters by hundreds of
// milliseconds, far more than the few milliseconds a fragmented handshake
// costs, so a challenger has to be clearly faster (at least a fifth and at
// least smartDialerSwitchGap) or the order keeps the incumbent. Without this
// margin the leader flips between probe rounds on noise alone, and the
// first-choice transport loses the SNI split it exists to provide for no
// measurable gain.
func beatsIncumbent(challenger time.Duration, incumbent time.Duration) bool {
	if incumbent <= challenger {
		return false
	}
	return float64(challenger) <= smartDialerSwitchRatio*float64(incumbent) &&
		smartDialerSwitchGap <= incumbent-challenger
}

// orderDialersByMeasuredCost returns the attempt order when the measured-cost
// preference is on: dialers whose cost has been measured here come first,
// fastest first, and every other dialer keeps the order it was given. Nothing
// is dropped, so a network where the preferred transport is blocked simply
// fails over to the next one, and the blocked transport's error count removes
// it from the front by itself. The first dialer given (the configured
// first choice) keeps the front of the measured group unless another measured
// dialer beats it by the switching margin.
func orderDialersByMeasuredCost(dialers []*clientDialer) []*clientDialer {
	ordered := slices.Clone(dialers)

	snapshots := snapshotMeasured(ordered)
	avgs := make(map[*clientDialer]time.Duration, len(snapshots))
	for _, snapshot := range snapshots {
		avgs[snapshot.dialer] = snapshot.avg
	}
	measured := make([]*clientDialer, 0, len(snapshots))
	rest := make([]*clientDialer, 0, len(ordered))
	for _, dialer := range ordered {
		if _, ok := avgs[dialer]; ok {
			measured = append(measured, dialer)
		} else {
			rest = append(rest, dialer)
		}
	}
	incumbent := (*clientDialer)(nil)
	if 0 < len(ordered) {
		if _, ok := avgs[ordered[0]]; ok {
			incumbent = ordered[0]
		}
	}
	slices.SortStableFunc(measured, func(a *clientDialer, b *clientDialer) int {
		return cmp.Compare(avgs[a], avgs[b])
	})
	if incumbent != nil && 0 < len(measured) && measured[0] != incumbent && !beatsIncumbent(avgs[measured[0]], avgs[incumbent]) {
		// within the margin: the incumbent stays first, the rest keep cost order
		index := slices.Index(measured, incumbent)
		measured = slices.Delete(measured, index, index+1)
		measured = slices.Insert(measured, 0, incumbent)
	}
	return append(measured, rest...)
}

// applySmartDialerWeights scales parallel-eval weights by measured cost. Only
// dialers with measured successes are scaled, the scale is relative to the
// fastest dialer that works here, and no weight drops below its dialer's
// minimum, which keeps every transport reachable. A cost inside the switching
// margin of the fastest is treated as equal to it.
func applySmartDialerWeights(weights map[*clientDialer]float32) {
	dialers := make([]*clientDialer, 0, len(weights))
	for dialer := range weights {
		dialers = append(dialers, dialer)
	}
	snapshots := snapshotMeasured(dialers)
	var fastest time.Duration
	haveFastest := false
	for _, snapshot := range snapshots {
		if !haveFastest || snapshot.avg < fastest {
			fastest = snapshot.avg
			haveFastest = true
		}
	}
	if !haveFastest || fastest <= 0 {
		return
	}

	for _, snapshot := range snapshots {
		if snapshot.avg <= 0 {
			continue
		}
		factor := float32(float64(fastest) / float64(snapshot.avg))
		if !beatsIncumbent(fastest, snapshot.avg) {
			factor = 1
		}
		if factor < smartDialerLatencyFloor {
			factor = smartDialerLatencyFloor
		}
		if 1 < factor {
			factor = 1
		}
		scaled := weights[snapshot.dialer] * factor
		snapshot.dialer.mutex.Lock()
		floor := snapshot.dialer.minimumWeight
		snapshot.dialer.mutex.Unlock()
		if scaled < floor {
			scaled = floor
		}
		weights[snapshot.dialer] = scaled
	}
}

const (
	// smartDialerProbeRefreshAge is the BASE age of a measurement before a
	// probe retakes it. While the ranking stays stable the interval doubles
	// each round up to smartDialerProbeMaxAge: once a transport's cost on a
	// path is known it does not change on a half hour scale, and every probe
	// is a full TLS connect through the proxy to the api. Anything that
	// suggests the picture changed (the leader failing live, a cost moving
	// by more than smartDialerStableDrift, a different leader) puts it back
	// to the base age.
	smartDialerProbeRefreshAge = 30 * time.Minute
	smartDialerProbeMaxAge     = 24 * time.Hour
	// smartDialerStableDrift is how far a transport's average may move between
	// two rounds, as a fraction of its previous value, and still count as
	// unchanged.
	smartDialerStableDrift = 0.25
	// smartDialerProbeJitter spreads each strategy's refresh age by +/- this
	// fraction (chosen once per strategy), so a fleet that started together
	// does not go stale and probe together forever after.
	smartDialerProbeJitter = 0.2
	// smartDialerBlockedAfter consecutive probe failures with no probe success
	// between mark a transport blocked on this path. A blocked transport no
	// longer asks for a round on behalf of the others; it is retried on its own
	// schedule, smartDialerBlockedBaseAge doubling to smartDialerBlockedMaxAge.
	smartDialerBlockedAfter   = 2
	smartDialerBlockedBaseAge = 6 * time.Hour
	smartDialerBlockedMaxAge  = 24 * time.Hour
	// smartDialerOverloadPause is how long probing stands down after the api
	// answered a probe 429 or 5xx.
	smartDialerOverloadPause = 15 * time.Minute
	// switching margin, see beatsIncumbent
	smartDialerSwitchRatio = 0.8
	smartDialerSwitchGap   = 50 * time.Millisecond
)

// errProbeOverloaded is a probe the api answered 429 or 5xx. It is not
// evidence about the transport, only a signal to stop adding load.
var errProbeOverloaded = errors.New("smart dialer probe: api overloaded")

// dialProbe makes one connect-only attempt through a dialer and reports
// whether it established a fresh connection and how long that took.
type dialProbe func(ctx context.Context, dialer *clientDialer) (fresh bool, establish time.Duration, err error)

// ProbeTransportResult is one transport's outcome in a probe round.
type ProbeTransportResult struct {
	Transport string
	// Latency is the transport's average connect cost after the round; zero
	// when it has none.
	Latency time.Duration
	// Status is "ok", "fail" (this round's probe failed), "blocked" (gave up
	// on it for now) or "skipped".
	Status string
}

// ProbeRound describes one finished probe round for the caller's logs.
type ProbeRound struct {
	Results []ProbeTransportResult
	// Leader is the transport first in the measured order after the round,
	// PreviousLeader the one before it; empty when there was none.
	Leader         string
	PreviousLeader string
	// Overloaded is set when the round stopped early because the api answered
	// a probe 429 or 5xx.
	Overloaded bool
}

// SmartDialerProbeOptions tunes one probe call. The zero value probes every
// transport with no gate and reports nothing.
type SmartDialerProbeOptions struct {
	// Gate, when set, is called before every individual connect and may block
	// until probing is allowed (a global rate limit, or auth having been
	// quiet). The returned release is called when the connect finishes. A
	// non-nil error ends the round (the context ended, or probing is
	// withdrawn); it is not held against the transport.
	Gate func(ctx context.Context) (release func(), err error)
	// CollapseSocksTwins skips the transports that behave identically behind a
	// SOCKS proxy: there the connection is not a bare TCP connection, so
	// "reorder" sends a plain handshake exactly like "normal", and
	// "fragment+reorder" fragments exactly like "fragment". Measuring a twin
	// ranks two copies of one behavior against each other on noise.
	CollapseSocksTwins bool
	// OnRound, when set, receives the round's outcome after it finishes. It
	// is not called for a round that probed nothing.
	OnRound func(ProbeRound)
}

// smartProbeState is a strategy's probe schedule. The zero value is valid.
type smartProbeState struct {
	mutex sync.Mutex
	// interval is the current refresh age; zero means the base age
	interval time.Duration
	// jitter is this strategy's fixed refresh-age multiplier, set on first use
	jitter float64
	// leader and avgs are the previous round's result, used to judge stability
	leader *clientDialer
	avgs   map[*clientDialer]time.Duration
	// pausedUntil stands probing down after the api looked overloaded
	pausedUntil time.Time
}

// refreshAge is the age at which a measurement is retaken, jittered.
func (self *smartProbeState) refreshAge() time.Duration {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if self.jitter == 0 {
		self.jitter = 1 + smartDialerProbeJitter*(2*mathrand.Float64()-1)
	}
	interval := self.interval
	if interval < smartDialerProbeRefreshAge {
		interval = smartDialerProbeRefreshAge
	}
	return time.Duration(float64(interval) * self.jitter)
}

// settle records a finished round: a stable ranking doubles the interval, a
// changed one returns it to the base.
func (self *smartProbeState) settle(leader *clientDialer, avgs map[*clientDialer]time.Duration) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	stable := self.leader != nil && self.leader == leader && len(avgs) == len(self.avgs)
	if stable {
		for dialer, avg := range avgs {
			previous, ok := self.avgs[dialer]
			if !ok || previous <= 0 || smartDialerStableDrift*float64(previous) < math.Abs(float64(avg-previous)) {
				stable = false
				break
			}
		}
	}
	if stable {
		self.interval = min(max(self.interval, smartDialerProbeRefreshAge)*2, smartDialerProbeMaxAge)
	} else {
		self.interval = smartDialerProbeRefreshAge
	}
	self.leader = leader
	self.avgs = avgs
}

// leaderFailed reports whether the previous round's leader has since failed on
// live traffic, which is the one thing that should bring a probe round forward.
func (self *smartProbeState) leaderFailed() bool {
	self.mutex.Lock()
	leader := self.leader
	self.mutex.Unlock()
	if leader == nil {
		return false
	}
	leader.mutex.Lock()
	defer leader.mutex.Unlock()
	return leader.lastErrorTime.After(leader.lastSuccessTime) && leader.lastErrorTime.After(leader.connectObservedAt)
}

func (self *smartProbeState) paused(now time.Time) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return now.Before(self.pausedUntil)
}

func (self *smartProbeState) pause(now time.Time) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.pausedUntil = now.Add(smartDialerOverloadPause)
	self.interval = smartDialerProbeRefreshAge
}

// needsConnectProbe reports whether the smart dialer lacks a usable
// connect-cost measurement for this dialer: too few samples, or a measurement
// older than maxAge. A transport the probes marked blocked asks only once its
// own penalty window has passed, and never on behalf of the others.
func (self *clientDialer) needsConnectProbe(now time.Time, maxAge time.Duration) (needed bool, attempts int, blocked bool) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if smartDialerBlockedAfter <= self.probeFailStreak {
		return !now.Before(self.probeBlockedUntil), 1, true
	}
	if self.connectSamples < smartDialerMinSamples {
		return true, smartDialerMinSamples - self.connectSamples, false
	}
	return now.Sub(self.connectObservedAt) > maxAge, 1, false
}

// socksTwin reports whether the transport behaves identically to another one
// when the dialed connection is not a bare TCP connection.
func socksTwin(description string) bool {
	return description == "reorder" || description == "fragment+reorder"
}

// probeDialers gives the dialers a few connect-only attempts each when any of
// them lacks a current connect-cost measurement, so the measured-cost
// preference has something to compare.
//
// Why it exists: the preference only ranks dialers it has measured, but the
// highest-priority transport (fragment) is tried first and, wherever it
// works, keeps winning, so the alternatives (normal above all) are never
// dialed and never measured. On exactly the networks that need no DPI
// circumvention the preference then has no faster option to pick.
//
// When any working dialer needs a probe, EVERY working dialer is probed in the
// same round, the current leader included. Samples taken at different times
// against different hosts (a real dial during the startup storm against a probe
// afterwards) are not comparable, so the comparison has to be made on one
// basis. A transport the probes found blocked is not part of that round and
// cannot trigger one: it is retried alone on its own, much longer, schedule.
// Otherwise one unreachable transport would keep every transport probed every
// tick, and a dead proxy would pay the full request timeout on all of them.
//
// A successful probe becomes a sample (and proof the transport can connect)
// but is NOT recorded as a live success, so it cannot restore a transport that
// failed real traffic. A failed probe is recorded like a real dial failure
// only for a dialer that has never succeeded; either way it counts toward the
// blocked verdict. A probe never carries a live request, does nothing while
// the smart dialer is off (and stops as soon as it is turned off), and is
// skipped while custom extenders are configured, because then only the
// extenders are used. It returns how many dialers it attempted.
func (self *ClientStrategy) probeDialers(ctx context.Context, probe dialProbe) int {
	return self.probeDialersWith(ctx, probe, SmartDialerProbeOptions{})
}

func (self *ClientStrategy) probeDialersWith(ctx context.Context, probe dialProbe, options SmartDialerProbeOptions) int {
	if !SmartDialerEnabled() {
		return 0
	}
	now := time.Now()
	if self.smartProbe.paused(now) {
		return 0
	}

	self.mutex.Lock()
	if 0 < len(self.extenderIpSecrets) {
		self.mutex.Unlock()
		return 0
	}
	dialers := make([]*clientDialer, 0, len(self.dialers))
	for dialer := range self.dialers {
		dialers = append(dialers, dialer)
	}
	self.mutex.Unlock()
	// a stable order keeps the probe sequence predictable in logs and tests
	slices.SortFunc(dialers, func(a *clientDialer, b *clientDialer) int {
		return cmp.Or(cmp.Compare(a.priority, b.priority), cmp.Compare(a.description, b.description))
	})

	// extender dialers report their outcomes to the extender directory and
	// carry their own hold policy, and a dialer whose api client is not a plain
	// http.Transport (the http3 alternatives) cannot be probed on a private
	// keep-alive-free copy; neither is probed
	probeable := dialers[:0]
	for _, dialer := range dialers {
		if dialer.IsExtender() || !dialer.probeable() {
			continue
		}
		if options.CollapseSocksTwins && socksTwin(dialer.description) {
			continue
		}
		probeable = append(probeable, dialer)
	}
	dialers = probeable

	forced := self.smartProbe.leaderFailed()
	maxAge := self.smartProbe.refreshAge()
	if forced {
		maxAge = 0
	}
	anyWorkingNeeded := false
	needed := map[*clientDialer]bool{}
	attemptsFor := map[*clientDialer]int{}
	blockedSet := map[*clientDialer]bool{}
	for _, dialer := range dialers {
		due, attempts, blocked := dialer.needsConnectProbe(now, maxAge)
		attemptsFor[dialer] = attempts
		blockedSet[dialer] = blocked
		if due {
			needed[dialer] = true
			if !blocked {
				anyWorkingNeeded = true
			}
		}
	}
	if len(needed) == 0 {
		return 0
	}
	// a working dialer is probed whenever any working dialer is due (same
	// basis); a blocked one only when its own window has passed
	round := make([]*clientDialer, 0, len(dialers))
	for _, dialer := range dialers {
		switch {
		case blockedSet[dialer]:
			if needed[dialer] {
				round = append(round, dialer)
			}
		case anyWorkingNeeded:
			round = append(round, dialer)
		}
	}
	if len(round) == 0 {
		return 0
	}

	var previousLeader *clientDialer
	if leading := orderDialersByMeasuredCost(dialers); 0 < len(leading) && leading[0].hasMeasuredLatency() {
		previousLeader = leading[0]
	}

	results := make(map[*clientDialer]string, len(round))
	overloaded := false
	probed := 0
	for _, dialer := range round {
		if ctx.Err() != nil || !SmartDialerEnabled() {
			break
		}
		attempts := attemptsFor[dialer]
		if !needed[dialer] {
			attempts = 1
		}
		hadClient := dialer.hasHttpClient()

		attempted := false
		status := "skipped"
	attemptLoop:
		for i := 0; i < attempts && ctx.Err() == nil && SmartDialerEnabled(); i += 1 {
			release := func() {}
			if options.Gate != nil {
				var err error
				if release, err = options.Gate(ctx); err != nil {
					break attemptLoop
				}
			}
			fresh, establish, err := probe(ctx, dialer)
			release()
			if ctx.Err() != nil {
				// canceled mid-probe: an aborted attempt is not evidence
				break
			}
			attempted = true
			switch {
			case err == nil:
				dialer.probeSucceeded(fresh, establish)
				status = "ok"
			case errors.Is(err, errProbeOverloaded):
				overloaded = true
				break attemptLoop
			default:
				dialer.probeFailed(ctx, err)
				status = "fail"
				if dialer.probeIsBlocked() {
					status = "blocked"
				}
				// do not hammer a transport that cannot connect here
				break attemptLoop
			}
		}
		// the probe built the dialer's shared client only to borrow its
		// transport; a transport that was never used for live traffic should
		// not keep one alive
		if !hadClient {
			dialer.Close()
		}
		if attempted {
			probed += 1
			results[dialer] = status
		}
		if overloaded {
			break
		}
	}
	if overloaded {
		self.smartProbe.pause(time.Now())
	}

	leader := (*clientDialer)(nil)
	if leading := orderDialersByMeasuredCost(dialers); 0 < len(leading) && leading[0].hasMeasuredLatency() {
		leader = leading[0]
	}
	avgs := map[*clientDialer]time.Duration{}
	for _, snapshot := range snapshotMeasured(dialers) {
		avgs[snapshot.dialer] = snapshot.avg
	}
	if !overloaded {
		self.smartProbe.settle(leader, avgs)
	}

	if options.OnRound != nil && 0 < probed {
		report := ProbeRound{Overloaded: overloaded}
		if leader != nil {
			report.Leader = leader.description
		}
		if previousLeader != nil {
			report.PreviousLeader = previousLeader.description
		}
		for _, dialer := range dialers {
			status, ok := results[dialer]
			if !ok {
				status = "skipped"
			}
			avg, _ := dialer.measuredLatency()
			report.Results = append(report.Results, ProbeTransportResult{
				Transport: dialer.description,
				Latency:   avg,
				Status:    status,
			})
		}
		options.OnRound(report)
	}
	return probed
}

// probeSucceeded folds a successful probe in: a sample when the connection was
// fresh, proof the transport can connect, and a clean slate for the blocked
// verdict. It is deliberately not a live success.
func (self *clientDialer) probeSucceeded(fresh bool, establish time.Duration) {
	self.mutex.Lock()
	self.probeSuccesses += 1
	self.probeLastSuccess = time.Now()
	self.probeFailStreak = 0
	self.probeBlockedCount = 0
	self.probeBlockedUntil = time.Time{}
	self.mutex.Unlock()
	if fresh {
		self.observeConnect(establish)
	}
}

// probeFailed counts a failed probe toward the blocked verdict. A dialer with
// no live success also takes it as an ordinary failure, so a blocked
// transport still cannot lead; a dialer carrying live traffic is not demoted
// by one failed GET to the api host.
func (self *clientDialer) probeFailed(ctx context.Context, err error) {
	self.mutex.Lock()
	self.probeFailStreak += 1
	if smartDialerBlockedAfter <= self.probeFailStreak {
		self.probeBlockedCount += 1
		penalty := smartDialerBlockedBaseAge
		for i := 1; i < self.probeBlockedCount && penalty < smartDialerBlockedMaxAge; i += 1 {
			penalty *= 2
		}
		self.probeBlockedUntil = time.Now().Add(min(penalty, smartDialerBlockedMaxAge))
	}
	neverWorked := self.successCount == 0
	self.mutex.Unlock()
	if neverWorked {
		self.Update(ctx, err)
	}
}

func (self *clientDialer) probeIsBlocked() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return smartDialerBlockedAfter <= self.probeFailStreak
}

func (self *clientDialer) hasHttpClient() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.httpClient != nil
}

// ProbeDialers measures the connect cost of every transport that lacks a
// current measurement by making connect-only requests to probeUrl (use the
// api url). Call it at startup and periodically while the smart dialer is on;
// a call with nothing due does no network work. It returns how many
// transports it attempted; zero when the smart dialer is off or every
// transport is already measured.
func (self *ClientStrategy) ProbeDialers(ctx context.Context, probeUrl string) int {
	return self.ProbeDialersWith(ctx, probeUrl, SmartDialerProbeOptions{})
}

// ProbeDialersWith is ProbeDialers with a gate, twin collapsing and a report.
func (self *ClientStrategy) ProbeDialersWith(ctx context.Context, probeUrl string, options SmartDialerProbeOptions) int {
	return self.probeDialersWith(ctx, func(ctx context.Context, dialer *clientDialer) (bool, time.Duration, error) {
		return connectProbe(ctx, dialer, probeUrl)
	}, options)
}

// probeable reports whether the dialer's api client is a plain http.Transport,
// which is what connectProbe clones.
func (self *clientDialer) probeable() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if self.httpClientFactory != nil {
		return false
	}
	if self.httpClient != nil {
		_, ok := self.httpClient.Transport.(*http.Transport)
		return ok
	}
	// not built yet: HttpClient builds a plain http.Transport unless a factory
	// is set, and a factory was ruled out above
	return true
}

// connectProbe does one GET through the dialer on a connection of its own.
// The dialer's shared client keeps connections alive, and a request served by
// a kept-alive connection dials nothing, so it would report reuse and produce
// no sample; a private transport with keep-alives off always dials. An api
// that answers 429 or 5xx is overloaded, which is no verdict on the transport
// and a reason to stop adding load.
func connectProbe(ctx context.Context, dialer *clientDialer, probeUrl string) (bool, time.Duration, error) {
	base, ok := dialer.HttpClient().Transport.(*http.Transport)
	if !ok {
		return false, 0, fmt.Errorf("dialer %s has no http transport to probe", dialer.description)
	}
	transport := base.Clone()
	transport.DisableKeepAlives = true
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		Timeout:   dialer.settings.RequestTimeout,
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, probeUrl, nil)
	if err != nil {
		return false, 0, err
	}
	timing := &dialTiming{}
	start := time.Now()
	response, err := client.Do(timing.trace(ctx, request, start))
	if err != nil {
		return false, 0, err
	}
	io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	response.Body.Close()
	if response.StatusCode == http.StatusTooManyRequests || 500 <= response.StatusCode {
		return false, 0, errProbeOverloaded
	}

	fresh, establish := timing.sample()
	return fresh, establish, nil
}

// dialerStats is a thread-safe snapshot of a dialer's outcome counters.
type dialerStats struct {
	successCount uint64
	errorCount   uint64
}

// Stats returns a thread-safe snapshot of the dialer's outcome counters.
func (self *clientDialer) Stats() dialerStats {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return dialerStats{
		successCount: self.successCount,
		errorCount:   self.errorCount,
	}
}

// observeConnect folds one connection-establishment cost into the average the
// smart dialer orders by (alpha = 0.3). Call it only for a genuine dial: a
// request served from a reused keep-alive connection performed no dial, and
// folding its fast round trip in would drift the average toward request-only
// cost and stop reflecting the front-loaded work a DPI-circumvention transport
// pays (fragmented or reordered TLS is set up at connect, not on every request
// an open connection carries).
func (self *clientDialer) observeConnect(duration time.Duration) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	const latencyAlpha = 0.3
	nanos := duration.Nanoseconds()
	if self.connectSamples == 0 {
		self.connectLatencyNanos = nanos
	} else {
		self.connectLatencyNanos = int64(latencyAlpha*float64(nanos) + (1-latencyAlpha)*float64(self.connectLatencyNanos))
	}
	self.connectSamples += 1
	self.connectObservedAt = time.Now()
}

// dialTiming observes one HTTP attempt's connection lifecycle. It reports
// whether the attempt actually dialed (obtained a fresh connection) and, if
// so, how long establishing that connection took. A request served from a
// reused keep-alive connection performs no dial, so its (fast) round trip
// must not enter the dial-cost average: folding it in would drift the
// average toward request-only cost and stop reflecting the front-loaded
// work a DPI-circumvention transport pays (fragmented or reordered TLS is
// set up at connect, not on every request an open connection carries).
type dialTiming struct {
	mutex         sync.Mutex
	fresh         bool
	establishTime time.Duration
}

// trace attaches an httptrace to the request that records whether the
// connection is fresh and how long its establishment took, measured from
// start. Callbacks may fire on transport goroutines, so state is guarded.
func (self *dialTiming) trace(ctx context.Context, request *http.Request, start time.Time) *http.Request {
	return request.WithContext(self.traceCtx(ctx, start))
}

// traceCtx attaches an httptrace to a context that records whether the
// connection established is fresh and how long its establishment took,
// measured from start. Used by dialers that take a context directly (the
// WebSocket path); gorilla's DialContext wires the context through to the
// transport, which fires ClientTrace callbacks on it.
func (self *dialTiming) traceCtx(ctx context.Context, start time.Time) context.Context {
	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if info.Reused {
				return
			}
			self.mutex.Lock()
			self.fresh = true
			self.establishTime = time.Since(start)
			self.mutex.Unlock()
		},
	}
	return httptrace.WithClientTrace(ctx, trace)
}

// sample returns whether a fresh connection was established and, if so, how
// long that establishment took.
func (self *dialTiming) sample() (bool, time.Duration) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.fresh, self.establishTime
}

// observe records the dial cost of a finished attempt for the smart dialer:
// only a successful attempt that established a fresh connection is a sample.
func (self *dialTiming) observe(dialer *clientDialer, err error) {
	if err != nil {
		return
	}
	if fresh, establish := self.sample(); fresh {
		dialer.observeConnect(establish)
	}
}

// wrapDialer returns a copy of the websocket dialer whose TLS dial records how
// long the connection took to establish (TCP and TLS, the same span the
// httptrace hook measures for the http and websocket paths). The H1+ dial
// builds its own connection through this function rather than through
// gorilla's trace hook, so it has to be timed here. The dialer is copied
// because strategy dialers are cached and shared by concurrent attempts. The
// last successful dial wins: when the framed upgrade fails and the dial falls
// back to a websocket on a new connection, that connection is the one kept.
func (self *dialTiming) wrapDialer(dialer *websocket.Dialer) *websocket.Dialer {
	attempt := *dialer
	if base := dialer.NetDialTLSContext; base != nil {
		attempt.NetDialTLSContext = func(ctx context.Context, network string, address string) (net.Conn, error) {
			start := time.Now()
			conn, err := base(ctx, network, address)
			if err == nil {
				self.mutex.Lock()
				self.fresh = true
				self.establishTime = time.Since(start)
				self.mutex.Unlock()
			}
			return conn, err
		}
	}
	return &attempt
}
