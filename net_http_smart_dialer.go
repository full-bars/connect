package connect

import (
	"cmp"
	"slices"
	"sync/atomic"
	"time"
)

// Smart dialer: latency-aware transport preference.
//
// The dialer set exists because different networks need different transports.
// Fragmented/reordered TLS survives paths that block or fingerprint a plain
// handshake, and it costs time where no such path exists. Selection has always
// been blind to that cost: a dialer that always succeeds but takes seconds
// holds the same weight as one that answers in a fraction of it.
//
// Smart dialer makes the measured cost a signal, within strict limits:
//
//   - It is off unless an operator turns it on (the provider exposes it as a
//     live control-socket setting). With it off, ordering and weights are
//     exactly as they were.
//   - It only ever REFINES the choice among dialers that have measured
//     successes. Failures keep their existing authority, so a transport that is
//     blocked on this network still loses to one that works, however slow the
//     working transport is.
//   - The reference is the fastest dialer that WORKS here, so a client with a
//     single working transport is unaffected (its own cost is its own
//     reference).
//   - Nothing is ever excluded: the static priority order is preserved for
//     unmeasured dialers, and weights stay at or above each dialer's
//     minimumWeight.
//   - The measurement is an exponential moving average, so a path that
//     recovers is believed again without an operator doing anything.
var smartDialerEnabled atomic.Bool

const (
	// smartDialerMinSamples is how many connection establishments a dialer
	// needs before its measured cost may influence anything. Below this,
	// static priority stands, so one cold or unlucky connect cannot reorder
	// anything. The count is deliberately small: a sample is a whole
	// connection establishment (a reused keep-alive request contributes
	// none), so a healthy client may produce only one sample per connection
	// lifetime, and a higher floor would leave the preference dormant on
	// exactly the healthy networks it exists to simplify.
	smartDialerMinSamples = 2
	// smartDialerLatencyFloor bounds how far a slower working dialer's weight
	// can fall relative to a faster one. It is a preference, not a ban: the
	// slow transport keeps a quarter of the faster one's pull and remains
	// reachable in the serial order, which is what a censored network needs.
	smartDialerLatencyFloor = 0.25
)

// SetSmartDialer turns latency-aware dialer preference on or off and returns
// the previous value. It is safe to call while requests are in flight: the
// measurement is always running, and this only decides whether the selection
// consults it.
func SetSmartDialer(enabled bool) bool {
	return smartDialerEnabled.Swap(enabled)
}

// SmartDialerEnabled reports whether latency-aware dialer preference is on.
func SmartDialerEnabled() bool {
	return smartDialerEnabled.Load()
}

// measuredLatency returns the dialer's average connection-establishment cost
// and how many samples that average is built from.
func (self *clientDialer) measuredLatency() (time.Duration, int) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return time.Duration(self.connectLatencyNanos), self.connectSamples
}

// hasMeasuredLatency reports whether this dialer has both worked here and
// produced enough samples for its cost to mean anything. It also requires
// current health: a dialer whose most recent attempt failed does not get to
// lead the measured order or set the fastest-working reference, no matter how
// fast its older samples were. Failures always outrank latency.
func (self *clientDialer) hasMeasuredLatency() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.connectSamples >= smartDialerMinSamples && 0 < self.successCount && !self.lastSuccessTime.Before(self.lastErrorTime)
}

// orderSerialDialers returns the order to attempt dialers in. With smart
// dialing off this is the static priority order. With it on, dialers whose cost
// has been measured on this network come first — fastest first — and everything
// else keeps the static order behind them. Nothing is dropped: a network where
// the preferred transport is blocked simply fails over to the next one, and the
// blocked transport's error count removes it from the front on its own.
func orderSerialDialers(dialers []*clientDialer) []*clientDialer {
	ordered := slices.Clone(dialers)
	if !SmartDialerEnabled() {
		slices.SortStableFunc(ordered, func(a *clientDialer, b *clientDialer) int {
			return a.priority - b.priority
		})
		return ordered
	}

	measured := make([]*clientDialer, 0, len(ordered))
	rest := make([]*clientDialer, 0, len(ordered))
	for _, dialer := range ordered {
		if dialer.hasMeasuredLatency() {
			measured = append(measured, dialer)
		} else {
			rest = append(rest, dialer)
		}
	}
	slices.SortStableFunc(measured, func(a *clientDialer, b *clientDialer) int {
		aAvg, _ := a.measuredLatency()
		bAvg, _ := b.measuredLatency()
		return cmp.Compare(aAvg, bAvg)
	})
	slices.SortStableFunc(rest, func(a *clientDialer, b *clientDialer) int {
		return a.priority - b.priority
	})
	return append(measured, rest...)
}

// applySmartDialerWeights scales the parallel-eval weights by measured cost
// when smart dialing is on. Only dialers with measured successes are scaled,
// and the scale is relative to the fastest dialer that works here, so a lone
// working transport is never penalised for being the only option. Weights never
// drop below the dialer's own minimumWeight, which keeps every transport
// reachable.
func applySmartDialerWeights(weights map[*clientDialer]float32) {
	if !SmartDialerEnabled() {
		return
	}

	var fastest time.Duration
	haveFastest := false
	for dialer := range weights {
		if !dialer.hasMeasuredLatency() {
			continue
		}
		if avg, _ := dialer.measuredLatency(); !haveFastest || avg < fastest {
			fastest = avg
			haveFastest = true
		}
	}
	if !haveFastest || fastest <= 0 {
		return
	}

	for dialer, weight := range weights {
		if !dialer.hasMeasuredLatency() {
			continue
		}
		avg, _ := dialer.measuredLatency()
		if avg <= 0 {
			continue
		}
		factor := float32(float64(fastest) / float64(avg))
		if factor < smartDialerLatencyFloor {
			factor = smartDialerLatencyFloor
		}
		if 1 < factor {
			factor = 1
		}
		scaled := weight * factor
		dialer.mutex.Lock()
		floor := dialer.minimumWeight
		dialer.mutex.Unlock()
		if scaled < floor {
			scaled = floor
		}
		weights[dialer] = scaled
	}
}
