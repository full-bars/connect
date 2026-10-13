package connect

import (
	"context"
	"sync/atomic"
	"time"
)

// The hooks the platform transport loops call into the reconnect governor, the
// attempt budget and the flap guard. The decisions live in reconnect_governor.go,
// reconnect_budget.go and transport_flap_guard.go; this file is only how a
// transport reports to them. All of it follows URNETWORK_GOVERN: off does nothing,
// shadow observes and reports, enforce acts.

var flapErrThrottle = newLogThrottle(time.Minute)

// transportFlapWouldHold counts the reconnects the flap guard would have held
// back in shadow mode.
var transportFlapWouldHold atomic.Int64

// TransportFlapWouldHold is how many reconnects shadow mode saw the flap guard
// would have held back after a session died young.
func TransportFlapWouldHold() int64 { return transportFlapWouldHold.Load() }

// governProxy is this transport's place in the attempt budget: the index of the
// proxy it dials through, and whether it is direct. A direct transport is never
// queued by the budget.
func (self *PlatformTransport) governProxy() (index int, direct bool) {
	if self.clientStrategy == nil || self.clientStrategy.settings == nil {
		return 0, true
	}
	index = proxyIndexFor(self.clientStrategy.settings.ProxySettings)
	return index, index == 0
}

// beginGovernedAttempt returns the stats one connect attempt carries in its dial
// context, or nil when the governor is off.
func (self *PlatformTransport) beginGovernedAttempt() *attemptStats {
	if currentGovernMode() == governOff {
		return nil
	}
	startGovernorLoop()
	return &attemptStats{}
}

// admitGovernedConnect holds a proxied transport its turn under the shared
// attempt budget while the backend is failing. The direct path never waits. It
// returns false only when ctx ends first.
func (self *PlatformTransport) admitGovernedConnect(ctx context.Context) bool {
	index, direct := self.governProxy()
	return admitConnect(ctx, direct, index)
}

// recordGovernedAttempt tells the governor how one connect attempt ended.
// stats may be nil (an H3 dial carries none). countsAsHealth is false for a
// transport that is never a health signal.
func (self *PlatformTransport) recordGovernedAttempt(err error, stats *attemptStats, countsAsHealth bool) {
	if !countsAsHealth || currentGovernMode() == governOff {
		return
	}
	connectGov.recordAttempt(classifyConnectError(err, stats), stats)
	if err == nil {
		if index, direct := self.governProxy(); !direct {
			noteProxyConnected(index)
		}
	}
}

// countsAsGovernHealth reports whether this mode's loop is a health signal for
// the platform. H1 is the primary carrier, so in Auto the H3 modes beside it
// would only count the same platform twice; an explicit H3 target is the only
// transport and does count.
func (self *PlatformTransport) countsAsGovernHealth(mode TransportMode) bool {
	if mode == TransportModeH1 {
		return true
	}
	return self.targetMode != TransportModeAuto
}

// sessionEndWait applies the end-of-session rules shared by the H1 and H3 loops
// and returns the extra wait before the next connect, 0 for none.
//
// A session that dies young is evidence about the platform, but only if it was
// not cut short by our own teardown (a reload, a network change, shutdown), which
// cutShort says. countsAsHealth is false for a loop that is not a health signal;
// it is still held back so it cannot redial in a tight loop. In shadow the wait
// is counted and reported, never taken.
func sessionEndWait(ctx context.Context, flaps *flapGuard, lived time.Duration, cutShort bool, clientId string, log Logger, countsAsHealth bool) time.Duration {
	mode := currentGovernMode()
	if mode == governOff || cutShort || ctx.Err() != nil {
		return 0
	}
	if countsAsHealth && flaps.isYoung(lived) {
		if flaps.everStable {
			connectGov.recordProvenEarlyDeath()
		} else {
			connectGov.recordEarlyDeath()
		}
	}
	flapWait := flaps.sessionEnded(lived)
	if flapWait <= 0 {
		return 0
	}
	if ok, suppressed := flapErrThrottle.Allow(time.Now()); ok {
		if mode == governEnforce {
			log.Infof("[t]flap %s session lived %s, holding reconnect %s (%d suppressed)\n", clientId, lived.Round(time.Millisecond), flapWait.Round(time.Millisecond), suppressed)
		} else {
			log.Infof("[t]flap %s session lived %s, would hold reconnect %s; shadow, not held (%d suppressed)\n", clientId, lived.Round(time.Millisecond), flapWait.Round(time.Millisecond), suppressed)
		}
	}
	if mode != governEnforce {
		transportFlapWouldHold.Add(1)
		return 0
	}
	transportFlapBackoffs.Add(1)
	return flapWait
}

// channelClosed reports whether a notify channel has already fired.
func channelClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
