package connect

import (
	"testing"
	"time"
)

// A pinned transport that dials through a proxy is not held by the local
// family probe, because the proxy supplies the exit. A direct pin is held.
func TestPinnedTransportThroughAProxyIgnoresTheLocalFamilyProbe(t *testing.T) {
	defer swapControlFamilyProbe(func(int) bool { return false })()

	newPin := func(throughProxy bool) *PlatformTransport {
		pin := &PlatformTransport{
			ipFamily:          6,
			dialsThroughProxy: throughProxy,
			log:               NewNoopLogger(),
			held:              NewMonitorValue(false),
			kickMonitor:       NewMonitor(),
			connectedMonitor:  NewMonitor(),
			pinnedBackoff:     newPinnedDialBackoff(time.Second, time.Minute),
		}
		pin.enabled.Store(true)
		return pin
	}

	proxied := newPin(true)
	proxied.refreshFamilyHold()
	if got := PlatformTransportState(proxied.familyHold.Load()); got != PlatformTransportStateConnecting {
		t.Fatalf("a pin through a proxy has state %s, want connecting", got)
	}
	if held, _ := proxied.held.Get(); held {
		t.Fatal("a pin through a proxy is held")
	}

	direct := newPin(false)
	direct.refreshFamilyHold()
	if got := PlatformTransportState(direct.familyHold.Load()); got != PlatformTransportStateSleeping {
		t.Fatalf("a direct pin without local IPv6 has state %s, want sleeping", got)
	}
}
