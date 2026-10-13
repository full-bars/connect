package connect

import (
	"testing"
)

func resetProxyRegistryForTest(t *testing.T) {
	t.Helper()
	reset := func() {
		proxyRegistry.mutex.Lock()
		defer proxyRegistry.mutex.Unlock()
		proxyRegistry.byKey = nil
		proxyRegistry.parked = nil
		proxyRegistry.connected = nil
	}
	reset()
	t.Cleanup(reset)
	t.Cleanup(func() { SetProxyScoutScorer(nil) })
}

func testProxy(address string) *ProxySettings {
	return &ProxySettings{Network: "tcp", Address: address}
}

func TestProxiedCount_LeavesOutTheDirectPath(t *testing.T) {
	resetProxyRegistryForTest(t)
	if proxyIndexFor(nil) != 0 {
		t.Fatal("the direct path is index 0")
	}
	for _, address := range []string{"127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3"} {
		if proxyIndexFor(testProxy(address)) == 0 {
			t.Fatalf("proxy %s was given the direct index", address)
		}
	}
	if got := proxiedCount(); got != 3 {
		t.Fatalf("proxiedCount = %d, want 3", got)
	}
}

func TestProxyIndexFor_IsStablePerProxy(t *testing.T) {
	resetProxyRegistryForTest(t)
	a, b := proxyIndexFor(testProxy("127.0.0.1:1")), proxyIndexFor(testProxy("127.0.0.1:2"))
	if a == b {
		t.Fatal("two proxies share an index")
	}
	if again := proxyIndexFor(testProxy("127.0.0.1:1")); again != a {
		t.Fatalf("index changed from %d to %d", a, again)
	}
	if proxiedCount() != 2 {
		t.Fatalf("a repeated proxy registered twice: %d", proxiedCount())
	}
}

func TestProxyParked_FollowsTheBudgetAndIgnoresDirect(t *testing.T) {
	resetProxyRegistryForTest(t)
	settings := testProxy("127.0.0.1:1")
	index := proxyIndexFor(settings)
	if ProxyParked(settings) {
		t.Fatal("a fresh proxy is not parked")
	}
	markProxyParked(index, true)
	if !ProxyParked(settings) {
		t.Fatal("a proxy waiting its turn must read as parked")
	}
	markProxyParked(index, false)
	markProxyParked(0, true)
	if ProxyParked(settings) || ProxyParked(nil) {
		t.Fatal("release must clear the mark and the direct path is never parked")
	}
}

func TestProxyScoutScore_DefaultRanksConnectedProxiesFirstAndCanBeReplaced(t *testing.T) {
	resetProxyRegistryForTest(t)
	served, never := proxyIndexFor(testProxy("127.0.0.1:1")), proxyIndexFor(testProxy("127.0.0.1:2"))
	noteProxyConnected(served)
	if proxyScoutScore(served) <= proxyScoutScore(never) {
		t.Fatal("a proxy that connected must outrank one with no record")
	}
	SetProxyScoutScorer(func(index int) uint64 { return uint64(index) })
	if proxyScoutScore(never) != uint64(never) {
		t.Fatal("an installed scorer must replace the default")
	}
	SetProxyScoutScorer(nil)
	if proxyScoutScore(served) != 1 {
		t.Fatal("a nil scorer must restore the default")
	}
}
