package connect

import (
	"sync"
	"sync/atomic"
)

// The proxies this process dials the platform through, as the reconnect attempt
// budget sees them.
//
// The budget queues the connects of proxied transports under a shared attempt
// rate while the backend is failing, and releases them best scout first. It
// keys waiters by a small integer, so each distinct ProxySettings gets a stable
// one, assigned the first time a transport behind it asks. The direct path has
// no proxy and is never queued.

// proxyIndexBase keeps assigned indexes clear of 0, which means "direct".
const proxyIndexBase = 1 << 20

var proxyRegistry struct {
	mutex sync.Mutex
	// by ProxySettings.Key()
	byKey map[string]int
	// parked and connected are by index
	parked    map[int]bool
	connected map[int]bool
}

// proxyIndexFor returns the stable index for a proxy, assigning one on first
// use. Nil settings, the direct path, is index 0.
func proxyIndexFor(settings *ProxySettings) int {
	if settings == nil {
		return 0
	}
	key := settings.Key()
	proxyRegistry.mutex.Lock()
	defer proxyRegistry.mutex.Unlock()
	if index, ok := proxyRegistry.byKey[key]; ok {
		return index
	}
	if proxyRegistry.byKey == nil {
		proxyRegistry.byKey = map[string]int{}
	}
	index := proxyIndexBase + len(proxyRegistry.byKey)
	proxyRegistry.byKey[key] = index
	return index
}

// proxiedCount is how many proxies are registered, the direct path not included.
func proxiedCount() int {
	proxyRegistry.mutex.Lock()
	defer proxyRegistry.mutex.Unlock()
	return len(proxyRegistry.byKey)
}

// markProxyParked records that the proxy is waiting its turn under the reconnect
// attempt budget (parked) or has been released from it. A parked proxy is not
// broken. Unknown indices are ignored.
func markProxyParked(index int, parked bool) {
	proxyRegistry.mutex.Lock()
	defer proxyRegistry.mutex.Unlock()
	if index < proxyIndexBase {
		return
	}
	if proxyRegistry.parked == nil {
		proxyRegistry.parked = map[int]bool{}
	}
	if parked {
		proxyRegistry.parked[index] = true
	} else {
		delete(proxyRegistry.parked, index)
	}
}

// ProxyParked reports whether the proxy behind these settings is waiting its
// turn under the reconnect attempt budget. A consumer that evicts dead proxies
// must not count a parked one: it is queued, not broken.
func ProxyParked(settings *ProxySettings) bool {
	if settings == nil {
		return false
	}
	key := settings.Key()
	proxyRegistry.mutex.Lock()
	defer proxyRegistry.mutex.Unlock()
	index, ok := proxyRegistry.byKey[key]
	return ok && proxyRegistry.parked[index]
}

// noteProxyConnected records that a transport behind the proxy connected, which
// is all the default scout ranking knows about it.
func noteProxyConnected(index int) {
	if index < proxyIndexBase {
		return
	}
	proxyRegistry.mutex.Lock()
	defer proxyRegistry.mutex.Unlock()
	if proxyRegistry.connected == nil {
		proxyRegistry.connected = map[int]bool{}
	}
	proxyRegistry.connected[index] = true
}

// proxyScoutScorer, when set, replaces the default scout ranking. A consumer
// sets it so scouts are ranked by what it knows and this package does not:
// earnings history and where each proxy came from.
var proxyScoutScorer atomic.Pointer[func(index int) uint64]

// SetProxyScoutScorer installs the scout ranking. The index is the one this
// package assigned (see ProxyIndex). A nil fn restores the default.
func SetProxyScoutScorer(fn func(index int) uint64) {
	if fn == nil {
		proxyScoutScorer.Store(nil)
		return
	}
	proxyScoutScorer.Store(&fn)
}

// ProxyIndex is the index the reconnect attempt budget uses for a proxy, for
// consumers that rank scouts. It registers the proxy if it is new.
func ProxyIndex(settings *ProxySettings) int { return proxyIndexFor(settings) }

// proxyScoutScore ranks a proxy as a recovery scout while the attempt budget is
// throttled: higher goes first. Unknown and never-connected proxies score zero.
func proxyScoutScore(index int) uint64 {
	if fn := proxyScoutScorer.Load(); fn != nil {
		return (*fn)(index)
	}
	return defaultProxyScoutScore(index)
}

// defaultProxyScoutScore is one point for having connected at all, so any proxy
// that has served outranks one with no record.
func defaultProxyScoutScore(index int) uint64 {
	proxyRegistry.mutex.Lock()
	defer proxyRegistry.mutex.Unlock()
	if proxyRegistry.connected[index] {
		return 1
	}
	return 0
}
