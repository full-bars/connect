// The peering round's extender choice against libp2p's dial backoff
// (EXTENDER.md D3).
//
// The swarm holds a backoff per peer address and refuses a dial to an address
// inside it at once, so a round that dials such a peer spends one of its slots
// on a dial that cannot succeed. The tests read the backoff through a fixed
// clock, so a window is exact and nothing waits, and one test holds the node's
// host to the swarm the round reads the backoff from.

package gossip

import (
	"slices"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"

	ma "github.com/multiformats/go-multiaddr"
)

// The round reaches the backoff through the host's network, which is the
// swarm only if the host was built on one. Any other network would leave every
// peer dialable and the skip silently off.
func TestNodeHostNetworkIsSwarm(t *testing.T) {
	node := newTestLocalNode(t, nil)
	if _, ok := node.host.Network().(*swarm.Swarm); !ok {
		t.Fatalf("node network is %T, not *swarm.Swarm", node.host.Network())
	}
}

// A peer's addresses inside the backoff window are skipped and the free slot
// goes to the next candidate; once the window passes, or is cleared, the peer
// is chosen again.
func TestSelectExtenderDialsBackoff(t *testing.T) {
	addr := ma.StringCast("/ip4/192.0.2.1/tcp/1234")
	a := peer.AddrInfo{ID: peer.ID("a"), Addrs: []ma.Multiaddr{addr}}
	b := peer.AddrInfo{ID: peer.ID("b"), Addrs: []ma.Multiaddr{addr}}
	now := time.Unix(100, 0)
	until := now.Add(5 * time.Second)
	backoff := func(id peer.ID, _ ma.Multiaddr) bool { return id == a.ID && now.Before(until) }
	selectDials := func() ([]peer.AddrInfo, bool) {
		return selectExtenderDials(
			[]peer.AddrInfo{a, b},
			1,
			func(peer.ID) bool { return false },
			func(info peer.AddrInfo) bool { return allAddrsBackedOff(info, backoff) },
		)
	}
	got, skipped := selectDials()
	if !skipped || len(got) != 1 || got[0].ID != b.ID {
		t.Fatalf("inside the window: %v %v", got, skipped)
	}
	now = until
	got, skipped = selectDials()
	if skipped || len(got) != 1 || got[0].ID != a.ID {
		t.Fatalf("after the window: %v %v", got, skipped)
	}
	now = until.Add(-time.Second)
	until = time.Time{}
	got, skipped = selectDials()
	if skipped || len(got) != 1 || got[0].ID != a.ID {
		t.Fatalf("after the backoff is cleared: %v %v", got, skipped)
	}
}

func TestAllAddrsBackedOff(t *testing.T) {
	addrA := ma.StringCast("/ip4/192.0.2.1/tcp/1234")
	addrB := ma.StringCast("/ip4/192.0.2.2/tcp/1234")
	backoff := func(_ peer.ID, addr ma.Multiaddr) bool { return addr.Equal(addrA) }
	for _, tc := range []struct {
		info      peer.AddrInfo
		backedOff bool
	}{
		// a peer with no addresses is not backed off: the dial says why
		{peer.AddrInfo{ID: peer.ID("none")}, false},
		{peer.AddrInfo{ID: peer.ID("one"), Addrs: []ma.Multiaddr{addrA}}, true},
		{peer.AddrInfo{ID: peer.ID("one-of-two"), Addrs: []ma.Multiaddr{addrA, addrB}}, false},
		{peer.AddrInfo{ID: peer.ID("other"), Addrs: []ma.Multiaddr{addrB}}, false},
	} {
		if got := allAddrsBackedOff(tc.info, backoff); got != tc.backedOff {
			t.Errorf("%s: backed off = %v, want %v", tc.info.ID, got, tc.backedOff)
		}
	}
}

// A connected peer counts toward the target, so only the missing slots are
// dialed, and the skipped result is set only while the target is unmet.
func TestSelectExtenderDialsTarget(t *testing.T) {
	addr := ma.StringCast("/ip4/192.0.2.1/tcp/1234")
	infos := []peer.AddrInfo{
		{ID: peer.ID("a"), Addrs: []ma.Multiaddr{addr}},
		{ID: peer.ID("b"), Addrs: []ma.Multiaddr{addr}},
		{ID: peer.ID("c"), Addrs: []ma.Multiaddr{addr}},
	}
	for _, tc := range []struct {
		name      string
		target    int
		connected map[peer.ID]bool
		backedOff map[peer.ID]bool
		pending   []peer.ID
		skipped   bool
	}{
		{
			name:      "connected fills the target",
			target:    1,
			connected: map[peer.ID]bool{peer.ID("a"): true},
			pending:   nil,
		},
		{
			name:      "a free slot goes to a later candidate",
			target:    2,
			connected: map[peer.ID]bool{peer.ID("a"): true},
			backedOff: map[peer.ID]bool{peer.ID("b"): true},
			pending:   []peer.ID{peer.ID("c")},
			// the connected peers alone are short of the target, so the round
			// reports that it skipped a peer, whatever the later dial does
			skipped: true,
		},
		{
			name:    "only the slots are dialed",
			target:  1,
			pending: []peer.ID{peer.ID("a")},
		},
		{
			name:      "a skipped peer does not report while the target is met",
			target:    1,
			connected: map[peer.ID]bool{peer.ID("a"): true},
			backedOff: map[peer.ID]bool{peer.ID("b"): true},
			pending:   nil,
		},
		{
			name:      "a skipped peer reports while the target is unmet",
			target:    2,
			backedOff: map[peer.ID]bool{peer.ID("a"): true},
			pending:   []peer.ID{peer.ID("b"), peer.ID("c")},
			skipped:   true,
		},
	} {
		got, skipped := selectExtenderDials(
			infos,
			tc.target,
			func(id peer.ID) bool { return tc.connected[id] },
			func(info peer.AddrInfo) bool { return tc.backedOff[info.ID] },
		)
		gotIds := []peer.ID{}
		for _, info := range got {
			gotIds = append(gotIds, info.ID)
		}
		if skipped != tc.skipped || !slices.Equal(gotIds, tc.pending) {
			t.Fatalf("%s: dials = %v %v, want %v %v", tc.name, gotIds, skipped, tc.pending, tc.skipped)
		}
	}
}
