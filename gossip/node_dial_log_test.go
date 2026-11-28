// What a peering round logs when a dial fails (EXTENDER.md D3).
//
// A class is logged the first time it happens to a peer and not on the repeats
// of it, so a round that keeps failing for the same reason writes one line per
// peer and class rather than one per round. The tests drive the transitions on
// a host whose dial returns a set error, with verbose logging off, so the only
// lines the logger sees are the first failures.

package gossip

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"

	"github.com/urnetwork/connect"
)

// A host whose dial returns the error the test set, so a failure class is
// reached without a network.
type dialClassHost struct {
	host.Host
	err error
}

func (self *dialClassHost) Connect(context.Context, peer.AddrInfo) error {
	return self.err
}

// A logger that keeps the lines it is given, with verbose logging off.
type dialClassLogger struct {
	connect.Logger
	lines []string
}

func (self *dialClassLogger) Infof(format string, args ...any) {
	self.lines = append(self.lines, fmt.Sprintf(format, args...))
}

func TestConnectPeerLogsFailureTransitions(t *testing.T) {
	logger := &dialClassLogger{Logger: connect.NewNoopLogger()}
	h := &dialClassHost{}
	n := &Node{
		ctx:      context.Background(),
		host:     h,
		settings: &NodeSettings{DialTimeout: time.Second},
		log:      logger,
	}
	addrInfo := peer.AddrInfo{ID: peer.ID("peer")}
	for _, err := range []error{swarm.ErrDialBackoff, swarm.ErrDialBackoff, swarm.ErrGaterDisallowedConnection, nil, swarm.ErrGaterDisallowedConnection} {
		h.err = err
		n.connectPeer(addrInfo)
	}
	if len(logger.lines) != 3 {
		t.Fatalf("lines = %q", logger.lines)
	}
	for i, want := range []string{"class=backoff", "class=gated", "class=gated"} {
		if !strings.Contains(logger.lines[i], want) {
			t.Fatalf("line %d = %q, want %q", i, logger.lines[i], want)
		}
	}
}

func TestDialClassTransitions(t *testing.T) {
	n := &Node{}
	id := peer.ID("peer")
	for _, tc := range []struct {
		class string
		first bool
	}{
		{"backoff", true}, {"backoff", false}, {"gated", true}, {"backoff", false}, {"gated", false},
		{"", false}, {"gated", true}, {"backoff", true},
	} {
		if got := n.noteDialClass(id, tc.class); got != tc.first {
			t.Fatalf("class %q: got %v, want %v", tc.class, got, tc.first)
		}
	}
	for i := 0; i < 2000; i++ {
		n.noteDialClass(peer.ID(fmt.Sprintf("peer-%d", i)), "other")
	}
	if 1024 < len(n.dialFailureClasses) {
		t.Fatalf("unbounded entries: %d", len(n.dialFailureClasses))
	}
}
