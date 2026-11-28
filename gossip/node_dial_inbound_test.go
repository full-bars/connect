// An inbound connection settles a peer's dial backoff (EXTENDER.md D3).
//
// The swarm clears its backoff for a peer whenever a connection to it comes
// up, inbound included, so a peer the round skipped as unreachable can be
// chosen again as soon as it reaches the node itself. The test holds a real
// swarm, puts one peer's address into its backoff, and then connects that peer
// inbound, so the round's choice is read against the swarm's own state rather
// than a stand-in for it.

package gossip

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/transport"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"

	ma "github.com/multiformats/go-multiaddr"
)

// A transport that listens on a channel of connections the test hands it, so a
// connection comes up with no socket and nothing to time out.
type inboundTestTransport struct {
	transport.Transport
	listener *inboundTestListener
}

func (self *inboundTestTransport) Protocols() []int { return []int{ma.P_TCP} }
func (self *inboundTestTransport) Proxy() bool      { return false }
func (self *inboundTestTransport) Listen(ma.Multiaddr) (transport.Listener, error) {
	return self.listener, nil
}

type inboundTestListener struct {
	addr     ma.Multiaddr
	incoming chan transport.CapableConn
	done     chan struct{}
	once     sync.Once
}

func (self *inboundTestListener) Accept() (transport.CapableConn, error) {
	select {
	case conn := <-self.incoming:
		return conn, nil
	case <-self.done:
		return nil, transport.ErrListenerClosed
	}
}

func (self *inboundTestListener) Close() error {
	self.once.Do(func() { close(self.done) })
	return nil
}

func (self *inboundTestListener) Addr() net.Addr          { return &net.TCPAddr{} }
func (self *inboundTestListener) Multiaddr() ma.Multiaddr { return self.addr }

// A connection the listener hands the swarm, which reports the peer and the
// address the test chose.
type inboundTestConn struct {
	transport.CapableConn
	id   peer.ID
	addr ma.Multiaddr
	done chan struct{}
	once sync.Once
}

func (self *inboundTestConn) RemotePeer() peer.ID           { return self.id }
func (self *inboundTestConn) RemoteMultiaddr() ma.Multiaddr { return self.addr }
func (self *inboundTestConn) LocalMultiaddr() ma.Multiaddr  { return self.addr }
func (self *inboundTestConn) RemotePublicKey() crypto.PubKey {
	return nil
}

func (self *inboundTestConn) Close() error {
	self.once.Do(func() { close(self.done) })
	return nil
}

func (self *inboundTestConn) CloseWithError(network.ConnErrorCode) error {
	return self.Close()
}

func (self *inboundTestConn) IsClosed() bool {
	select {
	case <-self.done:
		return true
	default:
		return false
	}
}

func (self *inboundTestConn) AcceptStream() (network.MuxedStream, error) {
	<-self.done
	return nil, errors.New("closed")
}

func (self *inboundTestConn) OpenStream(context.Context) (network.MuxedStream, error) {
	return nil, errors.New("unused")
}

func TestInboundConnectionClearsDialBackoff(t *testing.T) {
	peerstore, err := pstoremem.NewPeerstore()
	if err != nil {
		t.Fatal(err)
	}
	defer peerstore.Close()
	swarmNetwork, err := swarm.NewSwarm(peer.ID("local"), peerstore, eventbus.NewBus())
	if err != nil {
		t.Fatal(err)
	}
	defer swarmNetwork.Close()

	addr := ma.StringCast("/ip4/192.0.2.1/tcp/1234")
	listener := &inboundTestListener{addr: addr, incoming: make(chan transport.CapableConn), done: make(chan struct{})}
	if err := swarmNetwork.AddTransport(&inboundTestTransport{listener: listener}); err != nil {
		t.Fatal(err)
	}
	connected := make(chan network.Conn, 1)
	swarmNetwork.Notify(&network.NotifyBundle{
		ConnectedF: func(_ network.Network, conn network.Conn) { connected <- conn },
	})
	if err := swarmNetwork.Listen(addr); err != nil {
		t.Fatal(err)
	}

	skippedPeer := peer.AddrInfo{ID: peer.ID("skipped-peer"), Addrs: []ma.Multiaddr{addr}}
	laterPeer := peer.AddrInfo{ID: peer.ID("later-peer"), Addrs: []ma.Multiaddr{addr}}
	// a long backoff, so it is still in place when the connection arrives
	for range 100 {
		swarmNetwork.Backoff().AddBackoff(skippedPeer.ID, addr)
	}
	selectRound := func() ([]peer.AddrInfo, bool) {
		return selectExtenderDials(
			[]peer.AddrInfo{skippedPeer, laterPeer},
			1,
			func(id peer.ID) bool { return swarmNetwork.Connectedness(id) == network.Connected },
			func(info peer.AddrInfo) bool { return allAddrsBackedOff(info, swarmNetwork.Backoff().Backoff) },
		)
	}

	got, skipped := selectRound()
	if !skipped || len(got) != 1 || got[0].ID != laterPeer.ID {
		t.Fatalf("selection in backoff: %v %v", got, skipped)
	}

	listener.incoming <- &inboundTestConn{id: skippedPeer.ID, addr: addr, done: make(chan struct{})}
	conn := <-connected
	if conn.Stat().Direction != network.DirInbound {
		t.Fatal("the connection was not inbound")
	}
	if swarmNetwork.Backoff().Backoff(skippedPeer.ID, addr) {
		t.Fatal("the inbound connection did not clear the backoff")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	got, skipped = selectRound()
	if skipped || len(got) != 1 || got[0].ID != skippedPeer.ID {
		t.Fatalf("selection after the inbound connection: %v %v", got, skipped)
	}
}
