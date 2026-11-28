// The failure classes of a peering dial (EXTENDER.md D3).
//
// A round dials the operator and the extenders it chose, and a failed dial
// carries one of a few reasons that repeat every round: libp2p refuses the
// dial before it starts because the peer's addresses are in backoff or it
// holds none, the gate refuses a peer or an address the round did not choose,
// the address does not answer in time, or it answers and refuses the
// connection. One class per failure is what lets the round report the failure
// that is news and stay quiet on the rest (`Node.noteDialClass`), and it names
// what an operator reads in the log line.
//
// The classifier reads the error only. Nothing here counts or stores a
// failure; the first-failure log line is its whole use.

package gossip

import (
	"context"
	"errors"
	"net"
	"syscall"

	"github.com/libp2p/go-libp2p/p2p/net/swarm"
)

// The class of a failed dial, or "" when the dial succeeded. Every failure has
// exactly one class, including a round canceled under its own dial. A libp2p
// dial error wraps the transport's cause, so a wrapped class is read as
// itself.
func dialFailureClass(err error) string {
	if err == nil {
		return ""
	}
	var netErr net.Error
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, swarm.ErrGaterDisallowedConnection):
		return "gated"
	case errors.Is(err, swarm.ErrDialBackoff):
		return "backoff"
	case errors.Is(err, swarm.ErrNoGoodAddresses):
		return "no_addrs"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "refused"
	case errors.As(err, &netErr) && netErr.Timeout():
		return "timeout"
	default:
		return "other"
	}
}
