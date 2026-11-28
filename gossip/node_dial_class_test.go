package gossip

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"

	"github.com/libp2p/go-libp2p/p2p/net/swarm"
)

// A dial error that reports a timeout without a deadline in it, as a stalled
// connect does.
type dialClassTimeoutError struct{}

func (dialClassTimeoutError) Error() string   { return "i/o timeout" }
func (dialClassTimeoutError) Timeout() bool   { return true }
func (dialClassTimeoutError) Temporary() bool { return true }

func TestDialFailureClass(t *testing.T) {
	for _, tc := range []struct {
		class string
		err   error
	}{
		{"", nil},
		{"canceled", context.Canceled},
		{"canceled", fmt.Errorf("dial: %w", context.Canceled)},
		{"gated", swarm.ErrGaterDisallowedConnection},
		{"backoff", swarm.ErrDialBackoff},
		{"no_addrs", swarm.ErrNoGoodAddresses},
		{"no_addrs", swarm.ErrNoAddresses},
		{"timeout", context.DeadlineExceeded},
		{"timeout", &net.OpError{Op: "dial", Err: context.DeadlineExceeded}},
		{"timeout", &net.OpError{Op: "dial", Err: dialClassTimeoutError{}}},
		{"refused", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}},
		{"other", errors.New("unexpected")},
	} {
		if got := dialFailureClass(tc.err); got != tc.class {
			t.Errorf("class of %v = %q, want %q", tc.err, got, tc.class)
		}
	}

	// The swarm wraps the cause a refusable dial failed on, one per address.
	// The class comes from the cause, not from the wrapper.
	for _, tc := range []struct {
		class string
		err   error
	}{
		{"gated", swarm.ErrGaterDisallowedConnection},
		{"backoff", swarm.ErrDialBackoff},
		{"no_addrs", swarm.ErrNoGoodAddresses},
		{"no_addrs", swarm.ErrNoAddresses},
	} {
		for _, err := range []error{
			fmt.Errorf("dial: %w", tc.err),
			&swarm.DialError{Cause: tc.err},
			&swarm.DialError{DialErrors: []swarm.TransportError{{Cause: tc.err}}},
		} {
			if got := dialFailureClass(err); got != tc.class {
				t.Errorf("class of %v = %q, want %q", err, got, tc.class)
			}
		}
	}
}
