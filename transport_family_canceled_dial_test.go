package connect

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// A canceled dial is local (a racing transport, a closing window), not evidence
// about the backend. Counting one trips the process-wide degraded gate while
// the backend is answering every request, and the gate then suppresses contract
// creation for the whole CreateContractTimeout.
func TestIsCanceledDial(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"context canceled", context.Canceled, true},
		{"wrapped context canceled", fmt.Errorf("dial: %w", context.Canceled), true},
		{"runtime poller spelling", errors.New("h3 through proxy 48.44.151.46:6831: dial tcp 48.44.151.46:6831: operation was canceled"), true},
		{"british spelling", errors.New("connection cancelled"), true},
		{"timeout stays backend evidence", errors.New("Timeout."), false},
		{"refused stays backend evidence", errors.New("dial tcp 1.2.3.4:443: connect: connection refused"), false},
		{"dns stays backend evidence", errors.New("dial tcp: lookup connect.example: no such host"), false},
		{"deadline stays backend evidence", context.DeadlineExceeded, false},
	}
	for _, c := range cases {
		if got := isCanceledDial(c.err); got != c.want {
			t.Errorf("%s: isCanceledDial(%v) = %t, want %t", c.name, c.err, got, c.want)
		}
	}
}
