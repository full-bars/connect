package connect

import (
	"os"
	"strings"
	"sync/atomic"
)

// The protection mode shared by the outage protections that ship on this line.
// URNETWORK_GOVERN selects it. Unset, empty and unrecognised values mean
// shadow: these protections change when frames go on the wire, so enforcement
// has to be asked for by name. A typo therefore turns nothing up, and nothing
// down either.

type governMode int32

const (
	// governOff: the protections do nothing. An emergency override only.
	governOff governMode = iota
	// governShadow: observe and report only, never refuse or delay.
	governShadow
	// governEnforce: the protections act. Opt in with URNETWORK_GOVERN=enforce.
	governEnforce
)

var governModeValue atomic.Int32

func init() { governModeValue.Store(int32(parseGovernMode(os.Getenv("URNETWORK_GOVERN")))) }

func parseGovernMode(raw string) governMode {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "off", "0", "false", "no":
		return governOff
	case "enforce", "on", "1", "true", "yes":
		return governEnforce
	}
	// unset, empty or unrecognised: shadow. Enforcement is a behaviour change
	// and has to be explicit.
	return governShadow
}

func currentGovernMode() governMode { return governMode(governModeValue.Load()) }

// setGovernModeForTest swaps the mode and returns a restore func.
func setGovernModeForTest(mode governMode) func() {
	prev := governModeValue.Swap(int32(mode))
	return func() { governModeValue.Store(prev) }
}
