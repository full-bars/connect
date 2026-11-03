package extender

import (
	"errors"
	"strings"
	"syscall"
)

// The detail appended to a bind failure. Only an address-in-use failure gets
// one. The owner of the port comes from the caller's BindOwnerDetail hook,
// since the library does not run utilities to find it.
func (self *ExtenderServer) bindOwnerDetail(transport string, port int, err error) string {
	if !errors.Is(err, syscall.EADDRINUSE) {
		return ""
	}
	if self.settings.BindOwnerDetail != nil {
		// Normalize first, then decide: a hook that returns only whitespace is
		// not a usable detail, and returning "; " from it would suppress the
		// one line that names the cause.
		if detail := strings.Join(strings.Fields(self.settings.BindOwnerDetail(transport, port, err)), " "); detail != "" {
			return "; " + detail
		}
	}
	return "; port already in use"
}
