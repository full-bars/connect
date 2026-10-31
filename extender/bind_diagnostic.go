package extender

import (
	"errors"
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
		if detail := self.settings.BindOwnerDetail(transport, port, err); detail != "" {
			return "; " + detail
		}
	}
	return "; port already in use by another process"
}
