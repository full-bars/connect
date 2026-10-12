package connect

import (
	"errors"
	"net"
	"net/http"
	"strings"
)

// Dial error classification for the reconnect governor.
//
// The reconnect governor decides from how connect attempts fail, so it needs to
// tell the platform refusing an identity apart from the platform struggling, and
// a failure of the proxy path apart from a failure of the platform behind it.
// This fork already keeps the real cause of an exhausted dial as a typed cause
// graph (HttpRequestExhaustedError, see net_http_exhaustion.go), so nothing here
// wraps or replaces the dial error: these helpers read it.

type authRejector interface {
	AuthRejected() bool
}

// explicitRejection reports whether err, or anything it wraps, is the platform
// explicitly refusing the identity. It is structural on purpose: the text of an
// error ("401 Unauthorized") is not evidence, and a timeout is never one. Only
// 401 and 403 are the platform saying no to this identity; overload (429, 5xx, a
// load balancer's 499) and routing errors say nothing about whether the token is
// good.
func explicitRejection(err error) bool {
	for _, cause := range flattenHttpRequestCauses(err) {
		if causeRejects(cause.err) {
			return true
		}
	}
	return causeRejects(err)
}

func causeRejects(err error) bool {
	for e := err; e != nil; e = errors.Unwrap(e) {
		if r, ok := e.(authRejector); ok && r.AuthRejected() {
			return true
		}
		if upgrade, ok := e.(*HTTPUpgradeError); ok && upgrade != nil &&
			(upgrade.StatusCode == http.StatusUnauthorized || upgrade.StatusCode == http.StatusForbidden) {
			return true
		}
	}
	return false
}

// isProxyLevelError reports whether err is a failure of the proxy path itself (a
// SOCKS handshake that failed or timed out, a proxy that cannot be reached)
// rather than of the platform behind it.
func isProxyLevelError(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "socks connect") || (strings.Contains(msg, "proxy") && strings.Contains(msg, "unreachable"))
}

// isTimeoutError reports whether err, or anything it wraps, is a net.Error that
// timed out.
func isTimeoutError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
