package connect

import (
	"strings"
)

// proxyKeyUserSep separates Address from Auth.User in Key(). A proxy
// provider that shares one gateway host:port across multiple accounts (the
// account, not the address, decides which backend IP you land on) is
// identified by address+user, not address alone. \x1f is used because it
// cannot occur in a host:port and is exceedingly unlikely in a username, and
// it keeps an unauthenticated proxy's key byte-identical to its bare
// Address — the load-bearing property that lets every persisted store
// (proxy.state, earnings) keep reading old data with no migration for the
// unauthenticated case, and a bounded one for the authenticated case.
const proxyKeyUserSep = "\x1f"

// Key returns this proxy's unique identity: Address alone when there is no
// auth, or Address+proxyKeyUserSep+Auth.User when there is. Password is
// deliberately excluded — a credential rotation (same account, new
// password) must stay the same identity so its ID, health history, and
// earnings survive the rotation. Two proxies that differ only by password
// are a rotation, not a new proxy; two proxies at the same Address with
// different Auth.User are genuinely different proxies.
func (self *ProxySettings) Key() string {
	if self.Auth == nil || self.Auth.User == "" {
		return self.Address
	}
	return self.Address + proxyKeyUserSep + self.Auth.User
}

// SplitProxyKey is Key()'s inverse: it separates a key back into its
// address and (possibly empty) user, for callers that only have the key —
// e.g. formatting an operator-facing log line, where the raw key's \x1f
// separator must never appear verbatim.
func SplitProxyKey(key string) (address, user string) {
	address, user, ok := strings.Cut(key, proxyKeyUserSep)
	if !ok {
		return key, ""
	}
	return address, user
}
