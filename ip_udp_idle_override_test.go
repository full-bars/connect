package connect

import (
	"context"
	"testing"
	"time"
)

// The proxied-flow idle override rides on the DialContextSettings the sdk
// already installs on the exit nat's UDP buffer, so an embedder can shorten
// the NAT binding lifetime for one slot without the sdk learning a new knob.
func TestLocalUserNatUdpIdleOverride(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	build := func(dial *DialContextSettings) *LocalUserNat {
		settings := DefaultProviderLocalUserNatSettings()
		settings.UdpBufferSettings.DialContextSettings = dial
		return NewLocalUserNat(ctx, "udp-idle-override", settings)
	}

	// no dial settings: the provider default stands
	nat := build(nil)
	defer nat.Close()
	AssertEqual(t, nat.settings.UdpBufferSettings.IdleTimeout, providerUdpIdleTimeout)

	// dial settings without an override: unchanged
	nat = build(&DialContextSettings{})
	defer nat.Close()
	AssertEqual(t, nat.settings.UdpBufferSettings.IdleTimeout, providerUdpIdleTimeout)

	// override applies to this nat
	nat = build(&DialContextSettings{UdpIdleTimeout: 120 * time.Second})
	defer nat.Close()
	AssertEqual(t, nat.settings.UdpBufferSettings.IdleTimeout, 120*time.Second)
	AssertEqual(t, nat.settings.UdpBufferSettings.ReadTimeout, 120*time.Second)

	// a bound never lengthens a shorter timeout
	shortSettings := DefaultProviderLocalUserNatSettings()
	shortSettings.UdpBufferSettings.IdleTimeout = 30 * time.Second
	shortSettings.UdpBufferSettings.ReadTimeout = 30 * time.Second
	shortSettings.UdpBufferSettings.DialContextSettings = &DialContextSettings{UdpIdleTimeout: 120 * time.Second}
	nat = NewLocalUserNat(ctx, "udp-idle-short", shortSettings)
	defer nat.Close()
	AssertEqual(t, nat.settings.UdpBufferSettings.IdleTimeout, 30*time.Second)
	AssertEqual(t, nat.settings.UdpBufferSettings.ReadTimeout, 30*time.Second)

	// and never writes through the caller's shared settings
	shared := DefaultProviderLocalUserNatSettings()
	shared.UdpBufferSettings.DialContextSettings = &DialContextSettings{UdpIdleTimeout: 120 * time.Second}
	nat = NewLocalUserNat(ctx, "udp-idle-shared", shared)
	defer nat.Close()
	AssertEqual(t, shared.UdpBufferSettings.IdleTimeout, providerUdpIdleTimeout)
	AssertEqual(t, shared.UdpBufferSettings.ReadTimeout, 300*time.Second)
}
