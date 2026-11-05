package connect

import (
	"context"
	"errors"
	"net"
	"testing"
)

func TestControlProxiedHandshakeDoesNotDemoteOrRetry(t *testing.T) {
	for _, pinned := range []bool{false, true} {
		name := "agnostic"
		if pinned {
			name = "pinned"
		}
		t.Run(name, func(t *testing.T) {
			restore := swapControlFamilyProbe(func(int) bool { return true })
			defer restore()
			controlFamilyClear()
			defer controlFamilyClear()
			settings := DefaultConnectSettings()
			settings.ProxySettings = &ProxySettings{Network: "tcp", Address: "127.0.0.1:1080"}
			ctx := context.Background()
			if pinned {
				ctx = withPinnedIpFamily(ctx, 6)
			}
			attempts := 0
			demotedDuringRetry := 0
			dial := func(context.Context, string, string) (net.Conn, error) {
				attempts++
				if attempts > 1 {
					demotedDuringRetry = controlFamilyDemotedFamily()
				}
				return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 1080}}, nil
			}
			timeout := &timeoutError{}
			handshake := func(context.Context, net.Conn) (net.Conn, error) { return nil, timeout }
			_, err := dialControlTlsWithFamilyFallback(ctx, settings, "tcp", "api.example:443", dial, handshake)
			if !errors.Is(err, timeout) {
				t.Errorf("got error %v, want original timeout", err)
			}
			if attempts != 1 {
				t.Errorf("dialed %d times, want 1", attempts)
			}
			if demotedDuringRetry != 0 {
				t.Errorf("demoted IPv%d before proxy retry", demotedDuringRetry)
			}
			if got := controlFamilyDemotedFamily(); got != 0 {
				t.Errorf("demoted IPv%d for proxy handshake", got)
			}
		})
	}
}

func TestControlDirectHandshakeStillDemotesAndRetries(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()
	settings := DefaultConnectSettings()
	// An injected dial overrides ProxySettings, so this path is direct.
	settings.ProxySettings = &ProxySettings{Network: "tcp", Address: "127.0.0.1:1080"}
	settings.DialContextSettings = &DialContextSettings{}
	var networks []string
	dial := func(_ context.Context, network, _ string) (net.Conn, error) {
		networks = append(networks, network)
		ip := "2001:db8::1"
		if network == "tcp4" {
			ip = "192.0.2.1"
		}
		return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP(ip), Port: 443}}, nil
	}
	handshake := func(_ context.Context, conn net.Conn) (net.Conn, error) {
		if connFamily(conn) == 6 {
			return nil, &timeoutError{}
		}
		return conn, nil
	}
	conn, err := dialControlTlsWithFamilyFallback(context.Background(), settings, "tcp", "api.example:443", dial, handshake)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if len(networks) != 2 || networks[0] != "tcp" || networks[1] != "tcp4" {
		t.Errorf("dialed %v, want [tcp tcp4]", networks)
	}
	if got := controlFamilyDemotedFamily(); got != 6 {
		t.Errorf("demoted IPv%d, want IPv6", got)
	}
}

func TestControlProxiedDialFailureDoesNotUndoDemotion(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()
	if !controlFamilyDemote(6) {
		t.Fatal("could not seed demotion")
	}
	settings := DefaultConnectSettings()
	settings.ProxySettings = &ProxySettings{Network: "tcp", Address: "127.0.0.1:1080"}
	attempts := 0
	timeout := &timeoutError{}
	dial := func(context.Context, string, string) (net.Conn, error) { attempts++; return nil, timeout }
	handshake := func(context.Context, net.Conn) (net.Conn, error) { t.Fatal("unexpected handshake"); return nil, nil }
	_, err := dialControlTlsWithFamilyFallback(context.Background(), settings, "tcp", "api.example:443", dial, handshake)
	if !errors.Is(err, timeout) {
		t.Errorf("got %v, want original timeout", err)
	}
	if attempts != 1 {
		t.Errorf("dialed %d times, want 1", attempts)
	}
	if got := controlFamilyDemotedFamily(); got != 6 {
		t.Errorf("demoted IPv%d, want existing IPv6 demotion", got)
	}
}
