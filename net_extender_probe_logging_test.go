package connect

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestProbeExtenderCarrierFailurePhases(t *testing.T) {
	refusal := errors.New("dial refused")
	for _, tc := range []struct {
		name, phase string
		setup       func(*testing.T, *ConnectSettings) (int, func())
	}{
		{"dial", "dial", func(t *testing.T, settings *ConnectSettings) (int, func()) {
			settings.DialContextSettings = &DialContextSettings{DialContext: func(context.Context, string, string) (net.Conn, error) { return nil, refusal }}
			return 12345, func() {}
		}},
		{"handshake", "handshake", func(t *testing.T, settings *ConnectSettings) (int, func()) {
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
			_, port, _ := net.SplitHostPort(server.Listener.Addr().String())
			p, _ := strconv.Atoi(port)
			return p, server.Close
		}},
		{"challenge", "challenge", func(t *testing.T, settings *ConnectSettings) (int, func()) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusForbidden) }))
			_, port, _ := net.SplitHostPort(server.Listener.Addr().String())
			p, _ := strconv.Atoi(port)
			return p, server.Close
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings := DefaultConnectSettings()
			port, cleanup := tc.setup(t, settings)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := ProbeExtenderCarrier(ctx, settings, netip.MustParseAddr("127.0.0.1"), ExtenderConnectModeTcpTls, port, "", "", nil, "example.org", 443)
			var probe *ExtenderProbeError
			if !errors.As(err, &probe) || probe.Phase != tc.phase || probe.Port != port || probe.Carrier != ExtenderCarrierTcp {
				t.Fatalf("probe error = %#v (%v)", probe, err)
			}
			if !strings.Contains(err.Error(), "address=127.0.0.1:"+strconv.Itoa(port)) || strings.Contains(err.Error(), "\n") {
				t.Fatalf("probe message = %q", err)
			}
			if tc.name == "dial" && !errors.Is(err, refusal) {
				t.Fatal("dial cause lost")
			}
		})
	}
}

func TestProbeExtenderQuicHandshakeTimeoutPhase(t *testing.T) {
	blackhole, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer blackhole.Close()
	port := blackhole.LocalAddr().(*net.UDPAddr).Port
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = ProbeExtenderCarrier(ctx, DefaultConnectSettings(), netip.MustParseAddr("127.0.0.1"), ExtenderConnectModeQuic, port, "", "", nil, "example.org", 443)
	var probe *ExtenderProbeError
	if !errors.As(err, &probe) || probe.Phase != "dial/handshake" || probe.Carrier != ExtenderCarrierQuic || probe.Port != port || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("probe error = %#v (%v)", probe, err)
	}
	t.Logf("probe log: %s", err)
}
