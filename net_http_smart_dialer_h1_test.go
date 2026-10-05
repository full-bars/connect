package connect

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// h1TestDialer is a dialer with a TLS dial function that trusts the test
// server's self-signed certificate, which is what makes it websocket capable.
func h1TestDialer() *clientDialer {
	dialer := &clientDialer{
		description:   "counting",
		priority:      0,
		minimumWeight: 0.25,
		settings:      DefaultClientStrategySettings(),
	}
	dialer.dialTlsContext = func(ctx context.Context, network string, address string) (net.Conn, error) {
		d := &tls.Dialer{Config: &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"http/1.1"}}}
		return d.DialContext(ctx, network, address)
	}
	return dialer
}

// The provider's long-lived connection is made through H1DialContextWithDialer
// (H1+ by default), not the plain websocket dial, so its establishment must
// feed the same measurement or the preference would never see the connection
// the provider actually keeps.
func TestSmartDialerH1PlusDialFeedsConnectSamples(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := AcceptFramedUpgrade(w, r, H1FramerProtocol, time.Second)
		if err != nil {
			return
		}
		defer conn.Close()
		<-release
	}))
	defer server.Close()
	defer close(release)

	dialer := h1TestDialer()
	strategy := wiredStrategy(t, dialer)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := strategy.H1DialContextWithDialer(ctx, "wss"+strings.TrimPrefix(server.URL, "https"), nil, 1200, true, nil)
	if err != nil {
		t.Fatalf("h1+ dial: %v", err)
	}
	defer conn.Close()

	if _, samples := dialer.measuredLatency(); samples < 1 {
		t.Fatalf("no connect sample after a fresh H1+ connection")
	}
}

// When the server does not speak the framed upgrade the dial falls back to a
// websocket on a new connection; that connection is the one carrying traffic,
// so it is the one that counts.
func TestSmartDialerH1FallbackToWebSocketFeedsConnectSamples(t *testing.T) {
	upgrader := websocket.Upgrader{}
	release := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		<-release
	}))
	defer server.Close()
	defer close(release)

	dialer := h1TestDialer()
	strategy := wiredStrategy(t, dialer)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := strategy.H1DialContextWithDialer(ctx, "wss"+strings.TrimPrefix(server.URL, "https"), nil, 1200, true, nil)
	if err != nil {
		t.Fatalf("h1 dial with websocket fallback: %v", err)
	}
	defer conn.Close()

	if _, samples := dialer.measuredLatency(); samples < 1 {
		t.Fatalf("no connect sample after a fresh websocket-fallback connection")
	}
}
