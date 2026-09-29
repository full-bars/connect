package connect

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (self roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return self(request)
}

// wiredDialer is a measured dialer whose api client answers 200 and counts the
// requests it carried, so a test can see which transport the strategy chose.
func wiredDialer(description string, priority int, cost time.Duration, carried *atomic.Int32) *clientDialer {
	dialer := testDialer(description, priority, 0.25, 10, cost, 0)
	dialer.settings = DefaultClientStrategySettings()
	dialer.httpClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		carried.Add(1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     http.Header{},
			Body:       io.NopCloser(strings.NewReader("ok")),
			Request:    request,
		}, nil
	})}
	return dialer
}

func wiredStrategy(t *testing.T, dialers ...*clientDialer) *ClientStrategy {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	strategy := NewClientStrategyWithDefaults(ctx)
	strategy.mutex.Lock()
	strategy.dialers = map[*clientDialer]bool{}
	for _, dialer := range dialers {
		strategy.dialers[dialer] = true
	}
	strategy.mutex.Unlock()
	return strategy
}

func getRequest(t *testing.T) *http.Request {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, "https://api.example.invalid/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

// The serial path tries the priority-0 transport first; with the smart dialer
// on it must try the transport that measured cheaper first instead, and with
// it off the priority order must be untouched.
func TestSmartDialerOrdersTheSerialPath(t *testing.T) {
	for _, on := range []bool{true, false} {
		withSmartDialer(t, on)
		var fragmentCarried, normalCarried atomic.Int32
		fragment := wiredDialer("fragment", 0, 600*time.Millisecond, &fragmentCarried)
		normal := wiredDialer("normal", 25, 80*time.Millisecond, &normalCarried)
		strategy := wiredStrategy(t, fragment, normal)

		if _, err := strategy.HttpSerial(getRequest(t), getRequest(t)); err != nil {
			t.Fatalf("smart=%v: %v", on, err)
		}
		wantNormal := int32(0)
		wantFragment := int32(1)
		if on {
			wantNormal, wantFragment = 1, 0
		}
		if normalCarried.Load() != wantNormal || fragmentCarried.Load() != wantFragment {
			t.Fatalf("smart=%v: normal carried %d, fragment carried %d; want %d and %d",
				on, normalCarried.Load(), fragmentCarried.Load(), wantNormal, wantFragment)
		}
	}
}

// Same for the parallel path, whose serial-first list is built separately.
func TestSmartDialerOrdersTheParallelPath(t *testing.T) {
	for _, on := range []bool{true, false} {
		withSmartDialer(t, on)
		var fragmentCarried, normalCarried atomic.Int32
		fragment := wiredDialer("fragment", 0, 600*time.Millisecond, &fragmentCarried)
		normal := wiredDialer("normal", 25, 80*time.Millisecond, &normalCarried)
		strategy := wiredStrategy(t, fragment, normal)

		if _, err := strategy.HttpParallel(getRequest(t)); err != nil {
			t.Fatalf("smart=%v: %v", on, err)
		}
		wantNormal := int32(0)
		wantFragment := int32(1)
		if on {
			wantNormal, wantFragment = 1, 0
		}
		if normalCarried.Load() != wantNormal || fragmentCarried.Load() != wantFragment {
			t.Fatalf("smart=%v: normal carried %d, fragment carried %d; want %d and %d",
				on, normalCarried.Load(), fragmentCarried.Load(), wantNormal, wantFragment)
		}
	}
}

// A real dial through each api path leaves a connect sample on the dialer
// that carried it, so the measurement the preference ranks by is actually fed.
func TestSmartDialerApiPathsFeedConnectSamples(t *testing.T) {
	server := newTlsCountingServer(t)

	for name, run := range map[string]func(*ClientStrategy, *http.Request) error{
		"parallel": func(s *ClientStrategy, r *http.Request) error { _, err := s.HttpParallel(r); return err },
		"serial": func(s *ClientStrategy, r *http.Request) error {
			_, err := s.HttpSerial(r, r.Clone(r.Context()))
			return err
		},
	} {
		dialer := &clientDialer{
			description:   "counting",
			priority:      0,
			minimumWeight: 0.25,
			settings:      DefaultClientStrategySettings(),
		}
		// a private transport per case: the server's shared client would hand
		// the second case the first case's kept-alive connection, which is a
		// reuse and (correctly) yields no sample
		transport := server.Client().Transport.(*http.Transport).Clone()
		transport.DisableKeepAlives = true
		dialer.httpClient = &http.Client{Transport: transport}
		strategy := wiredStrategy(t, dialer)

		request, err := http.NewRequest(http.MethodGet, server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := run(strategy, request); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if _, samples := dialer.measuredLatency(); samples < 1 {
			t.Fatalf("%s: no connect sample after a fresh connection", name)
		}
	}
}

func newTlsCountingServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	return server
}

// The WebSocket dial is the connection the provider actually keeps, so its
// establishment must feed the same measurement.
func TestSmartDialerWebSocketDialFeedsConnectSamples(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		conn.Close()
	}))
	defer server.Close()

	dialer := &clientDialer{
		description:   "counting",
		priority:      0,
		minimumWeight: 0.25,
		settings:      DefaultClientStrategySettings(),
	}
	// only a dialer with a TLS dial function supports websockets; this one
	// dials the test server's self-signed certificate
	dialer.dialTlsContext = func(ctx context.Context, network string, address string) (net.Conn, error) {
		d := &tls.Dialer{Config: &tls.Config{InsecureSkipVerify: true}}
		return d.DialContext(ctx, network, address)
	}
	strategy := wiredStrategy(t, dialer)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	url := "wss" + strings.TrimPrefix(server.URL, "https")
	conn, _, err := strategy.WsDialContext(ctx, url, http.Header{})
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	conn.Close()

	if _, samples := dialer.measuredLatency(); samples < 1 {
		t.Fatalf("no connect sample after a fresh websocket connection")
	}
}
