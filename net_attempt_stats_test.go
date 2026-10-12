package connect

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"golang.org/x/net/proxy"
)

// serveSocks5 accepts SOCKS5 CONNECTs without authentication and answers every
// request with success, then echoes whatever the client writes.
func serveSocks5(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				head := make([]byte, 2)
				if _, err := io.ReadFull(conn, head); err != nil {
					return
				}
				methods := make([]byte, head[1])
				if _, err := io.ReadFull(conn, methods); err != nil {
					return
				}
				conn.Write([]byte{5, 0})
				request := make([]byte, 4)
				if _, err := io.ReadFull(conn, request); err != nil {
					return
				}
				switch request[3] {
				case 1:
					io.ReadFull(conn, make([]byte, 4+2))
				case 3:
					size := make([]byte, 1)
					io.ReadFull(conn, size)
					io.ReadFull(conn, make([]byte, int(size[0])+2))
				default:
					io.ReadFull(conn, make([]byte, 16+2))
				}
				conn.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0})
				io.Copy(conn, conn)
			}()
		}
	}()
	t.Cleanup(func() { listener.Close() })
	return listener
}

// A proxied dial for a governed connect attempt is counted on that attempt: one
// dial, and the bytes that cross the connection, handshake included. A dial with
// no attempt in its context is returned unwrapped.
func TestProxySettings_DialCountsOnTheAttemptInItsContext(t *testing.T) {
	listener := serveSocks5(t)
	settings := &ProxySettings{Network: "tcp", Address: listener.Addr().String()}
	dial := settings.NewDialContext(context.Background(), proxy.Direct)

	stats := &attemptStats{}
	ctx, cancel := context.WithTimeout(withAttemptStats(context.Background(), stats), 5*time.Second)
	defer cancel()
	conn, err := dial(ctx, "tcp", "127.0.0.1:443")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, ok := conn.(*attemptConn); !ok {
		t.Fatalf("a governed attempt's connection is %T, want it counted", conn)
	}
	if stats.dials.Load() != 1 {
		t.Fatalf("dials = %d, want 1", stats.dials.Load())
	}
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(conn, make([]byte, 5)); err != nil {
		t.Fatal(err)
	}
	// the SOCKS handshake itself moved bytes both ways before the 5 echoed
	if stats.tx.Load() < 5 || stats.rx.Load() < 5 {
		t.Fatalf("tx=%d rx=%d, want at least the 5 echoed bytes each way", stats.tx.Load(), stats.rx.Load())
	}

	plain, err := dial(context.Background(), "tcp", "127.0.0.1:443")
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	if _, ok := plain.(*attemptConn); ok {
		t.Fatal("a dial outside any attempt must not be wrapped")
	}
}

// A dial that fails is still a dial the attempt paid for.
func TestProxySettings_FailedDialStillCountsOnTheAttempt(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close() // nothing listens: the proxy is unreachable
	settings := &ProxySettings{Network: "tcp", Address: address}
	stats := &attemptStats{}
	ctx, cancel := context.WithTimeout(withAttemptStats(context.Background(), stats), 5*time.Second)
	defer cancel()
	if _, err := settings.NewDialContext(ctx, proxy.Direct)(ctx, "tcp", "127.0.0.1:443"); err == nil {
		t.Fatal("expected the dial to fail")
	}
	if stats.dials.Load() != 1 {
		t.Fatalf("dials = %d, want 1", stats.dials.Load())
	}
}

// The stats ride the context through the strategy's attempt contexts down to the
// proxy dial, so a platform dial through a proxy is counted without any change
// to the dial path's signatures.
func TestClientStrategy_PlatformDialCountsOnTheAttemptStats(t *testing.T) {
	listener := serveSocks5(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientStrategySettings()
	settings.RequestTimeout = 500 * time.Millisecond
	settings.ReconnectTimeout = 20 * time.Millisecond
	settings.MinNextConnectDelay = 0
	settings.MaxNextConnectDelay = 0
	settings.ProxySettings = &ProxySettings{Network: "tcp", Address: listener.Addr().String()}
	strategy := NewClientStrategy(ctx, settings)

	stats := &attemptStats{}
	_, _, err := strategy.WsDialContext(withAttemptStats(ctx, stats), "ws://127.0.0.1:1/", nil)
	if err == nil {
		t.Fatal("the echo proxy is not a websocket server")
	}
	if stats.dials.Load() == 0 {
		t.Fatal("the proxy dial was not counted on the attempt")
	}
	if stats.rx.Load() == 0 || stats.tx.Load() == 0 {
		t.Fatalf("tx=%d rx=%d: the handshake bytes were not counted", stats.tx.Load(), stats.rx.Load())
	}
}
