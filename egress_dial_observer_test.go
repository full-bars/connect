package connect

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
)

// observingLogger records what ControlDialObserver delivers.
type observingLogger struct {
	Logger
	mutex  sync.Mutex
	events []observedDial
}

type observedDial struct {
	tag     string
	network string
	addr    string
	err     error
}

func (self *observingLogger) ObserveControlDial(tag string, network string, addr string, err error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.events = append(self.events, observedDial{tag, network, addr, err})
}

func TestControlDialObserverGetsTheTypedError(t *testing.T) {
	socksErr := &net.OpError{
		Op:     "socks connect",
		Net:    "tcp",
		Source: &net.TCPAddr{IP: net.IPv4(203, 0, 113, 7), Port: 1080},
		Addr:   &net.TCPAddr{IP: net.IPv4(198, 51, 100, 9), Port: 443},
		Err:    errors.New("i/o timeout"),
	}
	log := &observingLogger{Logger: DefaultLogger()}

	fail := wrapControlDial("platform", log, true, func(ctx context.Context, network string, addr string) (net.Conn, error) {
		return nil, socksErr
	})
	if _, err := fail(context.Background(), "tcp", "connect.example:443"); !errors.Is(err, socksErr) {
		t.Fatalf("the dial error must pass through unchanged, got %v", err)
	}

	ok := wrapControlDial("platform", log, true, func(ctx context.Context, network string, addr string) (net.Conn, error) {
		client, server := net.Pipe()
		server.Close()
		return client, nil
	})
	conn, err := ok(context.Background(), "tcp", "connect.example:443")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	log.mutex.Lock()
	defer log.mutex.Unlock()
	if len(log.events) != 2 {
		t.Fatalf("the observer must see every dial, got %d events", len(log.events))
	}
	failed := log.events[0]
	var opErr *net.OpError
	if failed.tag != "platform" || !errors.As(failed.err, &opErr) || opErr.Op != "socks connect" {
		t.Fatalf("the failed dial must reach the observer typed, got tag=%q err=%v", failed.tag, failed.err)
	}
	if log.events[1].err != nil {
		t.Fatalf("a successful dial reports a nil error, got %v", log.events[1].err)
	}
}

func TestControlDialObserverSeesDialsTheLogThrottles(t *testing.T) {
	log := &observingLogger{Logger: DefaultLogger()}
	dial := wrapControlDial("platform", log, true, func(ctx context.Context, network string, addr string) (net.Conn, error) {
		return nil, errors.New("refused")
	})
	// the evidence line is throttled per tag and address; the observer is not
	for range 5 {
		_, _ = dial(context.Background(), "tcp", "throttle-probe.example:443")
	}
	log.mutex.Lock()
	defer log.mutex.Unlock()
	if len(log.events) != 5 {
		t.Fatalf("all 5 dials must be observed even though the log line is throttled, got %d", len(log.events))
	}
}

func TestControlDialWithoutAnObserverStillWorks(t *testing.T) {
	dial := wrapControlDial("platform", DefaultLogger(), true, func(ctx context.Context, network string, addr string) (net.Conn, error) {
		return nil, errors.New("refused")
	})
	if _, err := dial(context.Background(), "tcp", "plain.example:443"); err == nil {
		t.Fatal("expected the dial error")
	}
}
