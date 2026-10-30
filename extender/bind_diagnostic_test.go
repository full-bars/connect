package extender

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"syscall"
	"testing"

	"github.com/urnetwork/connect"
)

func TestExtenderBindFailureLogNamesPortAndTransport(t *testing.T) {
	var logs bytes.Buffer
	old := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(old)
	occupied, err := net.ListenPacket("udp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	port := occupied.LocalAddr().(*net.UDPAddr).Port
	settings := DefaultExtenderSettings()
	settings.ListenPacket = func(network, address string) (net.PacketConn, error) {
		if address == fmt.Sprintf(":%d", port) {
			return net.ListenPacket(network, address)
		}
		return net.ListenPacket("udp", "127.0.0.1:0")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := NewExtenderServer(ctx, nil, nil, map[int][]connect.ExtenderConnectMode{
		18443: {connect.ExtenderConnectModeDns},
		port:  {connect.ExtenderConnectModeQuic},
	}, &net.Dialer{}, settings)
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	<-server.Listening()
	server.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	text := logs.String()
	for _, want := range []string{
		fmt.Sprintf("bind attempt transport=udp port=%d carrier=quic", port),
		fmt.Sprintf("bind failed transport=udp port=%d carrier=quic", port),
		"port already in use",
		"bound transport=udp port=18443 carrier=dns",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in %s", want, text)
		}
	}
	if strings.Contains(text, fmt.Sprintf("bound transport=udp port=%d carrier=quic", port)) {
		t.Fatalf("failed bind logged success: %s", text)
	}
	t.Logf("bind logs: %s", text)
	if detail := bindOwnerDetail("udp", port, fmt.Errorf("wrapped: %w", syscall.EADDRINUSE)); !strings.Contains(detail, "port already in use") {
		t.Fatal(detail)
	}
	if detail := bindOwnerDetail("udp", port, errors.New("other")); detail != "" {
		t.Fatal(detail)
	}
}
