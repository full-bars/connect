package extender

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/urnetwork/connect"
	"google.golang.org/protobuf/proto"
)

func TestExtenderServedCountsAcceptedForwards(t *testing.T) {
	endpoint := newRelayEndpoint(t, net.IPv6loopback.String())
	fixture := newRelayStatsFixture(t, endpoint)
	if got := fixture.server.ServedCounts(); len(got) != 0 {
		t.Fatalf("initial served counts = %v", got)
	}
	for i, carrier := range []string{connect.ExtenderCarrierTcp, connect.ExtenderCarrierQuic, connect.ExtenderCarrierDns} {
		if err := relayTestSession(fixture, endpoint, i+1, carrier); err != nil {
			t.Fatal(err)
		}
		if got := fixture.server.ServedCounts()["forward"]; got != uint64(i+1) {
			t.Fatalf("%s: served = %d, want %d", carrier, got, i+1)
		}
	}
	carrierServed := fixture.server.CarrierServedCounts()
	for _, carrierPort := range []string{
		ExtenderCarrierTcp443,
		ExtenderCarrierUdp443,
		ExtenderCarrierUdp4053,
	} {
		if carrierServed[carrierPort] != 1 {
			t.Fatalf("carrier %s served = %d, want 1 (%v)", carrierPort, carrierServed[carrierPort], carrierServed)
		}
	}
}

func TestExtenderServedIgnoresRefusalAndProbe(t *testing.T) {
	fixture := newExtenderFixture(t, net.IPv6loopback.String(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, _, err := connect.DialExtender(ctx, fixture.connectSettings(), fixture.extenderConfig(connect.ExtenderCarrierTcp), &connect.ExtenderDial{
		DestinationHost: "blocked.example", DestinationPort: 443,
	})
	if err == nil {
		conn.Close()
		t.Fatal("blocked forward was accepted")
	}
	conn, _, err = connect.DialExtender(ctx, fixture.connectSettings(), fixture.extenderConfig(connect.ExtenderCarrierTcp), &connect.ExtenderDial{Service: connect.ExtenderServiceProbe})
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if got := fixture.server.ServedCounts(); len(got) != 0 {
		t.Fatalf("refusal or probe counted as served: %v", got)
	}
	if got := fixture.server.RefusalCounts()["destination authorization"]; got != 1 {
		t.Fatalf("destination authorization refusals = %d, want 1", got)
	}
	if got := fixture.server.CarrierRefusalCounts()[ExtenderCarrierTcp443]["destination authorization"]; got != 1 {
		t.Fatalf("tcp-443 destination authorization refusals = %d, want 1", got)
	}
}

func TestExtenderServedCountsV1Forwards(t *testing.T) {
	fixture := newExtenderFixture(t, net.IPv6loopback.String(), func(settings *ExtenderSettings) {
		settings.DialContext = func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("test dial failure")
		}
	})
	for i, host := range []string{"blocked.example", testRelayHost, testRelayHost} {
		header := newTestExtenderHeader(host, 443, testSecret)
		if i == 2 {
			header.Service = 77
		}
		data, err := proto.Marshal(header)
		if err != nil {
			t.Fatal(err)
		}
		_ = sendV1Header(t, fixture, data)
		want := uint64(i)
		if got := fixture.server.ServedCounts()["forward"]; got != want {
			t.Fatalf("case %d: served = %d, want %d", i, got, want)
		}
	}
	if got := fixture.server.CarrierServedCounts()[ExtenderCarrierTcp443]; got != 2 {
		t.Fatalf("tcp-443 carrier served for v1 = %d, want 2", got)
	}
	if got := fixture.server.CarrierRefusalCounts()[ExtenderCarrierTcp443]["destination authorization"]; got != 1 {
		t.Fatalf("tcp-443 carrier refused for v1 = %d, want 1", got)
	}
}
