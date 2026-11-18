package extender

import (
	"context"
	"io"
	"net"
	"testing"
	"testing/synctest"
	"time"
)

// Each relayed byte is counted once on its carrier, on the side it travelled, and
// the carriers always sum to the totals.
func TestExtenderRelayCountsBytesPerCarrier(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		settings := DefaultExtenderSettings()
		server := &ExtenderServer{settings: settings}
		cases := []struct {
			carrier string
			in, out int
		}{
			{ExtenderCarrierTcp443, 100, 40},
			{ExtenderCarrierUdp443, 7, 900},
			{ExtenderCarrierUdp4053, 55, 66},
			{ExtenderCarrierUdp53, 1, 2},
		}
		for _, tc := range cases {
			clientConn, clientPeer := net.Pipe()
			forwardConn, forwardPeer := net.Pipe()
			ctx, cancel := context.WithCancel(newExtenderRequestContextWithCarrier(context.Background(), "", tc.carrier))
			done := make(chan struct{})
			go func() {
				defer close(done)
				server.relay(ctx, cancel, clientConn, forwardConn)
			}()
			go clientPeer.Write(make([]byte, tc.in))
			if _, err := io.ReadFull(forwardPeer, make([]byte, tc.in)); err != nil {
				t.Fatal(err)
			}
			go forwardPeer.Write(make([]byte, tc.out))
			if _, err := io.ReadFull(clientPeer, make([]byte, tc.out)); err != nil {
				t.Fatal(err)
			}
			cancel()
			<-done
			clientPeer.Close()
			forwardPeer.Close()
		}
		stats := server.CarrierByteStats(time.Now())
		var sumIn, sumOut uint64
		for _, tc := range cases {
			got := stats[tc.carrier]
			if got.IngressBytes != uint64(tc.in) || got.EgressBytes != uint64(tc.out) {
				t.Errorf("%s: in %d out %d, want in %d out %d", tc.carrier, got.IngressBytes, got.EgressBytes, tc.in, tc.out)
			}
			if got.ConnsTotal != 1 || got.LiveConns != 0 {
				t.Errorf("%s: conns total %d live %d, want 1 and 0", tc.carrier, got.ConnsTotal, got.LiveConns)
			}
			sumIn += got.IngressBytes
			sumOut += got.EgressBytes
		}
		if sumIn != uint64(server.ingressByteCount.Load()) || sumOut != uint64(server.egressByteCount.Load()) {
			t.Errorf("carriers sum to in %d out %d, totals are in %d out %d", sumIn, sumOut, server.ingressByteCount.Load(), server.egressByteCount.Load())
		}
	})
}

// The process wide figures follow every server's relays, give a rate from two
// samples, and reset with the other process wide carrier counters.
func TestExtenderProcessCarrierByteStats(t *testing.T) {
	ResetProcessCarrierStats()
	defer ResetProcessCarrierStats()
	start := time.Unix(1_800_000_000, 0)
	SampleProcessCarrierRates(start)
	a := &ExtenderServer{}
	b := &ExtenderServer{}
	ctx := newExtenderRequestContextWithCarrier(context.Background(), "", ExtenderCarrierTcp443)
	a.noteRelayedConnOpen(ctx)
	b.noteRelayedConnOpen(ctx)
	a.noteCarrierBytes(ctx, 6000, true)
	b.noteCarrierBytes(ctx, 4000, true)
	a.noteCarrierBytes(ctx, 3000, false)
	SampleProcessCarrierRates(start.Add(10 * time.Second))
	got := ProcessCarrierByteStats(start.Add(10 * time.Second))[ExtenderCarrierTcp443]
	if got.IngressBytes != 10000 || got.EgressBytes != 3000 {
		t.Fatalf("process bytes in %d out %d, want 10000 and 3000", got.IngressBytes, got.EgressBytes)
	}
	if got.RateInBps != 1000 || got.RateOutBps != 300 {
		t.Fatalf("process rate in %d out %d, want 1000 and 300", got.RateInBps, got.RateOutBps)
	}
	if got.LiveConns != 2 || got.ConnsTotal != 2 {
		t.Fatalf("process conns live %d total %d, want 2 and 2", got.LiveConns, got.ConnsTotal)
	}
	a.noteRelayedConnClose(ctx)
	if live := ProcessCarrierByteStats(start.Add(11 * time.Second))[ExtenderCarrierTcp443].LiveConns; live != 1 {
		t.Fatalf("live conns after one close = %d, want 1", live)
	}
	if other := ProcessCarrierByteStats(start)[ExtenderCarrierUdp443]; other.IngressBytes != 0 || other.EgressBytes != 0 {
		t.Fatalf("another carrier counted bytes: %+v", other)
	}
	ResetProcessCarrierStats()
	if after := ProcessCarrierByteStats(start)[ExtenderCarrierTcp443]; after.IngressBytes != 0 || after.ConnsTotal != 0 {
		t.Fatalf("reset left %+v", after)
	}
}
