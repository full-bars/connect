package connect

import (
	"strings"
	"testing"
)

// streamKindOf and the live-minus-closed invariant are the parts of the ported
// metrics that do not need a live buffer: they are pure over the counters.
func TestStreamKindOfPort(t *testing.T) {
	a, b := NewId(), NewId()
	if streamKindOf(nil, &b) != streamKindSingle || streamKindOf(&a, nil) != streamKindSingle || streamKindOf(nil, nil) != streamKindSingle {
		t.Fatal("a stream with at most one end is single")
	}
	if streamKindOf(&a, &b) != streamKindTwoEnded {
		t.Fatal("a stream with both ends is two_ended")
	}
}

// The exposition must carry the new series and must not carry the v33-only
// dmca series, which the pinned lineage does not define.
func TestStreamMetricsPrometheusExposition(t *testing.T) {
	out := StreamMetricsPrometheus()
	for _, want := range []string{
		"urnet_stream_sequences_by_kind",
		"urnet_stream_open_requests_total",
		"urnet_stream_control_frames_total",
		"urnet_tls_handshakes_total",
		"urnet_dial_backoff_long_tier_active",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("exposition is missing %q", want)
		}
	}
	if strings.Contains(out, "dmca") {
		t.Fatal("exposition must not carry the dmca series; the pinned lineage has none")
	}
}
