package connect

import (
	"encoding/json"
	"testing"
)

func TestExtenderCarrierRefusalTimingPresence(t *testing.T) {
	refusal := &ExtenderCarrierRefusal{Carrier: "quic", Port: 443, Error: "no answer"}
	for _, c := range []struct {
		input string
		want  string
	}{
		{`{"elapsed_ms":12,"budget_left_ms":6400}`, "quic:443 after 12ms (6.4s left): no answer"},
		{`{}`, "quic:443: no answer"},
		{`{"elapsed_ms":0}`, "quic:443 after 0s: no answer"},
		{`{"budget_left_ms":0}`, "quic:443 (0s left): no answer"},
		{`{"elapsed_ms":null,"budget_left_ms":null}`, "quic:443: no answer"},
		{`{"elapsed_ms":0,"budget_left_ms":0}`, "quic:443 after 0s (0s left): no answer"},
		{`null`, "quic:443 after 0s (0s left): no answer"},
	} {
		if err := json.Unmarshal([]byte(c.input), refusal); err != nil {
			t.Fatal(err)
		}
		if got := extenderActivateCarrierRefusalsText([]*ExtenderCarrierRefusal{refusal}); got != c.want {
			t.Fatalf("%s: got %q, want %q", c.input, got, c.want)
		}
	}
}
