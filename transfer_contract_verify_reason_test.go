package connect

import (
	"testing"

	"github.com/urnetwork/connect/protocol"
)

func TestVerifyFailureReason(t *testing.T) {
	network := protocol.ProvideMode_Network
	stream := protocol.ProvideMode_Stream
	public := protocol.ProvideMode_Public

	allModes := map[protocol.ProvideMode]bool{network: true, stream: true, public: true}
	allSecrets := map[protocol.ProvideMode][]byte{
		network: []byte("network-key"),
		stream:  []byte("stream-key"),
		public:  []byte("public-key"),
	}
	hmacOK := func([]byte) bool { return true }
	hmacBad := func([]byte) bool { return false }

	cases := []struct {
		name    string
		paused  bool
		modes   map[protocol.ProvideMode]bool
		secrets map[protocol.ProvideMode][]byte
		mode    protocol.ProvideMode
		verify  func([]byte) bool
		want    string
	}{
		{"verifies", false, allModes, allSecrets, network, hmacOK, ""},
		{"paused blocks public", true, allModes, allSecrets, public, hmacOK, "paused"},
		{"paused allows network", true, allModes, allSecrets, network, hmacOK, ""},
		{"paused allows stream", true, allModes, allSecrets, stream, hmacOK, ""},
		{"mode not enabled", false, map[protocol.ProvideMode]bool{}, allSecrets, network, hmacOK, "mode-not-enabled"},
		{"no secret key", false, allModes, map[protocol.ProvideMode][]byte{}, network, hmacOK, "no-secret-key"},
		{"hmac mismatch", false, allModes, allSecrets, network, hmacBad, "hmac-mismatch"},
	}
	for _, c := range cases {
		got := verifyFailureReason(c.paused, c.modes, c.secrets, c.mode, c.verify)
		if got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}
