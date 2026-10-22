package connect

import "testing"

// The protections change when frames go on the wire, so only an explicit value
// may turn enforcement on. An unset or mistyped value must leave the caller in
// shadow, where nothing is ever refused or delayed.
func TestParseGovernModeRequiresExplicitEnforcement(t *testing.T) {
	for _, raw := range []string{"", " ", "shdow", "yes please"} {
		if got := parseGovernMode(raw); got != governShadow {
			t.Fatalf("parseGovernMode(%q) = %v, want shadow", raw, got)
		}
	}
	for _, raw := range []string{"enforce", "ENFORCE", " on ", "1", "true", "yes"} {
		if got := parseGovernMode(raw); got != governEnforce {
			t.Fatalf("parseGovernMode(%q) = %v, want enforce", raw, got)
		}
	}
	for _, raw := range []string{"off", "OFF", "0", "false", "no"} {
		if got := parseGovernMode(raw); got != governOff {
			t.Fatalf("parseGovernMode(%q) = %v, want off", raw, got)
		}
	}
	if got := parseGovernMode("shadow"); got != governShadow {
		t.Fatalf("parseGovernMode(shadow) = %v, want shadow", got)
	}
}
