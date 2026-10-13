package connect

import "testing"

func TestParseGovernMode_DefaultsToShadow(t *testing.T) {
	for raw, want := range map[string]governMode{
		"": governShadow, "   ": governShadow, "garbage": governShadow, "shadow": governShadow,
		"enforce": governEnforce, "ENFORCE": governEnforce, "on": governEnforce, "1": governEnforce, "true": governEnforce,
		"off": governOff, "0": governOff, "false": governOff, "no": governOff,
	} {
		if got := parseGovernMode(raw); got != want {
			t.Errorf("parseGovernMode(%q) = %v, want %v", raw, got, want)
		}
	}
}

func TestSetGovernModeForTest_Restores(t *testing.T) {
	before := currentGovernMode()
	restore := setGovernModeForTest(governEnforce)
	if currentGovernMode() != governEnforce {
		t.Fatal("mode not swapped")
	}
	restore()
	if currentGovernMode() != before {
		t.Fatal("mode not restored")
	}
}
