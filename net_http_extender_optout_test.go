package connect

import (
	"testing"
)

func TestDefaultClientStrategySettingsDisableExtenderNetworkDefaultsToFalse(t *testing.T) {
	settings := DefaultClientStrategySettings()
	if settings == nil {
		t.Fatal("DefaultClientStrategySettings returned nil")
	}
	if settings.DisableExtenderNetwork {
		t.Fatalf("DisableExtenderNetwork = true, want false")
	}
}
