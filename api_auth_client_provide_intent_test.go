package connect

import (
	"encoding/json"
	"testing"
)

// The platform reads `provide_intent` on POST /network/auth-client and records
// provider status only at creation. The key must be exactly that and must be
// omitted when false so older platforms see an unchanged request.
func TestAuthNetworkClientArgsProvideIntentJSON(t *testing.T) {
	on, err := json.Marshal(&AuthNetworkClientArgs{Description: "d", ProvideIntent: true})
	if err != nil {
		t.Fatal(err)
	}
	var onMap map[string]any
	_ = json.Unmarshal(on, &onMap)
	if onMap["provide_intent"] != true {
		t.Fatalf("declared request = %s, want provide_intent true", on)
	}

	off, err := json.Marshal(&AuthNetworkClientArgs{Description: "d"})
	if err != nil {
		t.Fatal(err)
	}
	var offMap map[string]any
	_ = json.Unmarshal(off, &offMap)
	if _, present := offMap["provide_intent"]; present {
		t.Fatalf("undeclared request = %s, provide_intent must be omitted", off)
	}
}
