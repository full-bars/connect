package connect

import (
	"context"
	"encoding/base64"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// A lifecycle-bound out-of-band control retries a transient failure, and the
// callback still fires exactly once with the successful result.
func TestSendControlRetriesTransientFailure(t *testing.T) {
	oob := &ApiOutOfBandControl{requests: newLifecycleAdmission()}
	ctx := context.Background()

	var calls atomic.Int64
	var callbacks atomic.Int64
	connectControl := func(_ *ConnectControlArgs, callback ConnectControlCallback) {
		if calls.Add(1) < 3 {
			callback.Result(nil, errors.New("synthetic control timeout"))
			return
		}
		packBytes, err := ProtoMarshal(&protocol.Pack{})
		if err != nil {
			t.Error(err)
			return
		}
		callback.Result(&ConnectControlResult{Pack: EncodeBase64(base64.StdEncoding, packBytes)}, nil)
	}

	frame, err := ToFrame(&protocol.SimpleMessage{Content: "retry"}, DefaultProtocolVersion)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	oob.sendControl(connectControl, ctx, []*protocol.Frame{frame}, func(_ []*protocol.Frame, err error) {
		callbacks.Add(1)
		result <- err
	}, true)

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("callback err = %v, want nil after retries", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("callback did not fire")
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
	if got := callbacks.Load(); got != 1 {
		t.Fatalf("callbacks = %d, want exactly 1", got)
	}
}

// A caller-context (one-shot) control never retries: shutdown cleanup must not
// be delayed by backoff.
func TestSendControlWithCtxDoesNotRetry(t *testing.T) {
	oob := &ApiOutOfBandControl{requests: newLifecycleAdmission()}
	ctx := context.Background()

	var calls atomic.Int64
	connectControl := func(_ *ConnectControlArgs, callback ConnectControlCallback) {
		calls.Add(1)
		callback.Result(nil, errors.New("synthetic control timeout"))
	}

	frame, err := ToFrame(&protocol.SimpleMessage{Content: "one-shot"}, DefaultProtocolVersion)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	oob.sendControl(connectControl, ctx, []*protocol.Frame{frame}, func(_ []*protocol.Frame, err error) {
		result <- err
	}, false)

	select {
	case err := <-result:
		if err == nil {
			t.Fatal("want the failure surfaced, got nil")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("callback did not fire")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("attempts = %d, want exactly 1 (no retry)", got)
	}
}
