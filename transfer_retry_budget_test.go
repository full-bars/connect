package connect

import (
	"strings"
	"testing/synctest"

	"github.com/urnetwork/connect/protocol"
	"testing"
	"time"
)

func newTestRetryBudget() (*transferRetryBudget, *time.Time) {
	now := time.Unix(3_000_000, 0)
	b := newTransferRetryBudget(func() time.Time { return now })
	return b, &now
}

func TestRetryBudget_StartsWithAGrantSoAFreshProcessCanRecover(t *testing.T) {
	b, _ := newTestRetryBudget()
	if !b.allow(1000) {
		t.Fatal("a fresh budget must allow a resend")
	}
}

func TestRetryBudget_ResendsAreChargedAgainstWhatWasFirstSent(t *testing.T) {
	b, _ := newTestRetryBudget()
	b.reset(0)

	if b.allow(1) {
		t.Fatal("an empty budget must refuse")
	}
	b.earn(10_000) // 10 KB first-sent earns 25%
	if !b.allow(2500) {
		t.Fatal("2.5 KB of resends is exactly the 25% earned")
	}
	if b.allow(1) {
		t.Fatal("that spent it all")
	}
}

func TestRetryBudget_FloorRefillsOverTimeSoQuietFlowsStillRecover(t *testing.T) {
	b, now := newTestRetryBudget()
	b.reset(0)
	*now = now.Add(time.Second)
	if !b.allow(retryBudgetFloorBytesPerSecond) {
		t.Fatal("one second must refill the floor rate")
	}
	if b.allow(1000) {
		t.Fatal("but not more than the floor rate")
	}
}

func TestRetryBudget_BurstIsCapped(t *testing.T) {
	b, now := newTestRetryBudget()
	b.reset(0)
	b.earn(1 << 40)
	*now = now.Add(24 * time.Hour)
	if !b.allow(retryBudgetBurstBytes) {
		t.Fatal("a full bucket must pay out its burst")
	}
	if b.allow(1) {
		t.Fatal("the bucket never holds more than the burst, however long it sits")
	}
}

func TestRetryBudget_RefusalsAreNotCharged(t *testing.T) {
	b, _ := newTestRetryBudget()
	b.reset(100)
	if b.allow(1000) {
		t.Fatal("too big")
	}
	if !b.allow(100) {
		t.Fatal("a refused resend must leave the tokens alone")
	}
}

// The incident: a peer that never acks. Every unacked frame is resent on a
// backed-off timer, so a node sends many times what it delivers. With the
// budget the resends are bounded by what was first sent plus a small floor.
// The same traffic runs once with the budget off and once enforced, on virtual
// time, and the enforced run must put far fewer rewrites on the wire.
func countRewritesToSilentPeer(t *testing.T, mode governMode) (rewrites int, deferred int64) {
	restore := setGovernModeForTest(mode)
	defer restore()
	// freeze the budget clock so only first sends earn tokens: the 64 KiB/s floor
	// would otherwise cover this small test's whole volume
	frozen := time.Now()
	previousNow := transferResendBudget.now
	transferResendBudget.now = func() time.Time { return frozen }
	defer func() { transferResendBudget.now = previousNow }()
	savedTokens, savedLast := transferResendBudget.snapshotForTest()
	defer transferResendBudget.restoreForTest(savedTokens, savedLast)
	transferResendBudget.reset(2_000)
	before := TransferSendStatsNow()
	synctest.Test(t, func(t *testing.T) {
		fixture, _, _ := newAckRetirementFixture(t, TransportTypeH1, 2, NewNoopLogger(), func(settings *ClientSettings) {
			defaults := DefaultSendBufferSettings()
			settings.SendBufferSettings.AckTimeout = defaults.AckTimeout
			settings.SendBufferSettings.MinResendInterval = time.Second
			settings.SendBufferSettings.RttMinResendInterval = defaults.RttMinResendInterval
			settings.SendBufferSettings.MaxResendInterval = 2 * time.Second
		})
		for i := 0; i < 24; i++ {
			frame := RequireToFrameWithDefaultProtocolVersion(&protocol.SimpleMessage{Content: strings.Repeat("x", 800)})
			admitted, err := fixture.sender.SendWithTimeoutDetailed(frame, fixture.receiver.ClientId(), func(error) {}, time.Second, sendPackRecoveryOption{})
			if !admitted || err != nil {
				MessagePoolReturn(frame.MessageBytes)
				t.Fatalf("admission %d = %t, %v", i, admitted, err)
			}
		}
		rewrites = advanceDroppingRewrites(fixture, 45*time.Second)
	})
	return rewrites, TransferSendStatsNow().ResendDeferredFrames - before.ResendDeferredFrames
}

func TestSendSequence_ResendsToANonAckingPeerAreBudgeted(t *testing.T) {
	assertMessagePoolOwnership(t)
	unbudgeted, unbudgetedDeferred := countRewritesToSilentPeer(t, governOff)
	budgeted, budgetedDeferred := countRewritesToSilentPeer(t, governEnforce)
	t.Logf("unbudgeted rewrites=%d budgeted=%d deferred=%d", unbudgeted, budgeted, budgetedDeferred)
	if unbudgeted == 0 {
		t.Fatal("the unbudgeted run never rewrote; the test measures nothing")
	}
	if unbudgetedDeferred != 0 {
		t.Fatalf("budget off deferred %d resends", unbudgetedDeferred)
	}
	if budgetedDeferred == 0 {
		t.Fatal("refused resends must be counted as deferred")
	}
	if budgeted == 0 {
		t.Fatal("the budget must still let some resends through")
	}
	if budgeted*2 > unbudgeted {
		t.Fatalf("budgeted rewrites %d, unbudgeted %d: the budget should cut resends by more than half", budgeted, unbudgeted)
	}
}

// shadow and off never delay a resend
func TestSendSequence_ResendBudgetIsOnlyEnforcedInEnforceMode(t *testing.T) {
	defer setGovernModeForTest(governShadow)()
	savedTokens, savedLast := transferResendBudget.snapshotForTest()
	defer transferResendBudget.restoreForTest(savedTokens, savedLast)
	transferResendBudget.reset(0)
	if !retryAllowed(1000) {
		t.Fatal("shadow mode must never refuse a resend")
	}
	defer setGovernModeForTest(governOff)()
	if !retryAllowed(1000) {
		t.Fatal("off mode must never refuse a resend")
	}
	restore := setGovernModeForTest(governEnforce)
	defer restore()
	if retryAllowed(1000) {
		t.Fatal("enforce mode refuses when the budget is empty")
	}
}

// The second send of any frame is free, and the sequence head is free through
// its fifth send; everything else is charged.
func TestRetryBudget_FirstResendIsFreeAndTheHeadGetsAHeadStart(t *testing.T) {
	for sendCount := 1; sendCount <= 8; sendCount++ {
		if free := resendIsFree(sendCount, false); free != (sendCount+1 <= 2) {
			t.Fatalf("non-head sendCount=%d free=%t", sendCount, free)
		}
		if free := resendIsFree(sendCount, true); free != (sendCount+1 <= 5) {
			t.Fatalf("head sendCount=%d free=%t", sendCount, free)
		}
	}
}
