package connect

import (
	"context"
	"net"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/urnetwork/connect/protocol"
)

func yieldView(silent, sinceAck, sinceSend time.Duration) destinationYieldView {
	return destinationYieldView{DestinationId: NewId(), SilentFor: silent, SinceLastAck: sinceAck, SinceLastSend: sinceSend}
}

// healthyFleet is n destinations that sent and were acknowledged moments ago.
func healthyFleet(n int) []destinationYieldView {
	views := make([]destinationYieldView, 0, n)
	for i := 0; i < n; i++ {
		views = append(views, yieldView(0, time.Second, time.Second))
	}
	return views
}

func TestDeadClientPolicy_ReleasesOneSilentClientWhileTheFleetIsHealthy(t *testing.T) {
	policy := defaultDeadClientPolicy()
	dead := yieldView(policy.Silence+time.Second, 0, time.Second)
	decision := decideDeadClients(append(healthyFleet(9), dead), policy)
	if decision.FleetCollapsed {
		t.Fatal("9 of 10 acking is not a collapse")
	}
	if len(decision.Release) != 1 || decision.Release[0].View.DestinationId != dead.DestinationId || decision.Release[0].Reason != deadClientReasonHealthy {
		t.Fatalf("release = %+v, want exactly the silent client", decision.Release)
	}
}

func TestDeadClientPolicy_NeverReleasesBeforeTheSilenceWindow(t *testing.T) {
	policy := defaultDeadClientPolicy()
	almost := yieldView(policy.Silence-time.Second, 0, time.Second)
	if decision := decideDeadClients(append(healthyFleet(9), almost), policy); len(decision.Release) != 0 {
		t.Fatalf("released a client silent for less than %s: %+v", policy.Silence, decision.Release)
	}
}

// When almost nothing is acknowledged anywhere it is the backend, not the
// clients: hold off, but never past the hard limit.
func TestDeadClientPolicy_FleetWideCollapseHoldsReleaseUntilTheHardLimit(t *testing.T) {
	policy := defaultDeadClientPolicy()
	build := func(silent time.Duration) []destinationYieldView {
		views := []destinationYieldView{yieldView(0, time.Second, time.Second)} // one still acking
		for i := 0; i < 9; i++ {
			views = append(views, yieldView(silent, 0, time.Second))
		}
		return views
	}
	held := decideDeadClients(build(policy.Silence+time.Minute), policy)
	if !held.FleetCollapsed || len(held.Release) != 0 || held.Silent != 9 {
		t.Fatalf("collapse must hold every release: %+v", held)
	}
	released := decideDeadClients(build(policy.HardSilence+time.Second), policy)
	if len(released.Release) != 9 || released.Release[0].Reason != deadClientReasonHard {
		t.Fatalf("past the hard limit all silent clients are released, got %d", len(released.Release))
	}
}

// Too few active destinations to blame the client rather than the platform: a
// small fleet waits for the hard limit, which still releases clients that are
// really gone.
func TestDeadClientPolicy_SmallFleetsWaitForTheHardLimit(t *testing.T) {
	policy := defaultDeadClientPolicy()
	build := func(silent time.Duration) []destinationYieldView {
		return []destinationYieldView{
			yieldView(silent, 0, time.Second),
			yieldView(silent, 0, time.Second),
		}
	}
	early := decideDeadClients(build(policy.Silence+time.Minute), policy)
	if early.FleetCollapsed || len(early.Release) != 0 || early.Silent != 2 {
		t.Fatalf("a small fleet must not release at the silence window: %+v", early)
	}
	late := decideDeadClients(build(policy.HardSilence+time.Second), policy)
	if len(late.Release) != 2 || late.Release[0].Reason != deadClientReasonSmall {
		t.Fatalf("a small fleet releases at the hard limit: %+v", late)
	}
}

// Silence with nothing being written is an idle client, not a gone one.
func TestDeadClientPolicy_IdleClientIsNeverReleased(t *testing.T) {
	policy := defaultDeadClientPolicy()
	idle := yieldView(policy.HardSilence+time.Hour, 0, policy.ActiveWindow+time.Minute)
	decision := decideDeadClients(append(healthyFleet(9), idle), policy)
	if len(decision.Release) != 0 || decision.Silent != 0 {
		t.Fatalf("a destination we stopped writing to was released: %+v", decision)
	}
}

// Destinations with no recent traffic do not make a healthy fleet look collapsed.
func TestDeadClientPolicy_IdleDestinationsDoNotCountAsActive(t *testing.T) {
	policy := defaultDeadClientPolicy()
	views := healthyFleet(6)
	for i := 0; i < 50; i++ {
		views = append(views, yieldView(0, 10*time.Minute, 10*time.Minute))
	}
	if decision := decideDeadClients(views, policy); decision.FleetCollapsed || decision.Active != 6 {
		t.Fatalf("idle destinations changed the verdict: %+v", decision)
	}
}

func TestDestinationYield_SilenceStartsAtFirstWriteAndOnlyAnAckClearsIt(t *testing.T) {
	yield := &destinationYield{destinationId: NewId()}
	base := time.Unix(1_000_000, 0)
	yield.noteSend(100, base.UnixNano())
	yield.noteSend(100, base.Add(30*time.Second).UnixNano())
	view := yield.view(base.Add(60 * time.Second))
	if view.SilentFor != 60*time.Second {
		t.Fatalf("silence = %s, want 60s since the first write, not the latest", view.SilentFor)
	}
	if view.SentBytes != 200 || view.AckedBytes != 0 {
		t.Fatalf("bytes = %d sent %d acked", view.SentBytes, view.AckedBytes)
	}
	yield.noteAck(100, base.Add(61*time.Second).UnixNano())
	if view := yield.view(base.Add(62 * time.Second)); view.SilentFor != 0 || view.AckedBytes != 100 {
		t.Fatalf("an ack must clear the silence: %+v", view)
	}
	yield.noteSend(100, base.Add(90*time.Second).UnixNano())
	if view := yield.view(base.Add(100 * time.Second)); view.SilentFor != 10*time.Second {
		t.Fatalf("silence restarts at the first write after the ack, got %s", view.SilentFor)
	}
	yield.reset()
	if view := yield.view(base.Add(100 * time.Second)); view.SilentFor != 0 {
		t.Fatal("reset must forget the silence")
	}
}

func TestSendBuffer_DestinationYieldsAreTrackedAndOldIdleOnesDropped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer client.Close()
	sendBuffer := NewSendBuffer(ctx, client, DefaultSendBufferSettings())

	live, stale := NewId(), NewId()
	now := time.Now()
	sendBuffer.yieldFor(live).noteSend(10, now.Add(-time.Minute).UnixNano())
	staleYield := sendBuffer.yieldFor(stale)
	staleYield.noteSend(10, now.Add(-2*yieldRetention).UnixNano())
	staleYield.noteAck(10, now.Add(-2*yieldRetention).UnixNano())

	silentIdle := sendBuffer.yieldFor(NewId())
	silentIdle.noteSend(10, now.Add(-2*yieldRetention).UnixNano()) // silent, but nothing written for a long time

	views := sendBuffer.DestinationYields(now)
	if len(views) != 1 || views[0].DestinationId != live || views[0].SilentFor < time.Minute {
		t.Fatalf("views = %+v, want only the live silent destination", views)
	}
	if len(sendBuffer.DestinationYields(now)) != 1 {
		t.Fatal("the stale record should have been dropped")
	}
}

// A real sequence writing to a peer that never answers makes the destination
// silent; an acknowledged frame clears it.
func TestSendSequence_FeedsTheDestinationYield(t *testing.T) {
	assertMessagePoolOwnership(t)
	synctest.Test(t, func(t *testing.T) {
		fixture, _, _ := newAckRetirementFixture(t, TransportTypeH1, 2, NewNoopLogger())
		destination := fixture.receiver.ClientId()
		viewOf := func() destinationYieldView {
			for _, view := range fixture.sender.sendBuffer.DestinationYields(time.Now()) {
				if view.DestinationId == destination {
					return view
				}
			}
			t.Fatal("no yield record for the destination")
			return destinationYieldView{}
		}

		first := fixture.write(64)
		time.Sleep(5 * time.Second)
		if view := viewOf(); view.SilentFor <= 0 || view.SentBytes == 0 || view.AckedBytes != 0 {
			t.Fatalf("a peer that never answers must be silent: %+v", view)
		}

		fixture.forward(fixture.receive(first), fixture.senderIn)
		if view := viewOf(); view.SilentFor != 0 || view.AckedBytes == 0 {
			t.Fatalf("an acknowledged frame must clear the silence: %+v", view)
		}
	})
}

func TestSendBuffer_CancelDestinationCancelsOnlyThatDestination(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Close()
	sendBuffer := NewSendBuffer(ctx, client, settings.SendBufferSettings)

	gone, other := NewId(), NewId()
	goneSeq := NewSendSequence(ctx, client, sendBuffer, gone, MultiHopId{}, false, true, false, sequenceTlsRoleClient, false, settings.SendBufferSettings)
	otherSeq := NewSendSequence(ctx, client, sendBuffer, other, MultiHopId{}, false, true, false, sequenceTlsRoleClient, false, settings.SendBufferSettings)
	sendBuffer.mutex.Lock()
	sendBuffer.sendSequences[sendSequenceId{Destination: gone}] = goneSeq
	sendBuffer.sendSequences[sendSequenceId{Destination: other}] = otherSeq
	sendBuffer.mutex.Unlock()

	if got := sendBuffer.CancelDestination(gone); got != 1 {
		t.Fatalf("cancelled %d sequences, want 1", got)
	}
	if goneSeq.ctx.Err() == nil {
		t.Fatal("the released destination's sequence is still live")
	}
	if otherSeq.ctx.Err() != nil {
		t.Fatal("another destination's sequence was cancelled")
	}
}

// The NAT cancels every flow of one source and leaves other sources alone.
func TestUdpBuffer_CancelSourceCancelsOnlyThatSourcesFlows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buffer := newUdpBuffer[BufferId4](ctx, receiveTransferPacketAdapter(func(TransferPath, protocol.ProvideMode, *IpPath, []byte) {}), DefaultUdpBufferSettings())

	newFlow := func(source TransferPath, port int) (*UdpSequence, BufferId4) {
		ip := net.IPv4(10, 0, 0, 1).To4()
		sequence := NewUdpSequence(ctx, func(TransferPath, protocol.ProvideMode, *IpPath, []byte) {}, source, protocol.ProvideMode_Network, 4, ip, uint16(port), ip, uint16(53), DefaultUdpBufferSettings())
		bufferId := NewBufferId4(source, ip, port, ip, 53)
		buffer.mutex.Lock()
		buffer.sequences[bufferId] = sequence
		if buffer.sourceSequences[source] == nil {
			buffer.sourceSequences[source] = map[BufferId4]*UdpSequence{}
		}
		buffer.sourceSequences[source][bufferId] = sequence
		buffer.mutex.Unlock()
		return sequence, bufferId
	}
	gone := TransferPath{SourceId: NewId()}
	goneA, _ := newFlow(TransferPath{SourceId: gone.SourceId, StreamId: NewId()}, 1000)
	goneB, _ := newFlow(TransferPath{SourceId: gone.SourceId, StreamId: NewId()}, 1001)
	live, _ := newFlow(TransferPath{SourceId: NewId(), StreamId: NewId()}, 1002)

	if got := buffer.CancelSource(gone.SourceId); got != 2 {
		t.Fatalf("cancelled %d flows, want 2", got)
	}
	if goneA.ctx.Err() == nil || goneB.ctx.Err() == nil {
		t.Fatal("a flow of the released source is still live")
	}
	if live.ctx.Err() != nil {
		t.Fatal("another source's flow was cancelled")
	}
}

// The sweeper acts on the yield by mode: enforce releases, shadow only counts
// (once per silence) and names it, off does nothing.
func TestDeadClientSweeper_ActsByMode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Close()
	nat := NewLocalUserNatWithDefaults(ctx, "t")
	defer nat.Close()
	provider := NewRemoteUserNatProviderWithDefaults(client, nat)
	defer provider.Close()

	var lines []string
	clock := time.Now()
	sweeper := newDeadClientSweeper(func() []*RemoteUserNatProvider { return []*RemoteUserNatProvider{provider} })
	sweeper.now = func() time.Time { return clock }
	sweeper.emit = func(line string) { lines = append(lines, line) }

	gone := NewId()
	seq := NewSendSequence(ctx, client, client.sendBuffer, gone, MultiHopId{}, false, true, false, sequenceTlsRoleClient, false, settings.SendBufferSettings)
	client.sendBuffer.mutex.Lock()
	client.sendBuffer.sendSequences[sendSequenceId{Destination: gone}] = seq
	client.sendBuffer.mutex.Unlock()
	// a lone destination is a small fleet: only the hard limit releases it
	silentFor := sweeper.policy.HardSilence + 20*time.Second
	seed := func() {
		yield := client.sendBuffer.yieldFor(gone)
		yield.noteSend(4096, clock.Add(-silentFor).UnixNano())
		yield.noteSend(10, clock.Add(-time.Second).UnixNano()) // still being written to
	}
	seed()

	before := DeadClientStatsNow()

	restore := setDeadClientModeForTest(governOff)
	if sweeper.sweep() != 0 || seq.ctx.Err() != nil || len(lines) != 0 {
		t.Fatal("off must do nothing")
	}
	restore()

	restore = setDeadClientModeForTest(governShadow)
	sweeper.sweep()
	sweeper.sweep() // the silence was forgotten after being counted: not counted again
	restore()
	shadow := DeadClientStatsNow()
	if seq.ctx.Err() != nil {
		t.Fatal("shadow must not cancel anything")
	}
	if shadow.WouldRelease-before.WouldRelease != 1 || shadow.Released != before.Released || shadow.ByReasonSmall-before.ByReasonSmall != 1 {
		t.Fatalf("shadow must count one silence once, by reason: before %+v after %+v", before, shadow)
	}
	if len(lines) != 1 || !containsAll(lines[0], "dead client", gone.String(), "shadow", deadClientReasonSmall) {
		t.Fatalf("shadow must log once: %v", lines)
	}

	lines = nil
	seed() // written to again and still silent: a new silence
	restore = setDeadClientModeForTest(governEnforce)
	released := sweeper.sweep()
	restore()
	if released != 1 || seq.ctx.Err() == nil {
		t.Fatalf("enforce must release the silent client (released=%d)", released)
	}
	after := DeadClientStatsNow()
	if after.Released-shadow.Released != 1 || after.ReleasedSequences-shadow.ReleasedSequences != 1 {
		t.Fatalf("enforce counters: %+v", after)
	}
	if len(lines) != 1 || !containsAll(lines[0], "released dead client", gone.String(), "cancelled 1 send sequences") {
		t.Fatalf("enforce must log the release: %v", lines)
	}
	restore = setDeadClientModeForTest(governEnforce)
	again := sweeper.sweep()
	restore()
	if again != 0 {
		t.Fatal("the same silence must not be released twice")
	}
}

func TestParseDeadClientMode_DefaultsToShadow(t *testing.T) {
	for raw, want := range map[string]governMode{
		"": governShadow, "garbage": governShadow, "shadow": governShadow,
		"enforce": governEnforce, "ON": governEnforce, "true": governEnforce,
		"off": governOff, "0": governOff,
	} {
		if got := parseDeadClientMode(raw); got != want {
			t.Fatalf("parseDeadClientMode(%q) = %v, want %v", raw, got, want)
		}
	}
}

// An ack ends a silence and the silence's length lands in the recovered-silence
// histogram; a selective ack (noteLive) proves liveness without counting bytes.
func TestDestinationYield_RecoveredSilenceFeedsTheHistogramAndSelectiveAckProvesLiveness(t *testing.T) {
	_, before := RecoveredSilenceHistogram()
	yield := &destinationYield{destinationId: NewId()}
	base := time.Unix(2_000_000, 0)
	yield.noteSend(100, base.UnixNano())
	yield.noteLive(base.Add(45 * time.Second).UnixNano())
	if view := yield.view(base.Add(46 * time.Second)); view.SilentFor != 0 || view.AckedBytes != 0 {
		t.Fatalf("a selective ack must end the silence without counting bytes: %+v", view)
	}
	yield.noteSend(100, base.Add(100*time.Second).UnixNano())
	yield.noteAck(100, base.Add(100*time.Second+2*time.Second).UnixNano())

	bounds, after := RecoveredSilenceHistogram()
	if len(after) != len(bounds)+1 {
		t.Fatalf("histogram shape: %d bounds, %d buckets", len(bounds), len(after))
	}
	// 45s lands in the <=60s bucket and 2s in the <=10s bucket, both counted in
	// every cumulative bucket from their own upwards
	if after[len(after)-1]-before[len(before)-1] != 2 {
		t.Fatalf("expected 2 recovered silences, got %d", after[len(after)-1]-before[len(before)-1])
	}
	if after[0]-before[0] != 1 || after[2]-before[2] != 2 {
		t.Fatalf("buckets: <=10s +%d, <=60s +%d", after[0]-before[0], after[2]-before[2])
	}
}

// A send that asked for no acknowledgement is settled locally; that is not the
// far end confirming anything.
func TestSendSequence_NoAckSendDoesNotClearSilenceOrEarnBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Close()
	sendBuffer := NewSendBuffer(ctx, client, settings.SendBufferSettings)
	destination := NewId()
	seq := NewSendSequence(ctx, client, sendBuffer, destination, MultiHopId{}, false, true, false, sequenceTlsRoleClient, false, settings.SendBufferSettings)

	yield := sendBuffer.yieldFor(destination)
	yield.noteSend(100, time.Now().Add(-time.Minute).UnixNano())
	earnedBefore := TransferSendStatsNow().BudgetEarnedBytes
	seq.ackItemCounted(&sendItem{transferItem: transferItem{messageByteCount: 100}, transferFrameBytes: make([]byte, 100)}, false)
	if view := yield.view(time.Now()); view.SilentFor == 0 || view.AckedBytes != 0 {
		t.Fatalf("a local no-ack settlement cleared the silence: %+v", view)
	}
	if TransferSendStatsNow().BudgetEarnedBytes != earnedBefore {
		t.Fatal("a local no-ack settlement earned resend budget")
	}
}

func containsAll(s string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(s, part) {
			return false
		}
	}
	return true
}

// A platform outage looks like many small fleets, each too small to judge. The
// sweeper judges every provider's destinations together, so the collapse is seen
// and nothing is released at the silence window.
func TestDeadClientSweeper_JudgesTheFleetAcrossAllProviders(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newProvider := func() *RemoteUserNatProvider {
		client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
		t.Cleanup(client.Close)
		nat := NewLocalUserNatWithDefaults(ctx, "t")
		t.Cleanup(nat.Close)
		provider := NewRemoteUserNatProviderWithDefaults(client, nat)
		t.Cleanup(provider.Close)
		return provider
	}
	providers := []*RemoteUserNatProvider{newProvider(), newProvider()}
	clock := time.Now()
	sweeper := newDeadClientSweeper(func() []*RemoteUserNatProvider { return providers })
	sweeper.now = func() time.Time { return clock }
	sweeper.emit = func(string) {}

	silentFor := sweeper.policy.Silence + time.Minute
	for _, provider := range providers {
		for i := 0; i < 3; i++ { // 3 per provider: each alone is below MinActive
			yield := provider.client.sendBuffer.yieldFor(NewId())
			yield.noteSend(100, clock.Add(-silentFor).UnixNano())
			yield.noteSend(100, clock.Add(-time.Second).UnixNano())
		}
	}
	restore := setDeadClientModeForTest(governEnforce)
	defer restore()
	if released := sweeper.sweep(); released != 0 {
		t.Fatalf("released %d clients while nothing anywhere was acknowledging", released)
	}
	stats := DeadClientStatsNow()
	if stats.FleetCollapsed != 1 || stats.Active != 6 || stats.Acking != 0 || stats.Silent != 6 {
		t.Fatalf("the sweep must see one collapsed fleet of 6: %+v", stats)
	}
}

// The NAT sums every shard's canceller and forgets one when its shard ends.
func TestLocalUserNat_CancelSourceSumsShardsAndForgetsEndedOnes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nat := NewLocalUserNatWithDefaults(ctx, "t")
	defer nat.Close()
	source := NewId()
	base := nat.CancelSource(source)
	unsubA := nat.addSourceCancel(func(id Id) int {
		if id == source {
			return 2
		}
		return 0
	})
	unsubB := nat.addSourceCancel(func(Id) int { return 3 })
	if got := nat.CancelSource(source); got != base+5 {
		t.Fatalf("cancelled %d flows, want %d", got, base+5)
	}
	unsubA()
	unsubB()
	if got := nat.CancelSource(source); got != base {
		t.Fatalf("an ended shard's canceller still ran: %d, want %d", got, base)
	}
}
