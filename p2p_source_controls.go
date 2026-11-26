package connect

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// Bound lane source policy modes, in the order a setting may select them.
// The values index the mismatch counters.
const (
	p2pSourceModeCount = iota
	p2pSourceModeOff
	p2pSourceModeDrop
)

// What happened to a frame whose source did not match its lane's bound peer.
// A transit hop forwards the bytes without decoding them and is not a policy
// point, so it reports no outcome: what it does is not evidence about the
// traffic it carries.
const (
	p2pSourceOutcomeDecoded = iota
	p2pSourceOutcomeRejected
	p2pSourceOutcomeDropped
)

// p2pSourceBindingDefault is the mode this library starts in. A bound
// peer-to-peer lane delivers its adjacent peer's frames and nothing else (see
// Client.run), so a frame naming another source is dropped before decode. A
// line that wants to measure before it enforces sets count at runtime, which
// delivers those frames and counts them.
const p2pSourceBindingDefault = p2pSourceModeDrop

var p2pSourceBinding atomic.Uint32
var p2pForgedSourceDropDisabled atomic.Bool

func init() {
	p2pSourceBinding.Store(p2pSourceBindingDefault)
}

// SetP2PSourceBinding selects what happens to a frame that arrives on a lane
// bound to one peer while naming a different source. It applies to the next
// frame, with no restart.
func SetP2PSourceBinding(value string) error {
	switch strings.ToLower(value) {
	case "count":
		p2pSourceBinding.Store(p2pSourceModeCount)
	case "off":
		p2pSourceBinding.Store(p2pSourceModeOff)
	case "drop":
		p2pSourceBinding.Store(p2pSourceModeDrop)
	default:
		return fmt.Errorf("p2p source binding must be off, count or drop")
	}
	return nil
}

func P2PSourceBinding() string {
	switch p2pSourceBinding.Load() {
	case p2pSourceModeOff:
		return "off"
	case p2pSourceModeDrop:
		return "drop"
	default:
		return "count"
	}
}

// P2PSourceBindingDefault is the mode this binary starts in, so a caller that
// keeps its own table of defaults cannot drift from the library.
func P2PSourceBindingDefault() string {
	switch p2pSourceBindingDefault {
	case p2pSourceModeOff:
		return "off"
	case p2pSourceModeDrop:
		return "drop"
	default:
		return "count"
	}
}

func p2pSourceBindingMode() uint32 { return p2pSourceBinding.Load() }

func SetP2PForgedSourceDrop(enabled bool) { p2pForgedSourceDropDisabled.Store(!enabled) }
func P2PForgedSourceDropEnabled() bool    { return !p2pForgedSourceDropDisabled.Load() }

// forgedSourceKind reports the enforced rule a frame on a direct peer-to-peer
// lane breaks: it names the platform, which only the platform may do, or it
// names this client, which only this client's own traffic does. A relayed
// frame can never legitimately name either. Every other frame returns "".
func forgedSourceKind(disposition transferReceiveDisposition, localId Id, sourceId Id) string {
	if disposition.transportType != TransportTypeP2p {
		return ""
	}
	switch sourceId {
	case ControlId:
		return "platform"
	case localId:
		return "self"
	}
	return ""
}

// boundSourceMismatch reports whether a frame named a source other than the
// peer its receive lane is bound to. Only a direct peer-to-peer lane is bound:
// the binding is the source the adjacent peer's signalling authenticated, and
// no other transport type carries one. An unbound lane accepts any source,
// because a custom carrier only has the sender's word for it.
func boundSourceMismatch(disposition transferReceiveDisposition, sourceId Id) bool {
	return disposition.transportType == TransportTypeP2p &&
		disposition.boundSourceId != (Id{}) &&
		sourceId != disposition.boundSourceId
}

// p2pSourceCounters holds the counters behind the two source policy metrics.
// Forged drops are counted by the rule the frame broke. Bound lane mismatches
// are counted by the policy mode in force when the frame arrived and by what
// happened to it, which is what separates legitimate multihop traffic from
// spoofing. Every index is a fixed constant, so the exposed label values stay
// bounded.
type p2pSourceCounters struct {
	forgedPlatform atomic.Uint64
	forgedSelf     atomic.Uint64
	mismatch       [3][3]atomic.Uint64
}

// p2pSourceMetrics aggregates every client in the process for a provider's
// metrics endpoint.
var p2pSourceMetrics p2pSourceCounters

func (self *p2pSourceCounters) addForged(kind string) {
	switch kind {
	case "platform":
		self.forgedPlatform.Add(1)
	case "self":
		self.forgedSelf.Add(1)
	}
}

func (self *p2pSourceCounters) forged() (platform uint64, self_ uint64) {
	return self.forgedPlatform.Load(), self.forgedSelf.Load()
}

func (self *p2pSourceCounters) addMismatch(mode uint32, outcome int) {
	if p2pSourceModeDrop < mode || p2pSourceOutcomeDropped < outcome {
		return
	}
	self.mismatch[mode][outcome].Add(1)
}

func (self *p2pSourceCounters) mismatchAt(mode uint32, outcome int) uint64 {
	if p2pSourceModeDrop < mode || p2pSourceOutcomeDropped < outcome {
		return 0
	}
	return self.mismatch[mode][outcome].Load()
}

func (self *p2pSourceCounters) mismatchTotal() (total uint64) {
	for mode := range self.mismatch {
		for outcome := range self.mismatch[mode] {
			total += self.mismatch[mode][outcome].Load()
		}
	}
	return total
}

func (self *Client) recordForgedSourceDrop(kind string) {
	self.p2pSources.addForged(kind)
	p2pSourceMetrics.addForged(kind)
}

func (self *Client) recordSourceMismatch(mode uint32, outcome int) {
	self.p2pSources.addMismatch(mode, outcome)
	p2pSourceMetrics.addMismatch(mode, outcome)
}

// P2PSourceStats is one read of the process-wide source policy counters. This
// package exposes no metrics endpoint, so a provider that serves /metrics
// renders these series itself. The zero value before the first event keeps
// the series present from the first scrape.
type P2PSourceStats struct {
	ForgedPlatformDropCount uint64
	ForgedSelfDropCount     uint64
	// Mismatch counts are split by the mode in force when the frame arrived.
	// Only count mode produces decoded and rejected. Drop mode resolves every
	// mismatch before decode, so it produces dropped alone.
	MismatchCountDecoded  uint64
	MismatchCountRejected uint64
	MismatchCountDropped  uint64
	MismatchDropDropped   uint64
}

// P2PSourceStatsSnapshot reads the process-wide counters without stopping any
// receive loop.
func P2PSourceStatsSnapshot() P2PSourceStats {
	platform, self_ := p2pSourceMetrics.forged()
	return P2PSourceStats{
		ForgedPlatformDropCount: platform,
		ForgedSelfDropCount:     self_,
		MismatchCountDecoded:    p2pSourceMetrics.mismatchAt(p2pSourceModeCount, p2pSourceOutcomeDecoded),
		MismatchCountRejected:   p2pSourceMetrics.mismatchAt(p2pSourceModeCount, p2pSourceOutcomeRejected),
		MismatchCountDropped:    p2pSourceMetrics.mismatchAt(p2pSourceModeCount, p2pSourceOutcomeDropped),
		MismatchDropDropped:     p2pSourceMetrics.mismatchAt(p2pSourceModeDrop, p2pSourceOutcomeDropped),
	}
}

// p2pSourceMismatch records the outcome of one frame whose source did not
// match the peer its lane is bound to. The record travels with the pack until
// delivery or discard. A nil record is silent and the first outcome wins.
// Entries exist only while an outcome is pending. Both identities are removed
// before an embedded owner can return to its pool and be reused.
var p2pSourceMismatchTable sync.Map

type p2pSourceMismatch struct {
	client *Client
	mode   uint32
	active atomic.Bool
	mu     sync.Mutex
	pack   *ReceivePack
	item   *receiveItem
}

func (self *p2pSourceMismatch) bindPack(pack *ReceivePack) {
	if self == nil {
		return
	}
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.active.Load() {
		self.pack = pack
		p2pSourceMismatchTable.Store(pack, self)
	}
}

func (self *p2pSourceMismatch) bindItem(item *receiveItem) {
	if self == nil {
		return
	}
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.active.Load() {
		self.item = item
		p2pSourceMismatchTable.Store(item, self)
	}
}

func p2pMismatchFor(key any) *p2pSourceMismatch {
	if value, ok := p2pSourceMismatchTable.Load(key); ok {
		return value.(*p2pSourceMismatch)
	}
	return nil
}

func newP2PSourceMismatch(client *Client, mode uint32) *p2pSourceMismatch {
	mismatch := &p2pSourceMismatch{client: client, mode: mode}
	mismatch.active.Store(true)
	return mismatch
}

func (self *p2pSourceMismatch) record(outcome int) {
	if self == nil {
		return
	}
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.active.CompareAndSwap(true, false) {
		if self.pack != nil {
			p2pSourceMismatchTable.Delete(self.pack)
			self.pack = nil
		}
		if self.item != nil {
			p2pSourceMismatchTable.Delete(self.item)
			self.item = nil
		}
		self.client.recordSourceMismatch(self.mode, outcome)
	}
}

// decoded means the pack passed the contract and plaintext delivery gates.
func (self *p2pSourceMismatch) decoded() { self.record(p2pSourceOutcomeDecoded) }

// rejected means the frame failed to decode or to decrypt, was discarded by the
// receive buffer (such as an older sequence), or this client requires
// encryption for the source and threw the plaintext frame away.
func (self *p2pSourceMismatch) rejected() { self.record(p2pSourceOutcomeRejected) }

// dropped means the frame was not decoded here: binding drop mode, or a
// receive guard that discards a frame it cannot place.
func (self *p2pSourceMismatch) dropped() { self.record(p2pSourceOutcomeDropped) }
