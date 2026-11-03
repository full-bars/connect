package connect

import (
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var sequenceExitDetailEnv = sync.OnceValue(func() bool {
	return os.Getenv("URNETWORK_SEQUENCE_EXIT_DETAIL") == "1"
})

var sequenceExitDetailForTest atomic.Int32

func isSequenceExitDetail(log Logger) bool {
	if v := sequenceExitDetailForTest.Load(); v != 0 {
		return v > 0
	}
	if sequenceExitDetailEnv() {
		return true
	}
	if log != nil && log.V(1).Enabled() {
		return true
	}
	return false
}

type sequenceExitGroupKey struct {
	reason      string
	destination Id
	cause       string
}

type sequenceExitGroup struct {
	key             sequenceExitGroupKey
	count           int
	sends           int
	lifetime        time.Duration
	sampleClientTag string
	log             Logger
	window          time.Duration
}

func (grp *sequenceExitGroup) emit() {
	if grp.log == nil {
		grp.log = DefaultLogger()
	}
	grp.log.Infof("[s]event=sequence_exit count=%d reason=%s destination=%s cause=%s sends=%d lifetime=%s client=%s window=%s\n",
		grp.count,
		grp.key.reason,
		grp.key.destination,
		grp.key.cause,
		grp.sends,
		grp.lifetime,
		grp.sampleClientTag,
		grp.window,
	)
}

type sequenceExitAggregator struct {
	mutex  sync.Mutex
	window time.Duration
	groups map[sequenceExitGroupKey]*sequenceExitGroup
	timer  *time.Timer
}

func newSequenceExitAggregator(window time.Duration) *sequenceExitAggregator {
	return &sequenceExitAggregator{
		window: window,
		groups: make(map[sequenceExitGroupKey]*sequenceExitGroup),
	}
}

var globalSequenceExitAggregator = newSequenceExitAggregator(60 * time.Second)

type sequenceExitRecord struct {
	reason      string
	destination Id
	cause       error
	sends       int
	lifetime    time.Duration
	clientTag   string
	log         Logger
}

func (a *sequenceExitAggregator) record(rec sequenceExitRecord) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	causeStr := "<nil>"
	if rec.cause != nil {
		causeStr = rec.cause.Error()
	}

	key := sequenceExitGroupKey{
		reason:      rec.reason,
		destination: rec.destination,
		cause:       causeStr,
	}

	grp, exists := a.groups[key]
	if !exists {
		grp = &sequenceExitGroup{
			key:             key,
			sends:           rec.sends,
			lifetime:        rec.lifetime,
			sampleClientTag: rec.clientTag,
			log:             rec.log,
			window:          a.window,
		}
		a.groups[key] = grp
	}
	grp.count++
	if grp.sampleClientTag == "" {
		grp.sampleClientTag = rec.clientTag
	}
	if grp.log == nil {
		grp.log = rec.log
	}
	if rec.sends > grp.sends {
		grp.sends = rec.sends
	}
	if grp.lifetime == 0 {
		grp.lifetime = rec.lifetime
	}

	if a.timer == nil {
		a.timer = time.AfterFunc(a.window, a.flush)
	}
}

func (a *sequenceExitAggregator) flush() {
	a.mutex.Lock()
	if a.timer != nil {
		a.timer.Stop()
		a.timer = nil
	}
	if len(a.groups) == 0 {
		a.mutex.Unlock()
		return
	}
	toEmit := a.groups
	a.groups = make(map[sequenceExitGroupKey]*sequenceExitGroup)
	a.mutex.Unlock()

	for _, grp := range toEmit {
		grp.emit()
	}
}

func (a *sequenceExitAggregator) reset() {
	a.mutex.Lock()
	if a.timer != nil {
		a.timer.Stop()
		a.timer = nil
	}
	a.groups = make(map[sequenceExitGroupKey]*sequenceExitGroup)
	a.mutex.Unlock()
}

func isRoutineSequenceExit(reason string, destination Id, routeWritten bool, pendingContracts int, sends int) bool {
	if routeWritten {
		return false
	}
	if reason != "ack_lifetime" && reason != "context" {
		return false
	}
	if pendingContracts > 0 {
		return false
	}
	if destination != (Id{}) && sends <= 1 {
		return false
	}
	return true
}

// flushSequenceExitAggregator emits every bucket still open in the process wide
// aggregator. The window timer would emit them a minute later, but a process
// that exits first takes them with it, and a closed client is the point where a
// short lived process (a probe, a test, a supervisor restart) ends. Called at
// client close, so the buckets of that client's sequences are logged rather
// than dropped; a flush emits the whole process's pending buckets, which costs
// nothing more than writing the lines early.
func flushSequenceExitAggregator() {
	globalSequenceExitAggregator.flush()
}

func resetSequenceExitAggregatorForTest() {
	globalSequenceExitAggregator.reset()
}

// One bounded metadata-only diagnostic identifies the first terminal owner
// decision. Generic callback errors intentionally retain their old contract.
// No packet history, payload, timer or additional queue is retained. Only the
// send owner calls this, including its synchronous pacing/route-write waits.
func (self *SendSequence) recordSendSequenceExit(reason string, item *sendItem, deadline time.Time, cause error) {
	if self.exitDiagnosticWritten {
		return
	}
	self.exitDiagnosticWritten = true
	if self.log == nil || self.client == nil || reason == "idle" {
		return
	}
	now := time.Now()
	if item == nil && len(self.ackLifetimes.items) > 0 {
		entry := self.ackLifetimes.items[0]
		item, deadline = entry.item, entry.at
	}
	pending := 0
	if self.resendQueue != nil {
		pending = self.resendQueue.Len()
	}
	var message Id
	var number uint64
	var sends int
	var lifetimeStart, writeStarted int64
	var lifetime time.Duration
	var selective, written, reliable, unreliable, carrierChanged, retained bool
	if item != nil {
		message, number, sends = item.messageId, item.sequenceNumber, item.sendCount
		// This is the start of the route attempt, not proof of a successful
		// P2P/SCTP physical write. The route-written bit is separate evidence.
		// SACKs may renew sendTime; this is the current lifetime anchor,
		// deliberately not labelled the original application offer time.
		lifetimeStart, writeStarted = item.sendTime.UnixNano(), item.pacingSentAtNanos
		lifetime = item.ackTimeout
		selective, written = item.selectiveAcked, item.transportWriteObserved
		reliable, unreliable, carrierChanged = item.reliableCarrierObserved, item.unreliableCarrierObserved, item.carrierChanged
		retained = item.acks.retainPastAckTimeout()
	}
	var headNumber uint64
	var headAt int64
	var headSet bool
	var pendingHead uint64
	var pendingSacks, pendingContracts int
	if self.ackWindow != nil {
		self.ackWindow.ackLock.Lock()
		headSet, headNumber, headAt = self.ackWindow.hasHeadAck, self.ackWindow.headAck.sequenceNumber, self.ackWindow.headAck.receivedAtNanos
		pendingHead = uint64(self.ackWindow.ackUpdateCount)
		pendingSacks, pendingContracts = len(self.ackWindow.selectiveAcks), len(self.ackWindow.contractMissingAcks)
		self.ackWindow.ackLock.Unlock()
	}

	if !isSequenceExitDetail(self.log) && isRoutineSequenceExit(reason, self.destination, written, pendingContracts, sends) {
		globalSequenceExitAggregator.record(sequenceExitRecord{
			reason:      reason,
			destination: self.destination,
			cause:       cause,
			sends:       sends,
			lifetime:    lifetime,
			clientTag:   self.client.ClientTag(),
			log:         self.log,
		})
		return
	}

	deadlineNanos := int64(0)
	if !deadline.IsZero() {
		deadlineNanos = deadline.UnixNano()
	}
	var contextErr, parentErr error
	if self.ctx != nil {
		contextErr = self.ctx.Err()
	}
	if self.client.ctx != nil {
		parentErr = self.client.ctx.Err()
	}
	self.log.Infof("[s]event=sequence_exit reason=%s client=%s destination=%s sequence=%s stream=%s ctx=%v parent_ctx=%v cause=%v pending=%d message=%s number=%d sends=%d lifetime_start_ns=%d write_started_ns=%d lifetime_ns=%d deadline_ns=%d observed_ns=%d selective=%t route_written=%t reliable=%t unreliable=%t carrier_changed=%t retained=%t head_set=%t head_number=%d head_received_ns=%d pending_head=%d pending_sacks=%d pending_contracts=%d\n",
		reason, self.client.ClientTag(), self.destination, self.sequenceId, self.contractMultiRouteWriterAlias.StreamId,
		contextErr, parentErr, cause, pending, message, number, sends, lifetimeStart, writeStarted,
		lifetime, deadlineNanos, now.UnixNano(), selective, written, reliable, unreliable, carrierChanged, retained,
		headSet, headNumber, headAt, pendingHead, pendingSacks, pendingContracts)
}
