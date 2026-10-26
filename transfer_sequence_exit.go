package connect

import "time"

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
	format := "[s]event=sequence_exit reason=%s client=%s destination=%s sequence=%s stream=%s ctx=%v parent_ctx=%v cause=%v pending=%d message=%s number=%d sends=%d lifetime_start_ns=%d write_started_ns=%d lifetime_ns=%d deadline_ns=%d observed_ns=%d selective=%t route_written=%t reliable=%t unreliable=%t carrier_changed=%t retained=%t head_set=%t head_number=%d head_received_ns=%d pending_head=%d pending_sacks=%d pending_contracts=%d\n"
	args := []any{
		reason, self.client.ClientTag(), self.destination, self.sequenceId, self.contractMultiRouteWriterAlias.StreamId,
		contextErr, parentErr, cause, pending, message, number, sends, lifetimeStart, writeStarted,
		lifetime, deadlineNanos, now.UnixNano(), selective, written, reliable, unreliable, carrierChanged, retained,
		headSet, headNumber, headAt, pendingHead, pendingSacks, pendingContracts,
	}
	// An ack-lifetime expiry is the ordinary outcome for a destination with no
	// route: one line per identity per start is a trace of that state, not an
	// incident. The caller reports the state itself at info level (control
	// plane not reached, how many identities, oldest age), so the per-message
	// detail lives at V(1). Every other reason is an abnormal exit and keeps
	// info, where an operator reads it without a verbosity flag.
	if reason == "ack_lifetime" {
		if log := self.log.V(1); log.Enabled() {
			log.Infof(format, args...)
		}
		return
	}
	self.log.Infof(format, args...)
}
