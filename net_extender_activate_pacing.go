package connect

import (
	"bytes"
	"maps"
	"net/netip"
	"slices"
	"time"
)

// Activation pacing: a working extender stops posting while its signed record is
// valid and renews before the record expires, and what the operator's budget said
// survives a restart.
//
// Until this, a success scheduled the next post a fixed ActivateTimeout away,
// whatever the record's expiry said, and every state lived in memory: a restarted
// process posted every family at once, which spent the operator's activation budget
// (six an hour per account) on hosts that were already activated, and a refusal
// that named a retry_after was forgotten with the process.
//
// What is kept, per schedule key (the ip version, or 0 for the plain api url):
//
//	State                 how the last attempt ended
//	RecordExpireTime      when the signed record the operator issued expires
//	LastAttemptAt         when the last post that reached the operator was made
//	NextAllowedAttemptAt  no post before this
//
// A success sets the next attempt to the renewal point of the verified record (renewalTime). A
// renewal is a post like any other: it takes the same hold on a refusal or a failure,
// and the record stays standing until it expires. A budget refusal and a failed
// renewal are a hold (Hold): neither the network change nor the address trigger posts
// through it. Persisted the moment it is set (SavePacing), and read at start, when no
// family posts before its NextAllowedAttemptAt.

// How the last attempt of a family ended.
const (
	// the family has not posted
	ExtenderPacingUnactivated = "unactivated"
	// the record is valid and the last post worked
	ExtenderPacingActivated = "activated"
	// the record is valid and the last post, a renewal, was refused
	ExtenderPacingRenewalRefused = "renewal_refused"
	// the record is valid and the last post, a renewal, failed
	ExtenderPacingRenewalFailed = "renewal_failed"
	// no valid record, and the last post was refused
	ExtenderPacingRefused = "refused"
	// no valid record, and the last post failed
	ExtenderPacingFailed = "failed"
)

// The pacing of one family, which is what is stored.
type ExtenderActivationPacing struct {
	State                string    `json:"state"`
	RecordExpireTime     time.Time `json:"record_expire_time,omitempty"`
	LastAttemptAt        time.Time `json:"last_attempt_at,omitempty"`
	NextAllowedAttemptAt time.Time `json:"next_allowed_attempt_at,omitempty"`
	// the address the record was issued for. A restored family without it reads
	// the first caller address check as an address that moved and posts.
	Ip netip.Addr `json:"ip,omitempty"`
	// whether NextAllowedAttemptAt holds against the triggers too (a network
	// change, an address change), because it is the operator's budget or a failed
	// renewal. Without it only the schedule moves it.
	Hold bool `json:"hold,omitempty"`
}

// The record valid at `now`: the operator's record expires at its expiry, not after.
func (self ExtenderActivationPacing) RecordValid(now time.Time) bool {
	return !self.RecordExpireTime.IsZero() && now.Before(self.RecordExpireTime)
}

// Whether an attempt is allowed at `now`. The boundary is allowed: the instant
// NextAllowedAttemptAt is reached the family may post, as the schedule is due at its
// time.
func (self ExtenderActivationPacing) AttemptAllowed(now time.Time) bool {
	return !now.Before(self.NextAllowedAttemptAt)
}

// What the store holds: the pacing of every family, of one identity. A record of
// another key says nothing of this one.
type ExtenderActivationPacingRecord struct {
	PublicKey []byte                           `json:"public_key"`
	Families  map[int]ExtenderActivationPacing `json:"families"`
}

// Where the pacing survives a restart. Both run on the loop goroutine and must not
// block for long. A load that fails starts as a new process does; a save that
// fails is logged and the pacing stands in memory.
type ExtenderActivationPacingStore interface {
	LoadPacing() (*ExtenderActivationPacingRecord, error)
	SavePacing(record *ExtenderActivationPacingRecord) error
}

// The pacing of every family, a copy. Safe to call from any goroutine.
func (self *ExtenderActivator) Pacing() map[int]ExtenderActivationPacing {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return maps.Clone(self.pacing)
}

// Reads the store and applies it to the schedules, once, at start. A record of
// another key is dropped. A family with a NextAllowedAttemptAt ahead of now waits for
// it, and one whose record is valid is shown as activated until it expires, since
// the directory holds the record the status describes.
func (self *ExtenderActivator) loadPacing() {
	store := self.settings.PacingStore
	if store == nil {
		return
	}
	record, err := store.LoadPacing()
	if err != nil {
		self.log.Infof("[extender]activate pacing load err = %s\n", activationLogError(err.Error()))
		return
	}
	if record == nil || !bytes.Equal(record.PublicKey, self.settings.PublicKey) {
		return
	}
	now := self.settings.Now()
	replaced := self.settings.PreviousState != nil
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	for key, pacing := range record.Families {
		schedule, ok := self.schedules[key]
		if !ok {
			continue
		}
		self.pacing[key] = pacing
		// a role replaced under a running process posts once more for the things only a
		// post gives it (continueFrom); only a hold stands against that
		if (pacing.Hold || !replaced) && schedule.nextActivateTime.Before(pacing.NextAllowedAttemptAt) {
			schedule.nextActivateTime = pacing.NextAllowedAttemptAt
		}
		if family, ok := self.families[key]; ok && !family.Activated && pacing.RecordValid(now) {
			family.Activated = true
			family.ExpireTime = pacing.RecordExpireTime
			family.LastActivationTime = pacing.LastAttemptAt
			family.Ip = pacing.Ip
		}
	}
	self.changedWithLock()
}

// Puts a hold back on the schedules the triggers have made due. It runs ahead of the
// pass, after every trigger.
func (self *ExtenderActivator) applyPacingHold(now time.Time) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	for key, pacing := range self.pacing {
		schedule, ok := self.schedules[key]
		if !ok || !pacing.Hold || pacing.AttemptAllowed(now) {
			continue
		}
		if schedule.nextActivateTime.Before(pacing.NextAllowedAttemptAt) {
			schedule.nextActivateTime = pacing.NextAllowedAttemptAt
		}
	}
}

// Records where one family's attempt left it, in memory (savePacing writes it). `outcome` is how
// the attempt ended; `next` is the schedule's next attempt as the outcome just set it, and
// `budgetHeld` says the operator named when it has room (retry_after_seconds).
func (self *ExtenderActivator) recordPacing(
	key int,
	now time.Time,
	outcome extenderActivateOutcome,
	next time.Time,
	budgetHeld bool,
) {
	self.stateLock.Lock()
	previous := self.pacing[key]
	pacing := ExtenderActivationPacing{
		State:                ExtenderPacingUnactivated,
		LastAttemptAt:        now,
		NextAllowedAttemptAt: next,
	}
	if outcome == extenderActivateActivated {
		pacing.State = ExtenderPacingActivated
		pacing.RecordExpireTime = self.recordExpiryWithLock(key)
		if family, ok := self.families[key]; ok {
			pacing.Ip = family.Ip
		}
	} else {
		// a failed renewal does not clear the record: it stands until it expires
		if previous.RecordValid(now) {
			pacing.RecordExpireTime = previous.RecordExpireTime
			pacing.Ip = previous.Ip
		}
		valid := pacing.RecordValid(now)
		switch {
		case outcome == extenderActivateRefused && valid:
			pacing.State = ExtenderPacingRenewalRefused
		case outcome == extenderActivateRefused:
			pacing.State = ExtenderPacingRefused
		case valid:
			pacing.State = ExtenderPacingRenewalFailed
		default:
			pacing.State = ExtenderPacingFailed
		}
		pacing.Hold = budgetHeld || (valid && outcome != extenderActivateActivated)
	}
	// no change notice of its own: the outcome that led here already moved the
	// counter, and Status reads the pacing live under the same lock
	self.pacing[key] = pacing
	self.stateLock.Unlock()
}

// The expiry of the record a family just activated with, zero when the answer named
// none. The one schedule of the plain api url answers for whichever family the
// operator named, the latest expiry of those standing.
func (self *ExtenderActivator) recordExpiryWithLock(key int) time.Time {
	if !self.fallback {
		if family, ok := self.families[key]; ok {
			return family.ExpireTime
		}
		return time.Time{}
	}
	expire := time.Time{}
	for _, family := range self.families {
		if family.Activated && expire.Before(family.ExpireTime) {
			expire = family.ExpireTime
		}
	}
	return expire
}

// Writes the pacing, now. A pass that moved none writes nothing.
func (self *ExtenderActivator) savePacing() {
	store := self.settings.PacingStore
	if store == nil {
		return
	}
	self.stateLock.Lock()
	record := &ExtenderActivationPacingRecord{
		PublicKey: slices.Clone(self.settings.PublicKey),
		Families:  maps.Clone(self.pacing),
	}
	self.stateLock.Unlock()
	if err := store.SavePacing(record); err != nil {
		self.log.Infof("[extender]activate pacing save err = %s\n", activationLogError(err.Error()))
	}
}
