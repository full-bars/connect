package extender

import (
	"sync"
	"sync/atomic"
	"time"
)

// The process wide per carrier traffic. A miner can run several extender servers
// in one process, and an operator reads one figure per carrier, so the relay path
// adds to these counters as well as to the server's own. Like the served and
// refused counts they are cumulative for the process. The rate and the minute
// history are derived on the periodic tick (SampleProcessCarrierRates), behind a
// lock, because the status and metrics paths read them from other goroutines.
var processCarrierTraffic struct {
	ingress   [extenderCarrierPortCount]atomic.Uint64
	egress    [extenderCarrierPortCount]atomic.Uint64
	liveConns [extenderCarrierPortCount]atomic.Int64
	connTotal [extenderCarrierPortCount]atomic.Uint64

	rateLock sync.Mutex
	rate     extenderCarrierRate
}

// ProcessCarrierByteStats returns the process wide relayed traffic by carrier name:
// the cumulative bytes on each side, the rate at the last sample, the live and total
// relayed connections, and the last minutes of ingress plus egress as whole KiB.
func ProcessCarrierByteStats(now time.Time) map[string]ExtenderCarrierByteStats {
	if now.IsZero() {
		now = time.Now()
	}
	processCarrierTraffic.rateLock.Lock()
	defer processCarrierTraffic.rateLock.Unlock()
	stats := make(map[string]ExtenderCarrierByteStats, extenderCarrierPortCount)
	for idx, carrier := range extenderCarrierPortNames {
		s := ExtenderCarrierByteStats{
			Carrier:      carrier,
			IngressBytes: processCarrierTraffic.ingress[idx].Load(),
			EgressBytes:  processCarrierTraffic.egress[idx].Load(),
			LiveConns:    uint64(max(processCarrierTraffic.liveConns[idx].Load(), 0)),
			ConnsTotal:   processCarrierTraffic.connTotal[idx].Load(),
			Minutes:      processCarrierTraffic.rate.minutes(idx, extenderRateMinuteCount, now),
		}
		if processCarrierTraffic.rate.haveSample {
			s.RateInBps = processCarrierTraffic.rate.rateIn[idx]
			s.RateOutBps = processCarrierTraffic.rate.rateOut[idx]
		}
		stats[carrier] = s
	}
	return stats
}

// SampleProcessCarrierRates folds the cumulative process wide counters into the
// rates and the minute history. Call it on the periodic tick.
func SampleProcessCarrierRates(now time.Time) {
	if now.IsZero() {
		now = time.Now()
	}
	var ingress, egress [extenderCarrierPortCount]uint64
	for i := range ingress {
		ingress[i] = processCarrierTraffic.ingress[i].Load()
		egress[i] = processCarrierTraffic.egress[i].Load()
	}
	processCarrierTraffic.rateLock.Lock()
	processCarrierTraffic.rate.sample(now, ingress, egress)
	processCarrierTraffic.rateLock.Unlock()
}

// ResetProcessCarrierByteStats zeroes the process wide traffic counters and history.
func ResetProcessCarrierByteStats() {
	processCarrierTraffic.rateLock.Lock()
	defer processCarrierTraffic.rateLock.Unlock()
	for i := range processCarrierTraffic.ingress {
		processCarrierTraffic.ingress[i].Store(0)
		processCarrierTraffic.egress[i].Store(0)
		processCarrierTraffic.liveConns[i].Store(0)
		processCarrierTraffic.connTotal[i].Store(0)
	}
	processCarrierTraffic.rate = extenderCarrierRate{}
}
