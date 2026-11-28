package connect

import (
	"sync"
	"testing"
	"time"
)

func TestBackendDegraded_RepeatedTimestampKeepsSnapshotsSeparate(t *testing.T) {
	resetBackendDegraded()
	defer resetBackendDegraded()

	fresh := time.Now().UnixNano()
	backendFailures.Store(&backendFailureState{count: 1, stamp: fresh})
	first := loadBackendFailureState()
	backendFailures.Store(&backendFailureState{count: backendDegradedFailThreshold + 5, stamp: fresh - int64(backendDegradedWindow) - 1})
	stale := loadBackendFailureState()
	backendFailures.Store(&backendFailureState{count: 1, stamp: fresh})
	last := loadBackendFailureState()

	for _, state := range []backendFailureState{first, stale, last} {
		if state.count >= backendDegradedFailThreshold && backendFailRecent(fresh, state.stamp) {
			t.Fatal("snapshot combined a stale count with a fresh timestamp")
		}
	}
	if first != last || stale.count != backendDegradedFailThreshold+5 {
		t.Fatal("published snapshots changed after a later update")
	}
}

func TestBackendDegraded_ConcurrentReadersNeverSeeATornPair(t *testing.T) {
	resetBackendDegraded()
	defer resetBackendDegraded()
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			backendFailMu.Lock()
			backendFailures.Store(&backendFailureState{count: backendDegradedFailThreshold + 5, stamp: time.Now().Add(-backendDegradedWindow - time.Second).UnixNano()})
			backendFailMu.Unlock()
			noteBackendFailure()
		}
	}()
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				if isBackendDegraded() {
					t.Error("reader observed a torn failure state")
					return
				}
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	close(done)
	wg.Wait()
}
