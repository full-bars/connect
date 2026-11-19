package connect

import (
	"sync"
	"testing"
	"time"
)

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
