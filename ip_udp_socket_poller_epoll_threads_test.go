//go:build linux

package connect

import (
	"bufio"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// processThreadCount reads the OS thread count of this process.
func processThreadCount(t *testing.T) int {
	t.Helper()
	file, err := os.Open("/proc/self/status")
	if err != nil {
		t.Skipf("no /proc/self/status: %v", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "Threads:") {
			count, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "Threads:")))
			if err != nil {
				t.Fatal(err)
			}
			return count
		}
	}
	t.Fatal("no Threads line in /proc/self/status")
	return 0
}

func udpSocketFd(t *testing.T, conn *net.UDPConn) int {
	t.Helper()
	rawConn, err := conn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	fd := -1
	if err := rawConn.Control(func(rawFd uintptr) { fd = int(rawFd) }); err != nil {
		t.Fatal(err)
	}
	return fd
}

// A readiness shard that waits for sockets must not hold an OS thread. The
// provider runs a few shards per proxy, so one thread per shard becomes
// thousands of threads on a host with a thousand proxies.
func TestUdpSocketEpollBackendWaitDoesNotPinThreads(t *testing.T) {
	const shardCount = 300
	before := processThreadCount(t)

	backends := make([]udpSocketPollBackend, 0, shardCount)
	for range shardCount {
		backend, err := newUdpSocketPollBackend()
		if err != nil {
			t.Fatal(err)
		}
		backends = append(backends, backend)
	}

	var entered sync.WaitGroup
	var done sync.WaitGroup
	results := make([]udpSocketPollEvent, shardCount)
	errs := make([]error, shardCount)
	for i, backend := range backends {
		entered.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			events := make([]udpSocketPollEvent, udpSocketPollEventCount)
			entered.Done()
			n, err := backend.wait(events)
			errs[i] = err
			if err == nil && 0 < n {
				results[i] = events[0]
			}
		}()
	}
	entered.Wait()

	// Let every goroutine reach the wait. The check polls until the thread
	// count settles or a generous bound passes; it never sleeps a fixed time.
	deadline := time.Now().Add(5 * time.Second)
	settled := processThreadCount(t)
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		now := processThreadCount(t)
		if now == settled {
			break
		}
		settled = now
	}

	// Before the netpoller wait this grew by about shardCount.
	if growth := settled - before; 40 < growth {
		t.Fatalf("%d parked shards grew the process by %d threads (before %d, after %d); a shard must not pin an OS thread", shardCount, growth, before, settled)
	}

	for _, backend := range backends {
		if udpBackend, ok := backend.(interface{ wake() }); ok {
			udpBackend.wake()
		}
	}
	done.Wait()
	for i := range shardCount {
		if errs[i] != nil {
			t.Fatalf("shard %d wait error = %v", i, errs[i])
		}
		if results[i].fd != -1 {
			t.Fatalf("shard %d got fd %d, want the wake marker -1", i, results[i].fd)
		}
	}
	for _, backend := range backends {
		if err := backend.close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestUdpSocketEpollBackendReadinessWakeAndClose(t *testing.T) {
	backend, err := newUdpSocketPollBackend()
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	sender, err := net.DialUDP("udp4", nil, receiver.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	fd := udpSocketFd(t, receiver)
	if err := backend.add(fd); err != nil {
		t.Fatal(err)
	}

	type waitResult struct {
		n      int
		events []udpSocketPollEvent
		err    error
	}
	waitOnce := func() chan waitResult {
		out := make(chan waitResult, 1)
		go func() {
			events := make([]udpSocketPollEvent, udpSocketPollEventCount)
			n, err := backend.wait(events)
			out <- waitResult{n: n, events: events[:max(n, 0)], err: err}
		}()
		return out
	}

	// readiness: a datagram arrives while the shard is parked
	pending := waitOnce()
	if _, err := sender.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-pending:
		if result.err != nil || result.n != 1 || result.events[0].fd != fd || result.events[0].terminal {
			t.Fatalf("readiness wait = %+v", result)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a ready socket did not end the wait")
	}
	buffer := make([]byte, 16)
	if _, _, err := receiver.ReadFromUDP(buffer); err != nil {
		t.Fatal(err)
	}

	// a datagram that is already queued ends the next wait at once
	if _, err := sender.Write([]byte("queued")); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-waitOnce():
		if result.err != nil || result.n != 1 || result.events[0].fd != fd {
			t.Fatalf("queued wait = %+v", result)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a queued datagram did not end the wait")
	}
	if _, _, err := receiver.ReadFromUDP(buffer); err != nil {
		t.Fatal(err)
	}

	// wake: ends the wait with the marker event
	pending = waitOnce()
	backend.(interface{ wake() }).wake()
	select {
	case result := <-pending:
		if result.err != nil || result.n != 1 || result.events[0].fd != -1 {
			t.Fatalf("wake wait = %+v", result)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a wake did not end the wait")
	}

	// remove stops events for the socket
	backend.remove(fd)

	// close: ends a parked wait with an error and does not hang
	pending = waitOnce()
	if err := backend.close(); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-pending:
		if result.err == nil {
			t.Fatalf("a closed backend wait returned no error: %+v", result)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("close did not end a parked wait")
	}
	if err := backend.close(); err != nil {
		t.Fatalf("a second close = %v", err)
	}
}
