//go:build linux || android

package connect

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

// udpSocketEpollBackend waits for readiness through the Go runtime's network
// poller. The epoll descriptor is nonblocking and wrapped in an os.File, so a
// shard with nothing ready parks as a goroutine. A blocking epoll_wait would
// hold one OS thread per shard for the life of the process, and a provider
// runs a few shards for every proxy.
type udpSocketEpollBackend struct {
	mutex    sync.Mutex
	fd       int
	wakeFd   int
	file     *os.File
	rawConn  syscall.RawConn
	osEvents [udpSocketPollEventCount]unix.EpollEvent
}

func newUdpSocketPollBackend() (udpSocketPollBackend, error) {
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	wakeFd, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	if err := unix.EpollCtl(fd, unix.EPOLL_CTL_ADD, wakeFd, &unix.EpollEvent{Events: unix.EPOLLIN, Fd: int32(wakeFd)}); err != nil {
		_ = unix.Close(wakeFd)
		_ = unix.Close(fd)
		return nil, err
	}
	// From here the file owns fd: closing the file closes the descriptor.
	file := os.NewFile(uintptr(fd), "udp-socket-epoll")
	if file == nil {
		_ = unix.Close(wakeFd)
		_ = unix.Close(fd)
		return nil, errors.New("udp socket epoll: cannot wrap the epoll descriptor")
	}
	rawConn, err := file.SyscallConn()
	if err != nil {
		_ = unix.Close(wakeFd)
		_ = file.Close()
		return nil, err
	}
	return &udpSocketEpollBackend{fd: fd, wakeFd: wakeFd, file: file, rawConn: rawConn}, nil
}

func (self *udpSocketEpollBackend) add(fd int) error {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return unix.EpollCtl(self.fd, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{
		Events: unix.EPOLLIN | unix.EPOLLERR | unix.EPOLLHUP,
		Fd:     int32(fd),
	})
}

func (self *udpSocketEpollBackend) remove(fd int) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	_ = unix.EpollCtl(self.fd, unix.EPOLL_CTL_DEL, fd, nil)
}

func (self *udpSocketEpollBackend) wake() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if self.wakeFd < 0 {
		return
	}
	var value [8]byte
	binary.NativeEndian.PutUint64(value[:], 1)
	_, _ = unix.Write(self.wakeFd, value[:])
}

func (self *udpSocketEpollBackend) wait(events []udpSocketPollEvent) (int, error) {
	// The callback polls without blocking. When nothing is ready the runtime
	// parks this goroutine until the epoll descriptor becomes readable, so the
	// wait costs no OS thread. Readiness or a wake ends it.
	var n int
	var waitErr error
	limit := self.osEvents[:min(len(events), len(self.osEvents))]
	readErr := self.rawConn.Read(func(fd uintptr) bool {
		n, waitErr = unix.EpollWait(int(fd), limit, 0)
		if waitErr != nil {
			return !errors.Is(waitErr, syscall.EINTR)
		}
		return 0 < n
	})
	if readErr != nil {
		return 0, readErr
	}
	if waitErr != nil {
		return 0, waitErr
	}
	for i := range n {
		event := self.osEvents[i]
		if int(event.Fd) == self.wakeFd {
			var value [8]byte
			_, _ = unix.Read(self.wakeFd, value[:])
			events[i] = udpSocketPollEvent{fd: -1}
			continue
		}
		events[i] = udpSocketPollEvent{
			fd:       int(event.Fd),
			terminal: event.Events&(unix.EPOLLERR|unix.EPOLLHUP) != 0,
		}
	}
	return n, nil
}

func (self *udpSocketEpollBackend) close() error {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if self.fd < 0 {
		return nil
	}
	_ = unix.Close(self.wakeFd)
	self.wakeFd = -1
	// Closing the file closes the epoll descriptor and ends a parked wait.
	err := self.file.Close()
	self.fd = -1
	return err
}
