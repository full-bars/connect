package connect

import (
	"context"
	"net"
	// "net/http"

	// "os"
	// "strings"
	"fmt"
	"time"
	// "strconv"
	// "slices"

	"crypto/tls"
	// "crypto/ecdsa"
	// "crypto/ed25519"
	// "crypto/elliptic"
	// "crypto/rand"
	// "crypto/rsa"
	// "crypto/x509"
	// "crypto/x509/pkix"
	// "encoding/pem"
	// "encoding/json"
	// "flag"
	// "log"
	// "math/big"

	// "crypto/md5"
	// "encoding/binary"
	// "encoding/hex"
	// "syscall"

	mathrand "math/rand"
	"sync/atomic"
	// "golang.org/x/crypto/cryptobyte"
	// "golang.org/x/net/idna"
	// "google.golang.org/protobuf/proto"
	// "src.agwa.name/tlshacks"
	// "github.com/urnetwork/glog"
)

// see https://upb-syssec.github.io/blog/2023/record-fragmentation/

// set this as the `DialTLSContext` or equivalent
// returns a tls connection
func NewResilientDialTlsContext(
	connectSettings *ConnectSettings,
	fragment bool,
	reorder bool,
) DialTlsContextFunction {
	return newResilientDialTlsContext(connectSettings, fragment, reorder, nil)
}

func newResilientDialTlsContext(
	connectSettings *ConnectSettings,
	fragment bool,
	reorder bool,
	nextProtos []string,
) DialTlsContextFunction {
	baseTlsConfig := newClientTlsConfig(connectSettings.TlsConfig, nextProtos)
	return func(
		ctx context.Context,
		network string,
		addr string,
	) (net.Conn, error) {
		switch network {
		case "tcp", "tcp4", "tcp6":
		default:
			panic(fmt.Errorf("Resilient connections only support tcp network."))
		}

		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			panic(err)
		}

		// fmt.Printf("Extender client 1\n")

		conn, err := connectSettings.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}

		rconn := NewResilientTlsConn(conn, fragment, reorder)

		// copy and extend
		tlsConfig := baseTlsConfig.Clone()
		tlsConfig.ServerName = host
		tlsConn := tls.Client(rconn, tlsConfig)

		func() {
			tlsCtx, tlsCancel := context.WithTimeout(ctx, connectSettings.TlsTimeout)
			defer tlsCancel()
			err = tlsConn.HandshakeContext(tlsCtx)
		}()
		if err != nil {
			tlsConn.Close()
			return nil, err
		}
		// once the stream is established, no longer need the resilient features
		rconn.Off()

		return tlsConn, nil
	}
}

// adapts techniques to overcome adversarial networks
// the network uses this to the connect to the platform and extenders
// inspiraton for techniques taken from the Jigsaw project Outline SDK

type ResilientTlsConn struct {
	conn     net.Conn
	fragment bool
	reorder  bool
	buffer   []byte

	enabled atomic.Bool
}

// must be created before the tls connection starts
func NewResilientTlsConn(conn net.Conn, fragment bool, reorder bool) *ResilientTlsConn {
	resilientTlsConn := &ResilientTlsConn{
		conn:     conn,
		fragment: fragment,
		reorder:  reorder,
		buffer:   []byte{},
	}
	resilientTlsConn.enabled.Store(true)
	return resilientTlsConn
}

// Off permanently disables the resilient fragment/reorder layer. It drains
// any partially-buffered record first — an earlier Write already returned
// len(b), nil for those bytes, so stranding them would silently lose data
// the caller believes was sent. A partial or failed drain leaves the wire
// state indeterminate, so the connection is closed rather than reused.
func (self *ResilientTlsConn) Off() {
	if 0 < len(self.buffer) {
		n, err := self.conn.Write(self.buffer)
		if err != nil || n < len(self.buffer) {
			self.failConnection()
			return
		}
		self.buffer = self.buffer[0:0]
	}
	// can't turn back on after off because we don't know where to align the tls header
	self.enabled.Store(false)
}

func (self *ResilientTlsConn) Enabled() bool {
	return self.enabled.Load()
}

// failConnection marks the connection unusable after an indeterminate write
// (a partial or failed record send: the peer has part of the bytes and the
// wire state is unknowable). The buffered record is dropped so it is never
// re-sent, the resilient layer is disabled, and the underlying connection is
// closed so later writes fail instead of appending to a corrupt stream.
func (self *ResilientTlsConn) failConnection() {
	self.buffer = nil
	self.enabled.Store(false)
	self.conn.Close()
}

func (self *ResilientTlsConn) Write(b []byte) (int, error) {
	if self.Enabled() {
		self.buffer = append(self.buffer, b...)
		for 5 <= len(self.buffer) {
			tlsHeader := parseTlsHeader(self.buffer[0:5])
			if 5+int(tlsHeader.contentLength) <= len(self.buffer) {
				if tlsHeader.contentType == TlsContentTypeHandshake {
					// handshake
					handshakeBytes := self.buffer[5 : 5+tlsHeader.contentLength]
					clientHello, meta := UnmarshalClientHello(handshakeBytes)
					if clientHello != nil && clientHello.Info.ServerName != nil {
						// send the server name one character at a time
						// for each fragment, alternate the ttl of the connection to force retransmits and out-of-order arrival

						// initialSplitLen := mathrand.Intn((meta.ServerNameValueEnd+meta.ServerNameValueStart)/2-meta.ServerNameValueStart)
						// guard mathrand.Intn against zero/negative bounds (very short ServerName values panic Intn)
						splitRangeMid := (meta.ServerNameValueEnd + meta.ServerNameValueStart) / 2
						splitRange := splitRangeMid - meta.ServerNameValueStart
						stepRange := meta.ServerNameValueEnd - (meta.ServerNameValueStart + splitRange)

						// a fragment write failed after earlier fragments of this
						// record were already sent: the peer has part of the
						// record while the buffer still holds all of it, so the
						// record cannot be coherently retried — resending from
						// the start would duplicate the fragments already on
						// the wire. Fail the connection: drop the buffered
						// record, disable the layer, and close the connection
						// so a later retry cannot append to the corrupt
						// stream.
						fragmentWriteFailed := func(err error) (int, error) {
							self.failConnection()
							return 0, err
						}

						if splitRange <= 0 || stepRange <= 0 {
							// the server name is too short to fragment;
							// fall back to a single write
							record := tlsHeader.reconstruct(handshakeBytes)
							n, err := self.conn.Write(record)
							if err != nil || n < len(record) {
								return fragmentWriteFailed(err)
							}
							self.buffer = self.buffer[5+tlsHeader.contentLength:]
							continue
						}
						split := meta.ServerNameValueStart + mathrand.Intn(splitRange)
						step := 1 + mathrand.Intn(stepRange)
						blockSize := 64

						if tcpConn, ok := self.conn.(*net.TCPConn); ok {

							if self.fragment && self.reorder {
								tcpConn.SetNoDelay(true)

								f, err := tcpConn.File()
								if err != nil {
									return 0, err
								}
								fd := SocketHandle(f.Fd())
								defer f.Close()

								nativeTtl := GetSocketTtl(fd)
								if nativeTtl <= 0 {
									// syscall failed or returned a value we can't safely restore
									// (setting back to 0 would drop all packets at the first hop)
									record := tlsHeader.reconstruct(handshakeBytes)
									n, err := tcpConn.Write(record)
									if err != nil || n < len(record) {
										return fragmentWriteFailed(err)
									}
									self.buffer = self.buffer[5+tlsHeader.contentLength:]
									continue
								}

								// fmt.Printf("native ttl=%d, server name start=%d, end=%d\n", nativeTtl, meta.ServerNameValueStart, meta.ServerNameValueEnd)

								SetSocketTtl(fd, 0)
								if _, err := tcpConn.Write(tlsHeader.reconstruct(handshakeBytes[0:split])); err != nil {
									return fragmentWriteFailed(err)
								}
								// fmt.Printf("frag ttl=0\n")

								for i := split; i < meta.ServerNameValueEnd; i += step {
									var ttl int
									if 0 == mathrand.Intn(2) {
										ttl = 0
									} else {
										ttl = nativeTtl
									}
									SetSocketTtl(fd, ttl)
									if _, err := tcpConn.Write(tlsHeader.reconstruct(handshakeBytes[i:min(i+step, meta.ServerNameValueEnd)])); err != nil {
										return fragmentWriteFailed(err)
									}
									// fmt.Printf("frag ttl=%d\n", ttl)
								}

								SetSocketTtl(fd, nativeTtl)

								if _, err := tcpConn.Write(tlsHeader.reconstruct(handshakeBytes[meta.ServerNameValueEnd:])); err != nil {
									return fragmentWriteFailed(err)
								}
								// fmt.Printf("frag ttl=%d\n", nativeTtl)
							} else if self.fragment {

								if _, err := tcpConn.Write(tlsHeader.reconstruct(handshakeBytes[0:split])); err != nil {
									return fragmentWriteFailed(err)
								}

								for i := split; i < meta.ServerNameValueEnd; i += step {
									if _, err := tcpConn.Write(tlsHeader.reconstruct(handshakeBytes[i:min(i+step, meta.ServerNameValueEnd)])); err != nil {
										return fragmentWriteFailed(err)
									}
								}

								if _, err := tcpConn.Write(tlsHeader.reconstruct(handshakeBytes[meta.ServerNameValueEnd:])); err != nil {
									return fragmentWriteFailed(err)
								}

							} else if self.reorder {

								tlsBytes := tlsHeader.reconstruct(handshakeBytes)

								tcpConn.SetNoDelay(true)

								f, err := tcpConn.File()
								if err != nil {
									return 0, err
								}
								fd := SocketHandle(f.Fd())
								defer f.Close()

								nativeTtl := GetSocketTtl(fd)
								if nativeTtl <= 0 {
									// syscall failed; fall back to a single write
									n, err := tcpConn.Write(tlsBytes)
									if err != nil || n < len(tlsBytes) {
										return fragmentWriteFailed(err)
									}
									self.buffer = self.buffer[5+tlsHeader.contentLength:]
									continue
								}

								for i := 0; i*blockSize < len(tlsBytes); i += 1 {
									var ttl int
									if 0 == i%2 {
										ttl = 0
									} else {
										ttl = nativeTtl
									}
									SetSocketTtl(fd, ttl)
									b := tlsBytes[i*blockSize : min((i+1)*blockSize, len(tlsBytes))]
									if _, err := tcpConn.Write(b); err != nil {
										return fragmentWriteFailed(err)
									}
								}

								SetSocketTtl(fd, nativeTtl)

							} else {
								record := tlsHeader.reconstruct(handshakeBytes)
								n, err := tcpConn.Write(record)
								if err != nil || n < len(record) {
									return fragmentWriteFailed(err)
								}
							}

						} else {

							if self.fragment {
								if _, err := self.conn.Write(tlsHeader.reconstruct(handshakeBytes[0:split])); err != nil {
									return fragmentWriteFailed(err)
								}

								for i := split; i < meta.ServerNameValueEnd; i += step {
									if _, err := self.conn.Write(tlsHeader.reconstruct(handshakeBytes[i:min(i+step, meta.ServerNameValueEnd)])); err != nil {
										return fragmentWriteFailed(err)
									}
								}

								if _, err := self.conn.Write(tlsHeader.reconstruct(handshakeBytes[meta.ServerNameValueEnd:])); err != nil {
									return fragmentWriteFailed(err)
								}
							} else {
								record := tlsHeader.reconstruct(handshakeBytes)
								n, err := self.conn.Write(record)
								if err != nil || n < len(record) {
									return fragmentWriteFailed(err)
								}
							}

						}

					} else {
						// flush the raw record; a short or failed write leaves a
						// partial record on the wire, so fail the connection
						n, err := self.conn.Write(self.buffer[0 : 5+tlsHeader.contentLength])
						if err != nil || n < 5+int(tlsHeader.contentLength) {
							self.failConnection()
							return 0, err
						}
					}
				} else {
					// flush the raw record; a short or failed write leaves a
					// partial record on the wire, so fail the connection
					n, err := self.conn.Write(self.buffer[0 : 5+tlsHeader.contentLength])
					if err != nil || n < 5+int(tlsHeader.contentLength) {
						self.failConnection()
						return 0, err
					}
				}

				self.buffer = self.buffer[5+tlsHeader.contentLength:]
			} else {
				break
			}
		}
		return len(b), nil
	} else {
		return self.conn.Write(b)
	}
}

func (self *ResilientTlsConn) Read(b []byte) (int, error) {
	return self.conn.Read(b)
}

func (self *ResilientTlsConn) Close() error {
	return self.conn.Close()
}

func (self *ResilientTlsConn) LocalAddr() net.Addr {
	return self.conn.LocalAddr()
}

func (self *ResilientTlsConn) RemoteAddr() net.Addr {
	return self.conn.RemoteAddr()
}

func (self *ResilientTlsConn) SetDeadline(t time.Time) error {
	return self.conn.SetDeadline(t)
}

func (self *ResilientTlsConn) SetReadDeadline(t time.Time) error {
	return self.conn.SetReadDeadline(t)
}

func (self *ResilientTlsConn) SetWriteDeadline(t time.Time) error {
	return self.conn.SetWriteDeadline(t)
}
