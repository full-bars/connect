//go:build !js

package connect

import (
	"net"
	"sync"
	"time"

	"github.com/pion/transport/v4"
	"github.com/pion/transport/v4/stdnet"
)

// iceInterfaceNet is a pion transport.Net that supplies interface enumeration
// for ICE host-candidate gathering WITHOUT reading netlink.
//
// Android (API 30+) denies apps the netlink route dump, so Go's
// net.Interfaces() — and therefore pion's default host-candidate gathering —
// fails with "route ip+net: netlinkrib: permission denied". The result is zero
// host candidates, no candidate pair, and every peer connection stalls on the
// WAN relay instead of the direct LAN path (OPTIMIZENETWORKPEER1.md §5.1 /
// PACKETRESEARCH1 §17). This is not fixed by VpnService.protect or app
// exclusion (the app is already excluded, so its sockets egress the physical
// interface) — pion simply cannot see the interface to build the candidate.
//
// The local egress address is discovered with a connect()-only UDP "dial
// trick", which consults the in-kernel routing table rather than netlink, and
// is surfaced as a single synthetic interface. Socket operations delegate to
// the embedded stdnet.Net; those use the standard net package (not netlink) and
// already work. Because the app is excluded from its own VPN, the dial trick
// returns the physical wlan0/cellular address — exactly the host candidate ICE
// needs to reach a same-premises peer directly.
type iceInterfaceNet struct {
	transport.Net
	stateLock   sync.Mutex
	interfaces  []*transport.Interface
	lastRefresh time.Time
}

// newIceInterfaceNet builds the synthetic-interface net. The synthetic
// interface carries only the current egress address (discovered via a
// connect-only UDP dial trick), avoiding the quadratic candidate-pair
// explosion caused by virtual/tunnel/bridge interfaces. When
// enumeration is denied (Android 11+), this is the only way to gather
// host candidates. When enumeration works (desktop, Android WiFi), we
// still use the synthetic interface to filter out private/CGNAT
// addresses that would be offered as unreachable host candidates.
func newIceInterfaceNet(log Logger, _ bool) (transport.Net, bool) {
	base, _ := stdnet.NewNet() // usable for sockets even though enumeration failed
	ifcs := localEgressInterfaces()
	if len(ifcs) == 0 {
		return nil, false
	}
	if log.V(1).Enabled() {
		for _, ifc := range ifcs {
			addrs, _ := ifc.Addrs()
			log.Infof("[ice-if]synthetic %s addrs=%v\n", ifc.Name, addrs)
		}
	}
	return &iceInterfaceNet{
		Net:         base,
		interfaces:  ifcs,
		lastRefresh: time.Now(),
	}, true
}

func (self *iceInterfaceNet) Interfaces() ([]*transport.Interface, error) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	// One manager factory can gather several peer connections concurrently.
	// Re-running two route-probe dials for every gather adds sockets and setup
	// latency without discovering a different path. NetworkChanged rebuilds
	// the factory immediately; this low-frequency refresh is only a fallback
	// for hosts that fail to report a path transition.
	if time.Second <= time.Since(self.lastRefresh) {
		if interfaces := localEgressInterfaces(); 0 < len(interfaces) {
			self.interfaces = interfaces
		}
		self.lastRefresh = time.Now()
	}
	return self.interfaces, nil
}

func (self *iceInterfaceNet) InterfaceByIndex(index int) (*transport.Interface, error) {
	interfaces, _ := self.Interfaces()
	for _, ifc := range interfaces {
		if ifc.Index == index {
			return ifc, nil
		}
	}
	return nil, transport.ErrInterfaceNotFound
}

func (self *iceInterfaceNet) InterfaceByName(name string) (*transport.Interface, error) {
	interfaces, _ := self.Interfaces()
	for _, ifc := range interfaces {
		if ifc.Name == name {
			return ifc, nil
		}
	}
	return nil, transport.ErrInterfaceNotFound
}

// localEgressInterfaces discovers the local egress IPv4/IPv6 via a
// connect()-only UDP dial (no packet is sent; the kernel resolves the route
// and assigns a local address) and wraps each as a synthetic pion interface
// carrying a host address. Nil entries (no route for a family) are skipped.
// egressIPv6Usable reports whether the device can actually reach the
// internet over IPv6. A connect()-only UDP dial or a fire-and-forget
// sendto is not enough: on Android the kernel has a native IPv6 route (e.g.
// from AT&T cellular) so connect()/sendto succeeds, but the VPN tunnel
// swallows outbound v6 traffic. We send a minimal DNS query to Google DNS
// over IPv6 and require a response within 2 seconds. If no response
// (i/o timeout) we know the tunnel blackholes v6 and must not gather
// IPv6 ICE candidates.
func egressIPv6Usable() bool {
	pc, err := net.ListenPacket("udp6", "[::]:0")
	if err != nil {
		return false
	}
	defer pc.Close()
	// Build a minimal DNS query for "a.gtld-servers.net" (A record) —
	// 32 bytes, enough to trigger a response from Google DNS.
	// DNS header: ID=0x0102, flags=0x0100 (standard query, RD=1)
	// Questions=1, Answers/Authority/Additional=0
	query := []byte{
		0x01, 0x02, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}
	// Encode "a.gtld-servers.net" as a DNS query name (labels).
	labels := [][]byte{[]byte("a"), []byte("gtld-servers"), []byte("net")}
	for _, lbl := range labels {
		query = append(query, byte(len(lbl)))
		query = append(query, lbl...)
	}
	query = append(query, 0) // root label
	query = append(query, 0x00, 0x01, 0x00, 0x01) // type A, class IN
	dnsServer := &net.UDPAddr{IP: net.ParseIP("2001:4860:4860::8888"), Port: 53}
	_, err = pc.WriteTo(query, dnsServer)
	if err != nil {
		return false
	}
	// Wait for DNS response — proves v6 packets actually reach the internet.
	pc.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 512)
	_, _, err = pc.ReadFrom(buf)
	return err == nil
}

// egressIPv4Usable reports whether the kernel has a default IPv4 route to the
// internet, using the connect()-only dial trick.
func egressIPv4Usable() bool {
	return dialLocalIP("udp4", "8.8.8.8:80") != nil
}

func localEgressInterfaces() []*transport.Interface {
	var out []*transport.Interface
	nativeInterfaces, nativeInterfacesErr := net.Interfaces()
	add := func(name string, index int, ip net.IP, maskBits int) {
		selected := net.Interface{
			Index: index,
			MTU:   1500,
			Name:  name,
			Flags: net.FlagUp | net.FlagRunning | net.FlagBroadcast | net.FlagMulticast,
		}
		selectedMask := net.CIDRMask(maskBits, maskBits)
		if nativeInterfacesErr == nil {
			// Desktop/device platforms that can enumerate interfaces still use
			// the route probe to select only the current egress address, but
			// retain its real interface identity and prefix. Supplying made-up
			// indices (the old en0=1/en1=2 fallback) made repeated ICE gathers
			// intermittently strand one side in checking, particularly when a
			// usable IPv6 address was present. Android's denied-netlink path
			// continues to use the synthetic identity below.
			for _, native := range nativeInterfaces {
				addrs, err := native.Addrs()
				if err != nil {
					continue
				}
				found := false
				for _, addr := range addrs {
					var addrIp net.IP
					var addrMask net.IPMask
					switch typed := addr.(type) {
					case *net.IPNet:
						addrIp = typed.IP
						addrMask = typed.Mask
					case *net.IPAddr:
						addrIp = typed.IP
					}
					if addrIp == nil || !addrIp.Equal(ip) {
						continue
					}
					selected = native
					if addrMask != nil {
						selectedMask = addrMask
					}
					found = true
					break
				}
				if found {
					break
				}
			}
		}
		selectedAddr := &net.IPNet{IP: ip, Mask: selectedMask}
		for _, existing := range out {
			if existing.Index == selected.Index && existing.Name == selected.Name {
				existing.AddAddress(selectedAddr)
				return
			}
		}
		ifc := transport.NewInterface(selected)
		ifc.AddAddress(selectedAddr)
		out = append(out, ifc)
	}
	if ip := dialLocalIP("udp4", "8.8.8.8:80"); ip != nil {
		// Behind carrier-grade NAT (e.g. AT&T cellular), the kernel's
		// egress address is a private RFC1918 or CGNAT address (10.x,
		// 192.168.x, 100.64.x). This address is unreachable from
		// internet peers — offering it as an ICE host candidate wastes
		// gather cycles and causes "Failed to ping without candidate
		// pairs" storms when the remote peer can never route to it.
		// URNetwork peers are always internet-facing, so srflx
		// candidates are the only viable path behind NAT. Filtering
		// private IPs here means localEgressInterfaces() returns
		// empty, newIceInterfaceNet falls back to Pion's default net,
		// and STUN gathering still produces srflx candidates via the
		// kernel's routing table (host candidates are suppressed).
		if !isPrivateOrCGNAT(ip) {
			add("en0", 1, ip, 32)
		}
	}
	if egressIPv6Usable() {
		if ip := dialLocalIP("udp6", "[2001:4860:4860::8888]:80"); ip != nil {
			add("en1", 2, ip, 128)
		}
	}
	return out
}

// dialLocalIP returns the local address the kernel would use to reach addr,
// via a connect()-only UDP socket. This does not send a packet and does not
// read netlink, so it works where net.Interfaces() is denied. Unspecified /
// loopback results are rejected.
func dialLocalIP(network, addr string) net.IP {
	conn, err := net.Dial(network, addr)
	if err != nil {
		return nil
	}
	defer conn.Close()
	ua, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok || ua.IP == nil || ua.IP.IsUnspecified() || ua.IP.IsLoopback() {
		return nil
	}
	return ua.IP
}

// isPrivateOrCGNAT reports whether ip falls in an RFC1918, RFC6598 (CGNAT),
// link-local, or other non-routable range. These addresses are unreachable
// from internet peers and should not be offered as ICE host candidates.
func isPrivateOrCGNAT(ip net.IP) bool {
	if ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	// CGNAT (RFC6598): 100.64.0.0/10
	if ip4 := ip.To4(); ip4 != nil {
		return ip4[0] == 100 && ip4[1] >= 64 && ip4[1] <= 127
	}
	return false
}
