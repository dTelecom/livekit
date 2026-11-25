package relay

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"time"

	"github.com/pion/ice/v2"
	"github.com/pion/stun"
	"github.com/pion/transport/v2/packetio"
)

// Each inbound relay has its own ICE agent. Rooms share UDP sockets, so the
// sender's IP and port can be reused before the previous room closes.
type relayUDPMux struct {
	socket         *net.UDPConn
	mu             sync.Mutex
	closed         bool
	lastGeneration uint64
	sessions       map[string]*relayUDPConn
	owners         map[netip.AddrPort]*relayUDPConn
	checks         map[relayCheck]pendingRelayCheck
	nextSweep      time.Time
}

type relayCheck struct {
	peer netip.AddrPort
	id   [stun.TransactionIDSize]byte
}

type pendingRelayCheck struct {
	conn    *relayUDPConn
	expires time.Time
}

type relayUDPConn struct {
	mux           *relayUDPMux
	ufrag         string
	packets       *packetio.Buffer
	generation    uint64
	writeDeadline time.Time
}

func newRelayUDPMux(socket *net.UDPConn) ice.UDPMux {
	m := &relayUDPMux{
		socket:   socket,
		sessions: make(map[string]*relayUDPConn),
		owners:   make(map[netip.AddrPort]*relayUDPConn),
		checks:   make(map[relayCheck]pendingRelayCheck),
	}
	go m.readLoop()
	return m
}

func (m *relayUDPMux) GetListenAddresses() []net.Addr { return []net.Addr{m.socket.LocalAddr()} }

func (m *relayUDPMux) GetConn(ufrag string, local net.Addr) (net.PacketConn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, net.ErrClosed
	}
	if local == nil || local.String() != m.socket.LocalAddr().String() {
		return nil, net.InvalidAddrError("not a relay listener address")
	}
	c := m.sessions[ufrag]
	if c == nil {
		m.lastGeneration++
		c = &relayUDPConn{mux: m, ufrag: ufrag, packets: packetio.NewBuffer(), generation: m.lastGeneration}
		m.sessions[ufrag] = c
	}
	return c, nil
}

func (m *relayUDPMux) remove(c *relayUDPConn) {
	if c == nil || m.sessions[c.ufrag] != c {
		return
	}
	delete(m.sessions, c.ufrag)
	for peer, owner := range m.owners {
		if owner == c {
			delete(m.owners, peer)
		}
	}
	for key, check := range m.checks {
		if check.conn == c {
			delete(m.checks, key)
		}
	}
	_ = c.packets.Close()
}

func (m *relayUDPMux) RemoveConnByUfrag(ufrag string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.remove(m.sessions[ufrag])
}

func (m *relayUDPMux) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	for _, c := range m.sessions {
		_ = c.packets.Close()
	}
	clear(m.sessions)
	clear(m.owners)
	clear(m.checks)
	return m.socket.Close()
}

// Pion ICE reads UDP datagrams up to 8192 bytes. Keep the source address with
// the datagram in packetio's packet-preserving buffer, including IPv6 zones.
const relayDatagramSize = 8192

type relayReadBuffer [relayDatagramSize + 512]byte

var relayReadBuffers = sync.Pool{New: func() any { return new(relayReadBuffer) }}

func (m *relayUDPMux) readLoop() {
	defer m.Close()
	var buf relayReadBuffer
	for {
		n, peer, err := m.socket.ReadFromUDPAddrPort(buf[:relayDatagramSize])
		if err != nil {
			return
		}
		peer = netip.AddrPortFrom(peer.Addr().Unmap(), peer.Port())
		c := m.destination(buf[:n], peer)
		if c == nil {
			continue
		}
		encoded, _ := peer.MarshalBinary()
		if len(encoded)+2 > len(buf)-n {
			continue
		}
		end := n + copy(buf[n:], encoded)
		binary.BigEndian.PutUint16(buf[end:], uint16(len(encoded)))
		_, _ = c.packets.Write(buf[:end+2])
	}
}

func (m *relayUDPMux) destination(data []byte, peer netip.AddrPort) *relayUDPConn {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !stun.IsMessage(data) {
		return m.owners[peer]
	}
	msg := stun.Message{Raw: data}
	if msg.Decode() != nil {
		return nil
	}
	switch msg.Type {
	case stun.BindingRequest:
		name, err := msg.Get(stun.AttrUsername)
		local, _, ok := bytes.Cut(name, []byte(":"))
		if err != nil || !ok {
			return nil
		}
		// Dispatch only. The agent checks the full username and MESSAGE-INTEGRITY
		// before writing a success response. Receipt never transfers the media route.
		return m.sessions[string(local)]
	case stun.BindingSuccess, stun.BindingError:
		check := m.checks[relayCheck{peer, msg.TransactionID}]
		if time.Now().Before(check.expires) {
			return check.conn
		}
		return nil
	default:
		return m.owners[peer]
	}
}

func (c *relayUDPConn) ReadFrom(b []byte) (int, net.Addr, error) {
	buf := relayReadBuffers.Get().(*relayReadBuffer)
	defer relayReadBuffers.Put(buf)
	n, err := c.packets.Read(buf[:])
	if err != nil {
		return 0, nil, err
	}
	size := n - 2 - int(binary.BigEndian.Uint16(buf[n-2:n]))
	var peer netip.AddrPort
	if err := peer.UnmarshalBinary(buf[size : n-2]); err != nil {
		return 0, nil, err
	}
	if size > len(b) {
		return 0, nil, io.ErrShortBuffer
	}
	return copy(b, buf[:size]), net.UDPAddrFromAddrPort(peer), nil
}

func (c *relayUDPConn) WriteTo(b []byte, remote net.Addr) (int, error) {
	addr, ok := remote.(*net.UDPAddr)
	if !ok || addr == nil {
		return 0, net.InvalidAddrError("expected a UDP peer")
	}
	peer := addr.AddrPort()
	peer = netip.AddrPortFrom(peer.Addr().Unmap(), peer.Port())
	m := c.mux
	m.mu.Lock()
	err := c.prepareWrite(b, peer)
	m.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return m.socket.WriteToUDPAddrPort(b, peer)
}

// Called under mux.mu. Only a local ICE agent's authenticated response (or
// controlling-side nomination of a validated pair) can transfer a media route.
func (c *relayUDPConn) prepareWrite(data []byte, peer netip.AddrPort) error {
	m := c.mux
	if m.sessions[c.ufrag] != c {
		return net.ErrClosed
	}
	if !c.writeDeadline.IsZero() && !time.Now().Before(c.writeDeadline) {
		return os.ErrDeadlineExceeded
	}
	if !stun.IsMessage(data) {
		if m.owners[peer] != c {
			return io.ErrClosedPipe
		}
		return nil
	}
	msg := stun.Message{Raw: data}
	if err := msg.Decode(); err != nil {
		return err
	}
	if msg.Type == stun.BindingSuccess || (msg.Type == stun.BindingRequest && msg.Contains(stun.AttrUseCandidate)) {
		// An older agent may validate a delayed check even if it never owned
		// this endpoint. Only advance ownership while a live owner exists.
		if owner := m.owners[peer]; owner != nil && owner.generation > c.generation {
			return io.ErrClosedPipe
		}
		m.owners[peer] = c
	}
	if msg.Type == stun.BindingRequest {
		now := time.Now()
		if !now.Before(m.nextSweep) {
			for key, check := range m.checks {
				if !now.Before(check.expires) {
					delete(m.checks, key)
				}
			}
			m.nextSweep = now.Add(time.Second)
		}
		// Responses carry no username. Match each to its originating agent
		// even when another session still owns media from this endpoint.
		m.checks[relayCheck{peer, msg.TransactionID}] = pendingRelayCheck{c, now.Add(10 * time.Second)}
	}
	return nil
}

func (c *relayUDPConn) LocalAddr() net.Addr { return c.mux.socket.LocalAddr() }

func (c *relayUDPConn) Close() error {
	c.mux.mu.Lock()
	defer c.mux.mu.Unlock()
	c.mux.remove(c)
	return nil
}

func (c *relayUDPConn) SetReadDeadline(t time.Time) error { return c.packets.SetReadDeadline(t) }
func (c *relayUDPConn) SetWriteDeadline(t time.Time) error {
	c.mux.mu.Lock()
	c.writeDeadline = t
	c.mux.mu.Unlock()
	return nil
}
func (c *relayUDPConn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}
