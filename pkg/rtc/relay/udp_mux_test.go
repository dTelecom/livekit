package relay

import (
	"errors"
	"io"
	"net"
	"net/netip"
	"testing"

	"github.com/pion/ice/v2"
	"github.com/pion/stun"
	"github.com/stretchr/testify/require"
)

// Exercise dispatch on real listener/session registrations; no datagrams leave
// the test. Pion authenticates routed checks before returning a success.
func TestRelayMuxBindingCheckRouting(t *testing.T) {
	peer := netip.MustParseAddrPort("127.0.0.1:9000")
	media := []byte("established session data")
	credentials := stun.NewShortTermIntegrity("loopback-test-session-password")
	check := func(username string) *stun.Message {
		return stun.MustBuild(stun.TransactionID, stun.BindingRequest, stun.NewUsername(username), credentials, stun.Fingerprint)
	}
	response := func(request *stun.Message) *stun.Message {
		return stun.MustBuild(stun.NewTransactionIDSetter(request.TransactionID), stun.BindingSuccess, credentials, stun.Fingerprint)
	}
	type listener struct {
		mux       *relayUDPMux
		old, next *relayUDPConn
	}
	newListener := func(t *testing.T) listener {
		t.Helper()
		socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		require.NoError(t, err)
		m := newRelayUDPMux(socket).(*relayUDPMux)
		t.Cleanup(func() { _ = m.Close() })
		old, err := m.GetConn("old", socket.LocalAddr())
		require.NoError(t, err)
		next, err := m.GetConn("next", socket.LocalAddr())
		require.NoError(t, err)
		return listener{m, old.(*relayUDPConn), next.(*relayUDPConn)}
	}

	write := func(c *relayUDPConn, message *stun.Message, remote netip.AddrPort) error {
		c.mux.mu.Lock()
		defer c.mux.mu.Unlock()
		return c.prepareWrite(message.Raw, remote)
	}

	t.Run("receiving a check leaves media on its established connection", func(t *testing.T) {
		a := newListener(t)
		require.NoError(t, write(a.old, response(check("old:sender")), peer))
		request := check("next:sender")
		require.Same(t, a.next, a.mux.destination(request.Raw, peer))
		require.Same(t, a.old, a.mux.destination(media, peer))
		// This is the success Pion writes after validating the check.
		require.NoError(t, write(a.next, response(request), peer))
		require.Same(t, a.next, a.mux.destination(media, peer))
	})

	t.Run("full ICE replies follow their local socket and transaction", func(t *testing.T) {
		a, b := newListener(t), newListener(t)
		// A newer session owns media while both agents have outstanding checks.
		for _, socket := range []listener{a, b} {
			require.NoError(t, write(socket.next, response(check("next:sender")), peer))
		}
		first, second := check("sender:next"), check("sender:old")
		require.NoError(t, write(a.next, first, peer))
		require.NoError(t, write(a.old, second, peer))
		require.NoError(t, write(b.next, second, peer))

		// Replies can arrive out of order and carry no username.
		firstReply, secondReply := response(first), response(second)
		require.Same(t, a.old, a.mux.destination(secondReply.Raw, peer))
		require.Same(t, a.next, a.mux.destination(firstReply.Raw, peer))
		require.Same(t, b.next, b.mux.destination(secondReply.Raw, peer))
		require.Nil(t, b.mux.destination(firstReply.Raw, peer))
		otherPeer := netip.AddrPortFrom(peer.Addr(), peer.Port()+1)
		require.Nil(t, a.mux.destination(firstReply.Raw, otherPeer))
		require.Same(t, a.next, a.mux.destination(media, peer))
		require.Same(t, b.next, b.mux.destination(media, peer))
	})

	for _, tc := range []struct {
		name     string
		oldWrite *stun.Message
	}{
		{"delayed success", response(check("old:sender"))},
		{"delayed nomination", stun.MustBuild(stun.TransactionID, stun.BindingRequest, stun.NewUsername("sender:old"), ice.UseCandidate(), credentials, stun.Fingerprint)},
	} {
		t.Run(tc.name+" from a session that never owned the endpoint", func(t *testing.T) {
			a := newListener(t)
			require.Nil(t, a.mux.destination(media, peer))
			require.NoError(t, write(a.next, response(check("next:sender")), peer))
			err := write(a.old, tc.oldWrite, peer)
			require.True(t, err == nil || errors.Is(err, io.ErrClosedPipe), "unexpected stale write error: %v", err)
			require.Same(t, a.next, a.mux.destination(media, peer))

			// A different, unclaimed endpoint remains available to the older session.
			freePeer := netip.AddrPortFrom(peer.Addr(), peer.Port()+1)
			require.NoError(t, write(a.old, tc.oldWrite, freePeer))
			require.Same(t, a.old, a.mux.destination(media, freePeer))
			require.NoError(t, a.old.Close())
			require.Nil(t, a.mux.destination(media, freePeer))
			require.Same(t, a.next, a.mux.destination(media, peer))
		})
	}
}
