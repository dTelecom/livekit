package pc

import (
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/pion/ice/v2"
	"github.com/pion/webrtc/v3"
	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/rtc/relay"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/telemetry/prometheus"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
)

// Advertise a single ingress address so the replacement must use the same route.
// Reads, writes and connection cleanup still use the SFU's shared listener.
type singleIngressAddress struct {
	ice.UDPMux
	addr net.Addr
}

func (m singleIngressAddress) GetListenAddresses() []net.Addr { return []net.Addr{m.addr} }

func freeRelayPorts(t *testing.T) (ports [2]uint32) {
	t.Helper()
	for i := range ports {
		sock, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		require.NoError(t, err)
		defer sock.Close()
		ports[i] = uint32(sock.LocalAddr().(*net.UDPAddr).Port)
	}
	return ports
}

func TestRelayEndpointReuseDuringTeardown(t *testing.T) {
	prometheus.Init("test", livekit.NodeType_SERVER, "test")
	t.Run("ICE Lite", func(t *testing.T) { testRelayEndpointReuse(t, true) })
	t.Run("full ICE", func(t *testing.T) { testRelayEndpointReuse(t, false) })
}

func testRelayEndpointReuse(t *testing.T, lite bool) {
	ports := freeRelayPorts(t)
	cfg := &config.Config{}
	cfg.RTC.NodeIP = "127.0.0.1"
	cfg.RTC.IPs.Includes = []string{"127.0.0.1/32"}
	cfg.RTC.Relay = config.RelayConfig{
		UdpPort: ports[0], PortRangeStart: ports[1], PortRangeEnd: ports[1],
		UseICELite: lite, EnableLoopbackCandidate: true,
	}
	rtcConfig, err := relay.NewWebRTCRelayConfig(cfg, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rtcConfig.RelayUDPMux.Close() })
	addresses := rtcConfig.RelayUDPMux.GetListenAddresses()
	require.NotEmpty(t, addresses)
	mux := singleIngressAddress{rtcConfig.RelayUDPMux, addresses[0]}

	open := func(name, side string, senderPort uint16, engine webrtc.SettingEngine) *PcRelay {
		t.Helper()
		if side == "out" {
			require.NoError(t, engine.SetEphemeralUDPPortRange(senderPort, senderPort))
		}
		r, err := NewRelay(logger.GetLogger(), &relay.RelayConfig{
			ID: name, Side: side, SettingEngine: engine,
			RelayUDPMux: mux, RelayUdpPort: rtcConfig.RelayUdpPort,
			BufferFactory: buffer.NewFactoryOfBufferFactory(100).CreateBufferFactory(),
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			r.Close()
			select {
			case <-r.Closed():
			case <-time.After(5 * time.Second):
				t.Errorf("%s %s did not close", name, side)
			}
		})
		return r
	}
	type pair struct {
		out, in  *PcRelay
		received chan string
	}
	connect := func(name string, senderPort uint16) pair {
		t.Helper()
		p := pair{open(name, "out", senderPort, rtcConfig.SettingEngine), open(name, "in", 0, rtcConfig.SettingEngine), make(chan string, 1)}
		p.in.OnMessage(func(_ uint64, msg []byte) { p.received <- string(msg) })
		outReady, inReady := make(chan struct{}), make(chan struct{})
		p.out.OnReady(func() { close(outReady) })
		p.in.OnReady(func() { close(inReady) })
		require.NoError(t, p.out.Offer(p.in.Answer))
		for _, started := range []chan struct{}{outReady, inReady} {
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatalf("%s relay did not establish", name)
			}
		}
		return p
	}
	send := func(p pair, message string) {
		t.Helper()
		require.NoError(t, p.out.SendMessage([]byte(message)))
		select {
		case got := <-p.received:
			require.Equal(t, message, got)
		case <-time.After(3 * time.Second):
			t.Fatalf("missing relay message %q", message)
		}
	}
	old := connect("original", 0)
	other := connect("independent", 0)
	send(old, "old session")
	send(other, "independent before replacement")
	transport := old.out.pc.SCTP().Transport().ICETransport()
	selected, err := transport.GetSelectedCandidatePair()
	require.NoError(t, err)
	require.NotNil(t, selected)
	// The sender's endpoint becomes available while the receiver still owns its old room.
	require.NoError(t, transport.Stop())

	// Both peers receive an incorrect remote ICE password from signaling. Let
	// their real agents time out before the intended replacement uses this port.
	shortChecks := rtcConfig.SettingEngine
	shortChecks.SetICETimeouts(200*time.Millisecond, 200*time.Millisecond, 100*time.Millisecond)
	mismatchOut := open("mismatched credentials", "out", selected.Local.Port, shortChecks)
	mismatchIn := open("mismatched credentials", "in", 0, shortChecks)
	failed := make(chan struct{})
	mismatchOut.OnConnectionStateChange(func(state webrtc.ICEConnectionState) {
		if state == webrtc.ICEConnectionStateFailed {
			close(failed)
		}
	})
	require.NoError(t, mismatchOut.Offer(func(offer []byte) ([]byte, error) {
		answer, err := mismatchIn.Answer(withIncorrectICEPassword(t, offer))
		if err != nil {
			return nil, err
		}
		return withIncorrectICEPassword(t, answer), nil
	}))
	select {
	case <-failed:
	case <-time.After(3 * time.Second):
		t.Fatal("expected ICE to reject the mismatched credentials")
	}
	send(other, "independent after mismatched credentials")
	mismatchOut.Close()
	select {
	case <-mismatchOut.Closed():
	case <-time.After(5 * time.Second):
		t.Fatal("misconfigured sender did not close")
	}

	replacement := connect("replacement", selected.Local.Port)
	reused, err := replacement.out.pc.SCTP().Transport().ICETransport().GetSelectedCandidatePair()
	require.NoError(t, err)
	require.NotNil(t, reused)
	require.Equal(t, selected.Local.Port, reused.Local.Port)
	require.Equal(t, selected.Remote.Port, reused.Remote.Port)
	send(replacement, "replacement before old receiver closes")
	send(other, "independent during overlap")
	old.in.Close()
	select {
	case <-old.in.Closed():
	case <-time.After(5 * time.Second):
		t.Fatal("old receiver did not close")
	}
	send(replacement, "replacement after old receiver closes")
	send(other, "independent after old receiver closes")
}

func withIncorrectICEPassword(t *testing.T, signaling []byte) []byte {
	t.Helper()
	var signal sessionDescriptionWithIceCandidates
	require.NoError(t, json.Unmarshal(signaling, &signal))
	lines := strings.Split(signal.SessionDescription.SDP, "\r\n")
	changed := false
	for i, line := range lines {
		if strings.HasPrefix(line, "a=ice-pwd:") {
			lines[i] = "a=ice-pwd:password-from-a-different-test-session"
			changed = true
		}
	}
	require.True(t, changed, "session description must include ICE credentials")
	signal.SessionDescription.SDP = strings.Join(lines, "\r\n")
	data, err := json.Marshal(signal)
	require.NoError(t, err)
	return data
}
