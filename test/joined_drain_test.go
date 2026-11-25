package test

import (
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/livekit/livekit-server/pkg/testutils"
	testclient "github.com/livekit/livekit-server/test/client"
	"github.com/livekit/protocol/livekit"
)

var joinedDrain = flag.Bool("joined-drain", false, "observe an operator-initiated drain of joined test node A and rejoin node B")

// Supply the two token files and node origins used by TestJoinedNodeSmoke.
// The operator sends SIGTERM to the owned node A after DTELECOM_DRAIN_READY.
// Node B stays up with its original participant and track for the entire test.
func TestJoinedNodeDrain(t *testing.T) {
	if testing.Short() || !*joinedDrain {
		t.Skip("requires -joined-drain, two test room tokens, and an operator to drain node A")
	}
	participants, err := smokeParticipants("", [2]string{
		os.Getenv("DTELECOM_SMOKE_TOKEN_A_FILE"), os.Getenv("DTELECOM_SMOKE_TOKEN_B_FILE"),
	})
	require.NoError(t, err)
	draining, err := testutils.SignalingOrigin(os.Getenv("DTELECOM_SMOKE_NODE_A"))
	require.NoError(t, err)
	surviving, err := testutils.SignalingOrigin(os.Getenv("DTELECOM_SMOKE_NODE_B"))
	require.NoError(t, err)
	require.NotEqual(t, draining, surviving)
	join := func(endpoint string, participant smokeParticipant, expectedNode string) (*testclient.RTCClient, string) {
		t.Helper()
		conn, response, err := dialJoinedNode(endpoint, participant)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		node := response.GetServerInfo().GetNodeId()
		if expectedNode != "" {
			require.Equal(t, expectedNode, node)
		}
		c, err := testclient.NewRTCClientFromJoin(conn, response)
		require.NoError(t, err)
		t.Cleanup(c.Stop)
		return c, node
	}

	original, firstNode := join(draining, participants[0], os.Getenv("DTELECOM_SMOKE_EXPECT_NODE_A"))
	observer, secondNode := join(surviving, participants[1], os.Getenv("DTELECOM_SMOKE_EXPECT_NODE_B"))
	require.NotEqual(t, firstNode, secondNode, "the two initial joins must land on different nodes")
	leaves := make(chan *livekit.LeaveRequest, 1)
	original.OnLeave = func(leave *livekit.LeaveRequest) {
		select {
		case leaves <- leave:
		default:
		}
	}
	type packet struct{ payload, sender string }
	inbox := make(chan packet, 8)
	observer.OnDataReceived = func(data []byte, sender string) {
		select {
		case inbox <- packet{string(data), sender}:
		default:
		}
	}
	go func() {
		_ = original.Run()
		close(leaves)
	}()
	go observer.Run()
	waitUntilConnected(t, original, observer)
	track, err := observer.AddStaticTrack("audio/opus", "drain-audio", "microphone")
	require.NoError(t, err)
	t.Cleanup(track.Stop)
	exchange := func(c *testclient.RTCClient, phase string) {
		t.Helper()
		require.Eventually(t, func() bool {
			return c.BytesReceivedFrom(observer.ID()) >= 256
		}, testutils.ConnectTimeout, 50*time.Millisecond, "%s: survivor's Opus RTP did not arrive", phase)
		payload := participants[0].room + "/" + phase
		require.NoError(t, c.PublishData([]byte(payload), livekit.DataPacket_RELIABLE))
		select {
		case got := <-inbox:
			require.Equal(t, packet{payload, string(c.ID())}, got)
		case <-time.After(testutils.ConnectTimeout):
			t.Fatalf("%s: survivor did not receive the reliable data packet", phase)
		}
	}
	exchange(original, "before-drain")
	t.Logf("DTELECOM_DRAIN_READY node_a=%s node_b=%s", firstNode, secondNode)
	select {
	case leave := <-leaves:
		require.NotNil(t, leave, "signaling closed without a server Leave")
		require.True(t, leave.CanReconnect, "server Leave must allow a full reconnect")
		require.Equal(t, livekit.DisconnectReason_SERVER_SHUTDOWN, leave.Reason)
	case <-time.After(90 * time.Second):
		t.Fatal("timed out waiting for the draining node's Leave")
	}
	original.Stop()
	rejoined, _ := join(surviving, participants[0], secondNode)
	go rejoined.Run()
	waitUntilConnected(t, rejoined)
	exchange(rejoined, "after-rejoin")
	t.Log("observed reconnectable SERVER_SHUTDOWN, rejoined node B with the same identity, received RTP and delivered reliable data")
}

// Check that the test client delivers the actual Leave before the signaling socket closes.
func TestJoinedDrainLeaveSignal(t *testing.T) {
	leave := &livekit.LeaveRequest{CanReconnect: true, Reason: livekit.DisconnectReason_SERVER_SHUTDOWN}
	payload, err := proto.Marshal(&livekit.SignalResponse{Message: &livekit.SignalResponse_Leave{Leave: leave}})
	require.NoError(t, err)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		if err := conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	endpoint, err := testutils.SignalingOrigin(server.URL)
	require.NoError(t, err)
	conn, _, err := websocket.DefaultDialer.Dial(endpoint, nil)
	require.NoError(t, err)
	defer conn.Close()
	client, err := testclient.NewRTCClientFromJoin(conn, &livekit.JoinResponse{
		Participant: &livekit.ParticipantInfo{Sid: "PA_drain", Identity: "test"}, SubscriberPrimary: true,
	})
	require.NoError(t, err)
	defer client.Stop()
	var observed *livekit.LeaveRequest
	client.OnLeave = func(msg *livekit.LeaveRequest) { observed = msg }
	_ = client.Run()
	require.True(t, proto.Equal(leave, observed))
}
