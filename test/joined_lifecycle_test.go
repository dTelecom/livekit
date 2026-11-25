package test

import (
	"flag"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/livekit/livekit-server/pkg/testutils"
	testclient "github.com/livekit/livekit-server/test/client"
	"github.com/livekit/protocol/livekit"
)

var joinedLifecycle = flag.Bool("joined-lifecycle", false, "exercise participant replacement and moderation in an operator-supplied dedicated room")

// Run with the two participant JWT files and the matching room admin JWT file
// used by TestJoinedNodeSmoke. All mutations target the first fixture identity.
func TestJoinedParticipantLifecycle(t *testing.T) {
	if testing.Short() || !*joinedLifecycle {
		t.Skip("requires -joined-lifecycle and dedicated participant and room admin grants")
	}
	participants, err := smokeParticipants("", [2]string{
		os.Getenv("DTELECOM_SMOKE_TOKEN_A_FILE"), os.Getenv("DTELECOM_SMOKE_TOKEN_B_FILE"),
	})
	require.NoError(t, err)
	admin, err := readSmokeAdminToken(os.Getenv("DTELECOM_SMOKE_ADMIN_TOKEN_FILE"), participants[0])
	require.NoError(t, err)
	a, err := testutils.SignalingOrigin(os.Getenv("DTELECOM_SMOKE_NODE_A"))
	require.NoError(t, err)
	b, err := testutils.SignalingOrigin(os.Getenv("DTELECOM_SMOKE_NODE_B"))
	require.NoError(t, err)
	require.NotEqual(t, a, b)
	nodeA, nodeB := os.Getenv("DTELECOM_SMOKE_EXPECT_NODE_A"), os.Getenv("DTELECOM_SMOKE_EXPECT_NODE_B")
	for _, topology := range []struct{ name, observerEndpoint, expectedObserverNode string }{
		{"single", a, nodeA}, {"mesh", b, nodeB},
	} {
		t.Run(topology.name, func(t *testing.T) {
			target := &livekit.RoomParticipantIdentity{Room: participants[0].room, Identity: participants[0].identity}
			ctx := contextWithToken(admin)
			api := joinedRoomService(a)

			// The first authenticated join is the only room-creation operation.
			first := joinLifecyclePeer(t, a, nodeA, participants[0])
			observer := joinLifecyclePeer(t, topology.observerEndpoint, topology.expectedObserverNode, participants[1])
			if topology.name == "mesh" {
				require.NotEqual(t, first.node, observer.node)
			} else {
				require.Equal(t, first.node, observer.node)
			}
			waitUntilConnected(t, first.client, observer.client)
			require.Eventually(t, func() bool {
				return observer.client.GetRemoteParticipant(first.client.ID()).GetIdentity() == target.Identity
			}, testutils.ConnectTimeout, 50*time.Millisecond)

			// Occupying the same identity on the same SFU replaces the original session.
			current := joinLifecyclePeer(t, a, first.node, participants[0])
			require.NotEqual(t, first.client.ID(), current.client.ID())
			first.expectLeave(t, livekit.DisconnectReason_DUPLICATE_IDENTITY)
			waitUntilConnected(t, current.client)
			writer, err := current.client.AddStaticTrack("audio/opus", "lifecycle-mic", "microphone")
			require.NoError(t, err)
			t.Cleanup(writer.Stop)
			require.Eventually(t, func() bool {
				return observer.client.GetRemoteParticipant(first.client.ID()) == nil &&
					observer.client.BytesReceivedFrom(current.client.ID()) >= 256
			}, testutils.ConnectTimeout, 50*time.Millisecond, "observer did not receive the replacement publisher")

			// Bind administration to the exact test session before mutating it.
			info, err := api.GetParticipant(ctx, target)
			require.NoError(t, err)
			require.Equal(t, string(current.client.ID()), info.GetSid())
			require.NotNil(t, info.Permission)
			require.True(t, info.Permission.CanPublish)
			require.True(t, info.Permission.CanPublishData)
			permission := proto.Clone(info.Permission).(*livekit.ParticipantPermission)
			permission.CanPublish = false
			metadata := "lifecycle-" + info.GetSid()
			updated, err := api.UpdateParticipant(ctx, &livekit.UpdateParticipantRequest{
				Room: target.Room, Identity: target.Identity, Metadata: metadata, Permission: permission,
			})
			require.NoError(t, err)
			require.Equal(t, info.GetSid(), updated.GetSid())
			require.Equal(t, metadata, updated.GetMetadata())
			require.False(t, updated.GetPermission().GetCanPublish())
			require.Eventually(t, func() bool {
				remote := observer.client.GetRemoteParticipant(current.client.ID())
				return remote.GetMetadata() == metadata && len(remote.GetTracks()) == 0 &&
					len(current.client.GetPublishedTrackIDs()) == 0
			}, testutils.ConnectTimeout, 50*time.Millisecond, "publish revocation and metadata did not reach both clients")
			observer.expectDataFrom(t, current.client, "after-publish-revocation")

			_, err = api.RemoveParticipant(ctx, target)
			require.NoError(t, err)
			current.expectLeave(t, livekit.DisconnectReason_PARTICIPANT_REMOVED)
			require.Eventually(t, func() bool {
				list, err := api.ListParticipants(ctx, &livekit.ListParticipantsRequest{Room: target.Room})
				if err != nil || observer.client.GetRemoteParticipant(current.client.ID()) != nil {
					return false
				}
				for _, peer := range list.GetParticipants() {
					if peer.GetIdentity() == target.Identity {
						return false
					}
				}
				return true
			}, testutils.ConnectTimeout, 50*time.Millisecond, "removed session remained visible")

			// Room moderation ends the current session. A fresh join uses the original participant grant.
			rejoined := joinLifecyclePeer(t, topology.observerEndpoint, observer.node, participants[0])
			require.NotEqual(t, current.client.ID(), rejoined.client.ID())
			waitUntilConnected(t, rejoined.client)
			observer.expectDataFrom(t, rejoined.client, "after-fresh-join")
			observerInfo, err := joinedRoomService(topology.observerEndpoint).GetParticipant(ctx, &livekit.RoomParticipantIdentity{
				Room: target.Room, Identity: participants[1].identity,
			})
			require.NoError(t, err)
			require.Equal(t, string(observer.client.ID()), observerInfo.GetSid(), "observer should retain its original session")
		})
	}
}

type lifecycleData struct{ sender, body string }

type lifecyclePeer struct {
	client  *testclient.RTCClient
	node    string
	roomSID string
	leaves  chan *livekit.LeaveRequest
	data    chan lifecycleData
}

func joinLifecyclePeer(t *testing.T, endpoint, expectedNode string, participant smokeParticipant) lifecyclePeer {
	t.Helper()
	conn, response, err := dialJoinedNode(endpoint, participant)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	c, err := testclient.NewRTCClientFromJoin(conn, response)
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	peer := lifecyclePeer{
		client: c, node: response.GetServerInfo().GetNodeId(), roomSID: response.GetRoom().GetSid(),
		leaves: make(chan *livekit.LeaveRequest, 1), data: make(chan lifecycleData, 4),
	}
	if expectedNode != "" {
		require.Equal(t, expectedNode, peer.node)
	}
	c.OnLeave = func(leave *livekit.LeaveRequest) {
		select {
		case peer.leaves <- leave:
		default:
		}
	}
	c.OnDataReceived = func(data []byte, sender string) {
		select {
		case peer.data <- lifecycleData{sender, string(data)}:
		default:
		}
	}
	go func() { _ = c.Run(); close(peer.leaves) }()
	return peer
}

func (p lifecyclePeer) expectLeave(t *testing.T, reason livekit.DisconnectReason) {
	t.Helper()
	select {
	case leave := <-p.leaves:
		require.NotNil(t, leave, "socket closed before the server supplied a Leave reason")
		require.Equal(t, reason, leave.GetReason())
		require.False(t, leave.GetCanReconnect())
	case <-time.After(testutils.ConnectTimeout):
		t.Fatal("did not receive the expected terminal Leave")
	}
}

func (p lifecyclePeer) expectDataFrom(t *testing.T, sender *testclient.RTCClient, label string) {
	t.Helper()
	message := string(sender.ID()) + "/" + label
	require.NoError(t, sender.PublishData([]byte(message), livekit.DataPacket_RELIABLE))
	select {
	case got := <-p.data:
		require.Equal(t, lifecycleData{string(sender.ID()), message}, got)
	case <-time.After(testutils.ConnectTimeout):
		t.Fatalf("%s: survivor did not receive reliable data", label)
	}
}
