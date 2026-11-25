package test

import (
	"flag"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/livekit/livekit-server/pkg/testutils"
	testclient "github.com/livekit/livekit-server/test/client"
	"github.com/livekit/protocol/livekit"
)

var joinedParticipants = flag.Bool("joined-participants", false, "exercise additional signed participant grants in a disposable room")

func TestJoinedAdditionalParticipants(t *testing.T) {
	if testing.Short() || !*joinedParticipants {
		t.Skip("requires -joined-participants and dedicated signed participant grants")
	}
	participants, err := smokeParticipants("", [2]string{
		os.Getenv("DTELECOM_SMOKE_TOKEN_A_FILE"), os.Getenv("DTELECOM_SMOKE_TOKEN_B_FILE"),
	})
	require.NoError(t, err)
	a, err := testutils.SignalingOrigin(os.Getenv("DTELECOM_SMOKE_NODE_A"))
	require.NoError(t, err)
	b, err := testutils.SignalingOrigin(os.Getenv("DTELECOM_SMOKE_NODE_B"))
	require.NoError(t, err)
	require.NotEqual(t, a, b)
	nodeA, nodeB := os.Getenv("DTELECOM_SMOKE_EXPECT_NODE_A"), os.Getenv("DTELECOM_SMOKE_EXPECT_NODE_B")
	additional := func(t *testing.T, env string, canSubscribe bool) smokeParticipant {
		t.Helper()
		raw, err := os.ReadFile(os.Getenv(env))
		require.NoError(t, err, env)
		p, err := verifyJoinedParticipantToken(strings.TrimSpace(string(raw)), canSubscribe)
		require.NoError(t, err)
		require.Equal(t, participants[0].issuer, p.issuer)
		require.Equal(t, participants[0].room, p.room)
		for _, other := range participants {
			require.NotEqual(t, other.identity, p.identity)
		}
		return p
	}
	t.Run("republish", func(t *testing.T) {
		third := additional(t, "DTELECOM_SMOKE_TOKEN_C_FILE", true)
		staying := joinLifecyclePeer(t, a, nodeA, participants[0])
		moving := joinLifecyclePeer(t, b, nodeB, participants[1])
		observer := joinLifecyclePeer(t, a, staying.node, third)
		require.NotEqual(t, staying.node, moving.node)
		waitUntilConnected(t, staying.client, moving.client, observer.client)
		publishJoinedTracks(t, staying.client)
		publishJoinedTracks(t, moving.client)
		expectJoinedTracks(t, observer.client, staying.client, "audio/opus", "video/vp8")
		expectJoinedTracks(t, observer.client, moving.client, "audio/opus", "video/vp8")
		expectJoinedTracks(t, staying.client, moving.client, "audio/opus", "video/vp8")

		moving.client.Stop()
		require.Eventually(t, func() bool {
			for _, c := range []*testclient.RTCClient{staying.client, observer.client} {
				if c.GetRemoteParticipant(moving.client.ID()) != nil || len(c.SubscribedTracks()[moving.client.ID()]) != 0 {
					return false
				}
			}
			return true
		}, testutils.ConnectTimeout, 50*time.Millisecond, "original session or its tracks still visible")
		before := observer.client.BytesReceivedFrom(staying.client.ID())
		require.Eventually(t, func() bool {
			return observer.client.BytesReceivedFrom(staying.client.ID()) > before
		}, testutils.ConnectTimeout, 50*time.Millisecond, "unrelated publisher stopped during the move")

		moved := joinLifecyclePeer(t, a, staying.node, participants[1])
		require.NotEqual(t, moving.client.ID(), moved.client.ID())
		waitUntilConnected(t, moved.client)
		publishJoinedTracks(t, moved.client)
		expectJoinedTracks(t, observer.client, moved.client, "audio/opus", "video/vp8")
		expectJoinedTracks(t, staying.client, moved.client, "audio/opus", "video/vp8")
	})

	t.Run("enable-subscribe", func(t *testing.T) {
		restricted := additional(t, "DTELECOM_SMOKE_RESTRICTED_TOKEN_FILE", false)
		token, err := readSmokeAdminToken(os.Getenv("DTELECOM_SMOKE_ADMIN_TOKEN_FILE"), restricted)
		require.NoError(t, err)
		ctx := contextWithToken(token)
		api := joinedRoomService(b)
		target := &livekit.RoomParticipantIdentity{Room: restricted.room, Identity: restricted.identity}
		publisher := joinLifecyclePeer(t, a, nodeA, participants[0])
		receiver := joinLifecyclePeer(t, b, nodeB, restricted)
		require.NotEqual(t, publisher.node, receiver.node)
		waitUntilConnected(t, publisher.client, receiver.client)
		initial, err := api.GetParticipant(ctx, target)
		require.NoError(t, err)
		require.Equal(t, string(receiver.client.ID()), initial.GetSid())
		require.NotNil(t, initial.Permission)
		require.False(t, initial.Permission.CanSubscribe)
		publishJoinedTracks(t, publisher.client)
		require.Eventually(t, func() bool {
			return len(receiver.client.GetRemoteParticipant(publisher.client.ID()).GetTracks()) == 2
		}, testutils.ConnectTimeout, 50*time.Millisecond, "signed-in receiver did not see advertised tracks")
		require.Never(t, func() bool {
			return receiver.client.BytesReceivedFrom(publisher.client.ID()) != 0
		}, time.Second, 20*time.Millisecond, "received RTP while the signed grant disallowed subscription")

		permission := proto.Clone(initial.Permission).(*livekit.ParticipantPermission)
		permission.CanSubscribe = true
		updated, err := api.UpdateParticipant(ctx, &livekit.UpdateParticipantRequest{
			Room: restricted.room, Identity: restricted.identity, Permission: permission,
		})
		require.NoError(t, err)
		require.Equal(t, initial.GetSid(), updated.GetSid())
		require.True(t, updated.GetPermission().GetCanSubscribe())
		expectJoinedTracks(t, receiver.client, publisher.client, "audio/opus", "video/vp8")
	})
}
