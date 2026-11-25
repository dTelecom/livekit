package test

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/testutils"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
)

var joinedRooms = flag.Bool("joined-rooms", false, "create and delete only a fresh disposable room using joined nodes")

func TestJoinedRoomLifecycle(t *testing.T) {
	if testing.Short() || !*joinedRooms {
		t.Skip("requires -joined-rooms, a fresh disposable room and its Cloud grants")
	}
	participants, err := smokeParticipants("", [2]string{
		os.Getenv("DTELECOM_SMOKE_TOKEN_A_FILE"), os.Getenv("DTELECOM_SMOKE_TOKEN_B_FILE"),
	})
	require.NoError(t, err)
	token, err := readSmokeAdminToken(os.Getenv("DTELECOM_SMOKE_ADMIN_TOKEN_FILE"), participants[0])
	require.NoError(t, err)
	v, err := auth.ParseAPIToken(token)
	require.NoError(t, err)
	grants, err := v.Verify(participants[0].issuer)
	require.NoError(t, err)
	require.True(t, grants.Video.RoomCreate, "Cloud admin JWT must carry roomCreate")
	ctx := contextWithToken(token)
	room := participants[0].room
	a, err := testutils.SignalingOrigin(os.Getenv("DTELECOM_SMOKE_NODE_A"))
	require.NoError(t, err)
	b, err := testutils.SignalingOrigin(os.Getenv("DTELECOM_SMOKE_NODE_B"))
	require.NoError(t, err)
	require.NotEqual(t, a, b)
	nodeA, nodeB := os.Getenv("DTELECOM_SMOKE_EXPECT_NODE_A"), os.Getenv("DTELECOM_SMOKE_EXPECT_NODE_B")
	for _, topology := range []struct{ name, endpoint, expected string }{{"single", a, nodeA}, {"mesh", b, nodeB}} {
		t.Run(topology.name, func(t *testing.T) {
			api := joinedRoomService(a)
			for _, endpoint := range []string{a, b} {
				before, err := joinedRoomService(endpoint).ListParticipants(ctx, &livekit.ListParticipantsRequest{Room: room})
				require.NoError(t, err)
				require.Empty(t, before.GetParticipants(), "supply a fresh disposable room; participants already present")
			}
			// The operator has issued fresh tokens for this dedicated room. Capture its SID from the join.
			first := joinLifecyclePeer(t, a, nodeA, participants[0])
			waitUntilConnected(t, first.client)
			require.NotEmpty(t, first.roomSID)

			second := joinLifecyclePeer(t, topology.endpoint, topology.expected, participants[1])
			waitUntilConnected(t, second.client)
			if topology.name == "mesh" {
				require.NotEqual(t, first.node, second.node)
			} else {
				require.Equal(t, first.node, second.node)
			}
			require.Eventually(t, func() bool {
				return second.client.GetRemoteParticipant(first.client.ID()).GetIdentity() == participants[0].identity
			}, testutils.ConnectTimeout, 50*time.Millisecond, "room membership did not converge")

			_, err = api.DeleteRoom(ctx, &livekit.DeleteRoomRequest{Room: room})
			require.NoError(t, err)
			first.expectLeave(t, livekit.DisconnectReason_ROOM_DELETED)
			second.expectLeave(t, livekit.DisconnectReason_ROOM_DELETED)
			// The server acknowledges deletion only after removing the room from its local store.
			// Rejoining the same name must create a room with a fresh SID without a CreateRoom RPC.
			fresh := joinLifecyclePeer(t, a, nodeA, participants[0])
			waitUntilConnected(t, fresh.client)
			require.NotEmpty(t, fresh.roomSID)
			require.NotEqual(t, first.roomSID, fresh.roomSID)
			fresh.client.Stop()

			// The room persists for a short departure grace period after its last client leaves.
			departedAt := time.Now()
			var lastListed []string
			require.Eventually(t, func() bool {
				res, err := api.ListParticipants(ctx, &livekit.ListParticipantsRequest{Room: room})
				if err != nil {
					return false
				}
				if peers := res.GetParticipants(); len(peers) != 0 {
					lastListed = lastListed[:0]
					for _, p := range peers {
						lastListed = append(lastListed, fmt.Sprintf("%s/%s/%s", p.GetIdentity(), p.GetSid(), p.GetState()))
					}
					return false
				}
				return true
			}, testutils.ConnectTimeout, 50*time.Millisecond)
			updated, err := api.UpdateRoomMetadata(ctx, &livekit.UpdateRoomMetadataRequest{
				Room: room, Metadata: "empty-room-" + fresh.roomSID,
			})
			require.NoError(t, err)
			require.Equal(t, fresh.roomSID, updated.GetSid(), "empty room was gone after %s waiting for an empty list; last listed %v, previous room local %s/remote %s, fresh local %s", time.Since(departedAt), lastListed, first.client.ID(), second.client.ID(), fresh.client.ID())
			require.Equal(t, "empty-room-"+fresh.roomSID, updated.GetMetadata())
			_, err = api.DeleteRoom(ctx, &livekit.DeleteRoomRequest{Room: room})
			require.NoError(t, err)
		})
	}
}
