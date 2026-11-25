package test

import (
	"flag"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/testutils"
	testclient "github.com/livekit/livekit-server/test/client"
	"github.com/livekit/protocol/livekit"
)

var joinedLegacy = flag.Bool("joined-legacy", false, "run adapted legacy scenarios on two existing test nodes")

// TestJoinedLegacyScenarios uses the same dedicated-room credentials and node origins
// as TestJoinedNodeSmoke. It runs the single-node control first and then the cross-node case.
// Supply DTELECOM_SMOKE_ADMIN_TOKEN_FILE to include moderation of the fixture's own track.
//
//	go test -count=1 -run '^TestJoinedLegacyScenarios$' ./test -args -joined-legacy
func TestJoinedLegacyScenarios(t *testing.T) {
	if testing.Short() || !*joinedLegacy {
		t.Skip("requires -joined-legacy and a dedicated joined-node fixture")
	}

	participants, err := smokeParticipants(os.Getenv("DTELECOM_SMOKE_ISSUER_KEY_FILE"), [2]string{
		os.Getenv("DTELECOM_SMOKE_TOKEN_A_FILE"), os.Getenv("DTELECOM_SMOKE_TOKEN_B_FILE"),
	})
	require.NoError(t, err)
	a, err := testutils.SignalingOrigin(os.Getenv("DTELECOM_SMOKE_NODE_A"))
	require.NoError(t, err)
	b, err := testutils.SignalingOrigin(os.Getenv("DTELECOM_SMOKE_NODE_B"))
	require.NoError(t, err)
	require.NotEqual(t, a, b)
	var adminToken string
	if path := os.Getenv("DTELECOM_SMOKE_ADMIN_TOKEN_FILE"); path != "" {
		adminToken, err = readSmokeAdminToken(path, participants[0])
		require.NoError(t, err)
	}

	for _, topology := range []struct{ name, subscriberEndpoint, expectedSubscriberNode string }{
		{"single", a, os.Getenv("DTELECOM_SMOKE_EXPECT_NODE_A")},
		{"mesh", b, os.Getenv("DTELECOM_SMOKE_EXPECT_NODE_B")},
	} {
		t.Run(topology.name, func(t *testing.T) {
			join := func(endpoint string, participant smokeParticipant, expected string, autoSubscribe bool) (*testclient.RTCClient, string) {
				t.Helper()
				conn, response, err := dialJoinedNodeWithSubscription(endpoint, participant, autoSubscribe)
				require.NoError(t, err)
				t.Cleanup(func() { _ = conn.Close() })
				nodeID := response.GetServerInfo().GetNodeId()
				if expected != "" {
					require.Equal(t, expected, nodeID)
				}
				c, err := testclient.NewRTCClientFromJoin(conn, response)
				require.NoError(t, err)
				t.Cleanup(c.Stop)
				go c.Run()
				return c, nodeID
			}
			publisher, firstNode := join(a, participants[0], os.Getenv("DTELECOM_SMOKE_EXPECT_NODE_A"), true)
			subscriber, secondNode := join(topology.subscriberEndpoint, participants[1], topology.expectedSubscriberNode, false)
			if topology.name == "single" {
				require.Equal(t, firstNode, secondNode)
			} else {
				require.NotEqual(t, firstNode, secondNode)
			}
			t.Run("ping", func(t *testing.T) {
				for _, c := range []*testclient.RTCClient{publisher, subscriber} {
					require.NoError(t, c.SendPing())
					require.Eventually(t, func() bool { return c.PongReceivedAt() > 0 }, 5*time.Second, 20*time.Millisecond)
				}
			})

			waitUntilConnected(t, publisher, subscriber)
			writer, err := publisher.AddStaticTrack("audio/opus", "legacy-opus", "microphone")
			require.NoError(t, err)
			t.Cleanup(writer.Stop)
			tracks := publisher.GetPublishedTrackIDs()
			require.Len(t, tracks, 1)
			trackSID := tracks[0]

			t.Run("manual subscription", func(t *testing.T) {
				require.Eventually(t, func() bool {
					peer := subscriber.GetRemoteParticipant(publisher.ID())
					for _, track := range peer.GetTracks() {
						if track.GetSid() == trackSID {
							return true
						}
					}
					return false
				}, testutils.ConnectTimeout, 50*time.Millisecond, "published track was not announced")
				require.Never(t, func() bool {
					return subscriber.BytesReceivedFrom(publisher.ID()) != 0
				}, time.Second, 20*time.Millisecond, "received RTP while auto-subscription was disabled")
				err := subscriber.SendRequest(&livekit.SignalRequest{Message: &livekit.SignalRequest_Subscription{
					Subscription: &livekit.UpdateSubscription{TrackSids: []string{trackSID}, Subscribe: true},
				}})
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					return subscriber.BytesReceivedFrom(publisher.ID()) >= 256
				}, testutils.ConnectTimeout, 50*time.Millisecond, "RTP did not arrive after explicit subscription")
			})

			if adminToken != "" {
				t.Run("mute named test track", func(t *testing.T) {
					// The synthetic publisher and its track are removed when this test disconnects.
					api := joinedRoomService(a)
					ctx := contextWithToken(adminToken)
					target := &livekit.RoomParticipantIdentity{Room: participants[0].room, Identity: participants[0].identity}
					peer, err := api.GetParticipant(ctx, target)
					require.NoError(t, err)
					require.Equal(t, string(publisher.ID()), peer.GetSid())
					res, err := api.MutePublishedTrack(ctx, &livekit.MuteRoomTrackRequest{
						Room: target.Room, Identity: target.Identity, TrackSid: trackSID, Muted: true,
					})
					require.NoError(t, err)
					require.Equal(t, trackSID, res.GetTrack().GetSid())
					require.True(t, res.GetTrack().GetMuted())
				})
			}
		})
	}
}
