package test

import (
	"flag"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pion/webrtc/v3"
	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/testutils"
	testclient "github.com/livekit/livekit-server/test/client"
)

var joinedMedia = flag.Bool("joined-media", false, "exercise additional media negotiation on joined nodes in a dedicated room")

func TestJoinedMediaScenarios(t *testing.T) {
	if testing.Short() || !*joinedMedia {
		t.Skip("requires -joined-media and dedicated participant grants")
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
	nodeA, nodeB := os.Getenv("DTELECOM_SMOKE_EXPECT_NODE_A"), os.Getenv("DTELECOM_SMOKE_EXPECT_NODE_B")
	for _, topology := range []struct{ name, endpoint, expected string }{{"single", a, nodeA}, {"mesh", b, nodeB}} {
		t.Run(topology.name, func(t *testing.T) {
			t.Run("CORS", func(t *testing.T) {
				origin := "https://joined-client.example"
				req, err := http.NewRequest(http.MethodPost, "http"+strings.TrimPrefix(topology.endpoint, "ws"), nil)
				require.NoError(t, err)
				req.Header.Set("Origin", origin)
				res, err := (&http.Client{Timeout: testutils.ConnectTimeout}).Do(req)
				require.NoError(t, err)
				defer res.Body.Close()
				require.Equal(t, origin, res.Header.Get("Access-Control-Allow-Origin"))
			})

			publisher := joinLifecyclePeer(t, a, nodeA, participants[0])
			waitUntilConnected(t, publisher.client)
			publishJoinedTracks(t, publisher.client)

			// A late subscriber receives both pre-existing tracks and their advertised codecs.
			observer := joinLifecyclePeer(t, topology.endpoint, topology.expected, participants[1])
			if topology.name == "mesh" {
				require.NotEqual(t, publisher.node, observer.node)
			} else {
				require.Equal(t, publisher.node, observer.node)
			}
			waitUntilConnected(t, observer.client)
			expectJoinedTracks(t, observer.client, publisher.client, "audio/opus", "video/vp8")

			// Add a different video codec after both clients completed their first negotiation.
			screen, err := publisher.client.AddStaticTrackWithCodec(webrtc.RTPCodecCapability{
				MimeType: webrtc.MimeTypeH264, ClockRate: 90000,
				SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
			}, "screen-h264", "screen")
			require.NoError(t, err)
			t.Cleanup(screen.Stop)
			expectJoinedTracks(t, observer.client, publisher.client, "audio/opus", "video/vp8", "video/h264")

			// The receiver can start publishing once it has received the other publisher's tracks.
			publishJoinedTracks(t, observer.client)
			expectJoinedTracks(t, publisher.client, observer.client, "audio/opus", "video/vp8")

			// A secondary publishing connection shares the original JWT and gets its own participant SID.
			const suffix = "secondary"
			conn, err := testclient.NewWebSocketConn(a, participants[0].token, &testclient.Options{Publish: suffix})
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })
			response, err := readJoinedNodeResponse(conn, participants[0].room, participants[0].identity+"#"+suffix)
			require.NoError(t, err)
			require.Equal(t, publisher.node, response.GetServerInfo().GetNodeId())
			secondary, err := testclient.NewRTCClientFromJoin(conn, response)
			require.NoError(t, err)
			t.Cleanup(secondary.Stop)
			go secondary.Run()
			waitUntilConnected(t, secondary)
			require.NotEqual(t, publisher.client.ID(), secondary.ID())
			camera, err := secondary.AddStaticTrack("video/vp8", "secondary-camera", "camera")
			require.NoError(t, err)
			t.Cleanup(camera.Stop)
			expectJoinedTracks(t, observer.client, secondary, "video/vp8")
			expectJoinedTracks(t, observer.client, publisher.client, "audio/opus", "video/vp8", "video/h264")
		})
	}
}

func expectJoinedTracks(t *testing.T, receiver, publisher *testclient.RTCClient, codecs ...string) {
	t.Helper()
	want := make(map[string]bool, len(codecs))
	for _, codec := range codecs {
		want[codec] = true
	}
	require.Eventually(t, func() bool {
		tracks := receiver.SubscribedTracks()[publisher.ID()]
		if len(tracks) != len(want) || receiver.BytesReceivedFrom(publisher.ID()) == 0 {
			return false
		}
		got := make(map[string]bool, len(tracks))
		for _, track := range tracks {
			id, _ := rtc.UnpackStreamID(track.StreamID())
			mime := strings.ToLower(track.Codec().MimeType)
			if id != publisher.ID() || !want[mime] || !strings.HasPrefix(track.ID(), "TR_") {
				return false
			}
			got[mime] = true
		}
		return len(got) == len(want)
	}, testutils.ConnectTimeout, 50*time.Millisecond, "receiver %s did not receive %v from %s", receiver.ID(), codecs, publisher.ID())
}
