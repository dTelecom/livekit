package rtc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/webhook"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/rtc/relay"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/rtc/types/typesfakes"
	"github.com/livekit/livekit-server/pkg/telemetry"
	"github.com/livekit/livekit-server/pkg/telemetry/prometheus"
	"github.com/livekit/livekit-server/pkg/telemetry/telemetryfakes"
	lkutils "github.com/livekit/livekit-server/pkg/utils"
)

var prometheusOnce sync.Once

func initPrometheusForTest() {
	prometheusOnce.Do(func() { prometheus.Init("test", livekit.NodeType_SERVER, "test") })
}

func setRTTForTest(p *ParticipantImpl, rtt uint32) {
	p.lock.Lock()
	p.rttUpdatedAt = time.Time{} // past the update interval
	p.lock.Unlock()
	p.UpdateMediaRTT(rtt)
}

func TestToProtoCarriesLastRTT(t *testing.T) {
	p := newParticipantForTest("test")
	require.Zero(t, p.ToProto().RttMs)

	setRTTForTest(p, 42)
	require.Equal(t, uint32(42), p.ToProto().RttMs)

	p.UpdateMediaRTT(99) // within the update interval: keeps the last accepted sample
	require.Equal(t, uint32(42), p.ToProto().RttMs)
}

func TestToProtoRTTIsSafeWithUpdates(t *testing.T) {
	p := newParticipantForTest("test")
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := uint32(1); i < 200; i++ {
			setRTTForTest(p, i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = p.ToProto()
		}
	}()
	wg.Wait()
}

// addTrackForTest publishes a real media track the way the participant does, from a pending track.
func addTrackForTest(p *ParticipantImpl, cid string, ti *livekit.TrackInfo) *MediaTrack {
	p.pendingTracksLock.Lock()
	p.pendingTracks[cid] = &pendingTrackInfo{trackInfos: []*livekit.TrackInfo{ti}}
	p.pendingTracksLock.Unlock()
	return p.addMediaTrack(cid, cid, ti)
}

func grantsForTest(legacy string, urls ...string) *auth.ClaimGrants {
	return &auth.ClaimGrants{Video: &auth.VideoGrant{}, WebHookURL: legacy, WebHookURLs: urls}
}

func TestTrackPublishedCallSite(t *testing.T) {
	tests := []struct {
		name   string
		grants *auth.ClaimGrants
		want   []string
	}{
		{"backend token", grantsForTest("https://in", "https://in", "https://c"), []string{"https://in", "https://c"}},
		{"list only", grantsForTest("", "https://c"), []string{"https://c"}},
		{"legacy only gets none", grantsForTest("https://legacy"), nil},
		{"no urls", grantsForTest(""), nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newParticipantForTest("test")
			p.grants = tt.grants
			fake := p.params.Telemetry.(*telemetryfakes.FakeTelemetryService)
			track := &typesfakes.FakeMediaTrack{}
			track.ToProtoReturns(&livekit.TrackInfo{Sid: "TR_a"})

			p.UpTrackManager.AddPublishedTrack(track)
			p.handleTrackPublished(track)

			require.Equal(t, 1, fake.TrackPublishedCallCount())
			_, _, _, _, send, urls := fake.TrackPublishedArgsForCall(0)
			require.True(t, send)
			require.Equal(t, tt.want, urls)
		})
	}
}

func TestTrackUnpublishedCallSite(t *testing.T) {
	tests := []struct {
		name   string
		grants *auth.ClaimGrants
		want   []string
	}{
		{"backend token", grantsForTest("https://in", "https://in", "https://c"), []string{"https://in", "https://c"}},
		{"legacy only gets none", grantsForTest("https://legacy"), nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newParticipantForTest("test")
			p.grants = tt.grants
			fake := p.params.Telemetry.(*telemetryfakes.FakeTelemetryService)
			mt := addTrackForTest(p, "cid", &livekit.TrackInfo{Sid: "TR_a", Type: livekit.TrackType_VIDEO})

			mt.Close(false)

			require.Equal(t, 1, fake.TrackUnpublishedCallCount())
			_, _, _, _, send, urls := fake.TrackUnpublishedArgsForCall(0)
			require.True(t, send)
			require.Equal(t, tt.want, urls)
		})
	}
}

// A relayed copy has no URL claims and passes shouldSendEvent false: only the home node fires.
func TestRelayedParticipantFiresNothing(t *testing.T) {
	fake := &telemetryfakes.FakeTelemetryService{}
	p, err := NewRelayedParticipant(RelayedParticipantParams{
		Identity:  "alice",
		SID:       "PA_a",
		Telemetry: fake,
		Logger:    logger.GetLogger(),
	})
	require.NoError(t, err)
	require.Nil(t, p.ClaimGrants().AllWebHookURLs())

	track := &typesfakes.FakeMediaTrack{}
	track.ToProtoReturns(&livekit.TrackInfo{Sid: "TR_a"})
	p.handleTrackPublished(track)

	require.Equal(t, 1, fake.TrackPublishedCallCount())
	_, _, _, _, send, urls := fake.TrackPublishedArgsForCall(0)
	require.False(t, send)
	require.Nil(t, urls)
}

// The room's state callback is the participant_joined call site.
func TestParticipantActiveCallSite(t *testing.T) {
	tests := []struct {
		name   string
		grants *auth.ClaimGrants
		want   []string
	}{
		{"backend token", grantsForTest("https://in", "https://in", "https://c"), []string{"https://in", "https://c"}},
		{"legacy only", grantsForTest("https://legacy"), []string{"https://legacy"}},
		{"no urls", grantsForTest(""), nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &telemetryfakes.FakeTelemetryService{}
			rm := NewRoom(&livekit.Room{Name: "room"}, nil, WebRTCConfig{}, relay.WebRTCRelayConfig{},
				&config.AudioConfig{UpdateInterval: audioUpdateInterval},
				&livekit.ServerInfo{Protocol: types.CurrentProtocol}, fake, nil)
			defer rm.Close()
			p := newMockParticipant("alice", types.CurrentProtocol, false, true)
			p.ClaimGrantsReturns(tt.grants)
			require.NoError(t, rm.Join(p, nil, &ParticipantOptions{AutoSubscribe: false}, iceServersForRoom))

			p.StateReturns(livekit.ParticipantInfo_ACTIVE)
			p.OnStateChangeArgsForCall(p.OnStateChangeCallCount()-1)(p, livekit.ParticipantInfo_JOINED)

			require.Equal(t, 1, fake.ParticipantActiveCallCount())
			_, _, _, _, urls := fake.ParticipantActiveArgsForCall(0)
			require.Equal(t, tt.want, urls)
		})
	}
}

type wirePost struct {
	Path          string `json:"path"`
	Authorization string `json:"authorization"`
	ContentType   string `json:"contentType"`
	Body          string `json:"body"`
}

type wireReceiver struct {
	lock  sync.Mutex
	posts []wirePost
}

func (w *wireReceiver) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	w.lock.Lock()
	defer w.lock.Unlock()
	w.posts = append(w.posts, wirePost{r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("Content-Type"), string(body)})
}

func (w *wireReceiver) all() []wirePost {
	w.lock.Lock()
	defer w.lock.Unlock()
	return append([]wirePost(nil), w.posts...)
}

type wireRun struct {
	receiver *wireReceiver
	nodeKey  string
	project  string
}

// runWireSession drives one call through the node's real code: a real participant and media tracks,
// the real telemetry service, and the real notifier signing with a Solana key. Only the room manager's
// two participant event calls are repeated here, with the same arguments.
func runWireSession(t *testing.T, legacy string, paths ...string) *wireRun {
	initPrometheusForTest()
	receiver := &wireReceiver{}
	server := httptest.NewServer(receiver)
	t.Cleanup(server.Close)

	nodeKey, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	projectKey, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	service := telemetry.NewTelemetryService(webhook.NewNotifier(nodeKey.PublicKey().String(), nodeKey.String()), &telemetryfakes.FakeAnalyticsService{})

	p := newParticipantForTest("alice")
	p.params.Telemetry = service
	grants := grantsForTest("")
	if legacy != "" {
		grants.WebHookURL = server.URL + legacy
	}
	for _, path := range paths {
		grants.WebHookURLs = append(grants.WebHookURLs, server.URL+path)
	}
	p.grants = grants

	room := &livekit.Room{
		Sid:          "RM_wire",
		Name:         "wire-room",
		Key:          string(lkutils.RoomKey("wire-room", livekit.ApiKey(projectKey.PublicKey().String()))),
		CreationTime: time.Now().Unix(),
	}
	ctx := context.Background()
	service.ParticipantJoined(ctx, room, p.ToProto(), nil, &livekit.AnalyticsClientMeta{}, true)
	service.ParticipantActive(ctx, room, p.ToProto(), &livekit.AnalyticsClientMeta{}, p.ClaimGrants().AllWebHookURLs())

	video := addTrackForTest(p, "cid-v", &livekit.TrackInfo{
		Sid: "TR_video", Type: livekit.TrackType_VIDEO, Source: livekit.TrackSource_CAMERA, Width: 1920, Height: 1080,
		Layers: []*livekit.VideoLayer{{Quality: livekit.VideoQuality_HIGH, Width: 1920, Height: 1080}},
	})
	audio := addTrackForTest(p, "cid-a", &livekit.TrackInfo{
		Sid: "TR_audio", Type: livekit.TrackType_AUDIO, Source: livekit.TrackSource_MICROPHONE,
	})
	p.handleTrackPublished(video)
	p.handleTrackPublished(audio)
	video.Close(false)
	audio.Close(false)

	setRTTForTest(p, 42)
	service.ParticipantLeft(ctx, room, p.ToProto(), true, p.ClaimGrants().AllWebHookURLs())

	// the last event queued is participant_left; the pool delivers concurrently, so allow for the rest
	time.Sleep(300 * time.Millisecond)
	return &wireRun{receiver: receiver, nodeKey: nodeKey.PublicKey().String(), project: projectKey.PublicKey().String()}
}

func decodeBody(t *testing.T, post wirePost) map[string]any {
	t.Helper()
	var body map[string]any
	require.NoError(t, json.Unmarshal([]byte(post.Body), &body))
	return body
}

// TestWebhookWireFormat checks what a receiver gets. With WEBHOOK_WIRE_OUT set, it also saves each
// POST so a receiver's own parser can be run against real output.
func TestWebhookWireFormat(t *testing.T) {
	run := runWireSession(t, "/ingest", "/ingest", "/customer")
	posts := run.receiver.all()
	require.Len(t, posts, 12, "6 events to 2 URLs: the ingest URL in both claims is posted to once")

	idsByPath := map[string]map[string]string{}
	for _, post := range posts {
		body := decodeBody(t, post)
		event := body["event"].(string)
		if track, ok := body["track"].(map[string]any); ok {
			event += "/" + track["sid"].(string)
		}
		if idsByPath[post.Path] == nil {
			idsByPath[post.Path] = map[string]string{}
		}
		_, duplicate := idsByPath[post.Path][event]
		require.False(t, duplicate, "%s delivered twice to %s", event, post.Path)
		idsByPath[post.Path][event] = body["id"].(string)

		// headers and signature
		require.Equal(t, "application/webhook+json", post.ContentType)
		verifier, err := auth.ParseAPIToken(post.Authorization) // no Bearer prefix
		require.NoError(t, err)
		require.Equal(t, run.nodeKey, verifier.APIKey())
		claims, err := verifier.Verify(run.nodeKey)
		require.NoError(t, err)
		sum := sha256.Sum256([]byte(post.Body))
		require.Equal(t, base64.StdEncoding.EncodeToString(sum[:]), claims.Sha256)

		// body
		require.Regexp(t, `^EV_`, body["id"])
		require.IsType(t, "", body["createdAt"], "int64 as a string")
		room := body["room"].(map[string]any)
		require.Equal(t, "wire-room", room["name"])
		require.Equal(t, "RM_wire", room["sid"])
		name, apiKey, err := lkutils.ParseRoomKey(livekit.RoomKey(room["key"].(string)))
		require.NoError(t, err)
		require.Equal(t, livekit.RoomName("wire-room"), name)
		require.Equal(t, livekit.ApiKey(run.project), apiKey)
		participant := body["participant"].(map[string]any)
		require.Equal(t, "alice", participant["identity"])
		require.NotEmpty(t, participant["sid"])

		switch body["event"] {
		case webhook.EventParticipantJoined:
			require.IsType(t, "", participant["joinedAt"], "int64 as a string")
			require.NotContains(t, participant, "rttMs", "zero is omitted")
		case webhook.EventParticipantLeft:
			require.IsType(t, "", participant["joinedAt"], "int64 as a string")
			require.EqualValues(t, 42, participant["rttMs"])
		default:
			track := body["track"].(map[string]any)
			require.NotContains(t, participant, "joinedAt")
			if track["sid"] == "TR_video" {
				require.Equal(t, "VIDEO", track["type"])
				require.Equal(t, "CAMERA", track["source"])
				require.EqualValues(t, 1920, track["width"])
				require.EqualValues(t, 1080, track["height"])
			} else {
				require.NotContains(t, track, "type", "AUDIO is the zero value")
				require.Equal(t, "MICROPHONE", track["source"])
			}
		}
	}
	require.Equal(t, idsByPath["/ingest"], idsByPath["/customer"], "one id per event across URLs")
	require.Len(t, idsByPath["/ingest"], 6)

	out := os.Getenv("WEBHOOK_WIRE_OUT")
	if out == "" {
		return
	}
	require.NoError(t, os.MkdirAll(out, 0o755))
	encoded, err := json.MarshalIndent(map[string]any{"nodeKey": run.nodeKey, "project": run.project, "posts": posts}, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(out, "posts.json"), encoded, 0o644))
}

// A token with only the legacy claim gets exactly today's two POSTs.
func TestWebhookLegacyTokenFlowUnchanged(t *testing.T) {
	run := runWireSession(t, "/legacy")
	posts := run.receiver.all()
	require.Len(t, posts, 2)
	require.Equal(t, "/legacy", posts[0].Path)
	require.Equal(t, "/legacy", posts[1].Path)
	var events []any
	for _, post := range posts {
		events = append(events, decodeBody(t, post)["event"])
	}
	require.ElementsMatch(t, []any{webhook.EventParticipantJoined, webhook.EventParticipantLeft}, events)
}

func TestWebhookNoURLsNoPosts(t *testing.T) {
	require.Empty(t, runWireSession(t, "").receiver.all())
}
