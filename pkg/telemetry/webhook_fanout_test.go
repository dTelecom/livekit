package telemetry_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/telemetry"
	"github.com/livekit/livekit-server/pkg/telemetry/telemetryfakes"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/webhook"
)

type delivery struct {
	event *livekit.WebhookEvent
	url   string
}

type captureNotifier struct {
	lock       sync.Mutex
	deliveries []delivery
}

func (n *captureNotifier) Notify(_ context.Context, payload interface{}, url string) error {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.deliveries = append(n.deliveries, delivery{payload.(*livekit.WebhookEvent), url})
	return nil
}

func (n *captureNotifier) all() []delivery {
	n.lock.Lock()
	defer n.lock.Unlock()
	return append([]delivery(nil), n.deliveries...)
}

func (n *captureNotifier) waitFor(t *testing.T, count int) []delivery {
	t.Helper()
	require.Eventually(t, func() bool { return len(n.all()) >= count }, 2*time.Second, 5*time.Millisecond)
	return n.all()
}

func newFanoutService() (telemetry.TelemetryService, *captureNotifier) {
	notifier := &captureNotifier{}
	return telemetry.NewTelemetryService(notifier, &telemetryfakes.FakeAnalyticsService{}), notifier
}

func TestNotifyEventFansOutWithOneID(t *testing.T) {
	sut, notifier := newFanoutService()
	urls := []string{"https://a", "https://b", "https://c"}

	sut.NotifyEvent(context.Background(), &livekit.WebhookEvent{Event: webhook.EventParticipantJoined}, urls)

	got := notifier.waitFor(t, 3)
	require.Len(t, got, 3)
	var delivered []string
	for _, d := range got {
		delivered = append(delivered, d.url)
		require.Equal(t, got[0].event.Id, d.event.Id)
		require.NotZero(t, d.event.CreatedAt)
	}
	require.NotEmpty(t, got[0].event.Id)
	require.ElementsMatch(t, urls, delivered)
}

func TestNotifyEventWithoutURLsDeliversNothing(t *testing.T) {
	for name, urls := range map[string][]string{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			sut, notifier := newFanoutService()
			event := &livekit.WebhookEvent{Event: webhook.EventParticipantJoined}

			sut.NotifyEvent(context.Background(), event, urls)

			time.Sleep(50 * time.Millisecond)
			require.Empty(t, notifier.all())
			require.Empty(t, event.Id)
		})
	}
}

func TestParticipantEventsDeliverToURLs(t *testing.T) {
	sut, notifier := newFanoutService()
	ctx := context.Background()
	room := &livekit.Room{Sid: "RM_a", Name: "room", Key: "name|key"}
	participant := &livekit.ParticipantInfo{Sid: "PA_a", Identity: "alice"}
	urls := []string{"https://a", "https://b"}

	sut.ParticipantActive(ctx, room, participant, &livekit.AnalyticsClientMeta{}, urls)
	got := notifier.waitFor(t, 2)
	require.Equal(t, webhook.EventParticipantJoined, got[0].event.Event)

	sut.ParticipantLeft(ctx, room, participant, true, urls)
	got = notifier.waitFor(t, 4)
	require.Len(t, got, 4)
	require.Equal(t, webhook.EventParticipantLeft, got[3].event.Event)
	require.NotEqual(t, got[0].event.Id, got[3].event.Id)
}

func TestTrackEventsDeliverToURLsWithRoomKey(t *testing.T) {
	sut, notifier := newFanoutService()
	ctx := context.Background()
	room := &livekit.Room{Sid: "RM_a", Name: "room", Key: "name|key"}
	sut.ParticipantJoined(ctx, room, &livekit.ParticipantInfo{Sid: "PA_a", Identity: "alice"}, nil, &livekit.AnalyticsClientMeta{}, true)
	track := &livekit.TrackInfo{Sid: "TR_a", Type: livekit.TrackType_VIDEO}
	urls := []string{"https://a", "https://b"}

	sut.TrackPublished(ctx, "PA_a", "alice", track, true, urls)
	got := notifier.waitFor(t, 2)
	require.Equal(t, webhook.EventTrackPublished, got[0].event.Event)
	require.Equal(t, got[0].event.Id, got[1].event.Id)
	require.Equal(t, "name|key", got[0].event.Room.Key)
	require.Equal(t, "room", got[0].event.Room.Name)
	require.Equal(t, "RM_a", got[0].event.Room.Sid)
	require.Equal(t, "alice", got[0].event.Participant.Identity)
	require.Equal(t, "PA_a", got[0].event.Participant.Sid)

	sut.TrackUnpublished(ctx, "PA_a", "alice", track, true, urls)
	got = notifier.waitFor(t, 4)
	require.Equal(t, webhook.EventTrackUnpublished, got[2].event.Event)
	require.Equal(t, "name|key", got[2].event.Room.Key)
}

func TestTrackEventsWithheld(t *testing.T) {
	sut, notifier := newFanoutService()
	ctx := context.Background()
	room := &livekit.Room{Sid: "RM_a", Name: "room", Key: "name|key"}
	sut.ParticipantJoined(ctx, room, &livekit.ParticipantInfo{Sid: "PA_a"}, nil, &livekit.AnalyticsClientMeta{}, true)
	track := &livekit.TrackInfo{Sid: "TR_a"}

	sut.TrackPublished(ctx, "PA_a", "alice", track, false, []string{"https://a"}) // shouldSendEvent false
	sut.TrackUnpublished(ctx, "PA_a", "alice", track, false, []string{"https://a"})
	sut.TrackPublished(ctx, "PA_a", "alice", track, true, nil) // no URLs
	sut.TrackUnpublished(ctx, "PA_a", "alice", track, true, nil)

	time.Sleep(50 * time.Millisecond)
	require.Empty(t, notifier.all())
}

func TestRoomAndEgressEventsStayUndelivered(t *testing.T) {
	sut, notifier := newFanoutService()
	ctx := context.Background()
	room := &livekit.Room{Sid: "RM_a", Name: "room"}
	egress := &livekit.EgressInfo{EgressId: "EG_a"}

	sut.RoomStarted(ctx, room)
	sut.RoomEnded(ctx, room)
	sut.EgressStarted(ctx, egress)
	sut.EgressUpdated(ctx, egress)
	sut.EgressEnded(ctx, egress)

	time.Sleep(50 * time.Millisecond)
	require.Empty(t, notifier.all())
}
