package rtc

import (
	"testing"
	"time"

	"github.com/pion/webrtc/v3"
	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/rtc/types/typesfakes"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
)

func TestSubscriberCloseRemovesOnlyItsPublisherDownTrack(t *testing.T) {
	track := &typesfakes.FakeMediaTrack{}
	track.IDReturns("TR_audio")
	track.PublisherIDReturns("PA_publisher")
	track.ToProtoReturns(&livekit.TrackInfo{Sid: string(track.ID()), Type: livekit.TrackType_AUDIO})
	subscriptions := NewMediaTrackSubscriptions(MediaTrackSubscriptionsParams{MediaTrack: track, Logger: logger.GetLogger()})
	receiver := NewWrappedReceiver(WrappedReceiverParams{
		TrackID: track.ID(), StreamId: string(track.PublisherID()), DisableRed: true,
		UpstreamCodecs: []webrtc.RTPCodecParameters{{RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2,
		}, PayloadType: 111}},
	})
	track.AddSubscriberStub = func(sub types.LocalParticipant) (types.SubscribedTrack, error) {
		return subscriptions.AddSubscriber(sub, receiver)
	}

	subscribe := func(id livekit.ParticipantID) *SubscriptionManager {
		t.Helper()
		pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
		require.NoError(t, err)
		t.Cleanup(func() { _ = pc.Close() })
		manager := newTestSubscriptionManager(t)
		t.Cleanup(func() { manager.Close(false) })
		sub := manager.params.Participant.(*typesfakes.FakeLocalParticipant)
		sub.IDReturns(id)
		sub.GetLoggerReturns(logger.GetLogger())
		sub.AddTransceiverFromTrackToSubscriberStub = func(track webrtc.TrackLocal, _ types.AddTrackParams) (*webrtc.RTPSender, *webrtc.RTPTransceiver, error) {
			tr, err := pc.AddTransceiverFromTrack(track)
			if err != nil {
				return nil, nil, err
			}
			return tr.Sender(), tr, nil
		}
		manager.params.TrackResolver = func(livekit.ParticipantIdentity, livekit.TrackID) types.MediaResolverResult {
			return types.MediaResolverResult{Track: track, HasPermission: true, PublisherID: track.PublisherID()}
		}
		manager.SubscribeToTrack(track.ID())
		require.Eventually(t, func() bool {
			return manager.IsSubscribedTo(track.PublisherID()) && subscriptions.IsSubscriber(id)
		}, time.Second, time.Millisecond)
		return manager
	}
	departing := subscribe("PA_departing")
	subscribe("PA_staying")
	departing.Close(false)
	require.Eventually(t, func() bool { return !subscriptions.IsSubscriber("PA_departing") }, time.Second, time.Millisecond)
	require.True(t, subscriptions.IsSubscriber("PA_staying"))
}
