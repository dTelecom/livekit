package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	lksdk "github.com/dtelecom/server-sdk-go"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v3"
	"github.com/pion/webrtc/v3/pkg/media"
)

type TrackSampleProvider struct {
	samples          chan *rtp.Packet
	done             chan struct{}
	isVideo          bool
}

func NewTrackSampleProvider(isVideo bool) *TrackSampleProvider {
	return  &TrackSampleProvider{
		samples: make(chan *rtp.Packet, 200),
		done:    make(chan struct{}),
		isVideo: isVideo,
	}
}

func (t *TrackSampleProvider) PushSample(pkt *rtp.Packet) {
	select {
	case t.samples <- pkt:
	case <-t.done:
		return
	default:
		if t.isVideo {
			logger.Infow("Warning: Dropped video packet due to full buffer", "timestamp", pkt.Timestamp)
		}
	}
}

func (t *TrackSampleProvider) Close() {
	close(t.done)
	close(t.samples)
}

type LiveKitSDKPublisher struct {
	room                *lksdk.Room
	videoTrackPub       *lksdk.LocalTrackPublication
	audioTrackPub       *lksdk.LocalTrackPublication
	videoSampleProvider *TrackSampleProvider
	audioSampleProvider *TrackSampleProvider
	ctx                 context.Context
	cancel              context.CancelFunc
	userDisconnectCh    chan struct{}
	disconnectOnce      sync.Once
}

func StartLiveKitSDKPublisher(wsURL string, token string, user string) (*LiveKitSDKPublisher, error) {
	ctx, cancel := context.WithCancel(context.Background())
	pub := &LiveKitSDKPublisher{
		ctx:                 ctx,
		cancel:              cancel,
		videoSampleProvider: NewTrackSampleProvider(true),
		audioSampleProvider: NewTrackSampleProvider(false),
		userDisconnectCh:    make(chan struct{}),
		disconnectOnce:      sync.Once{},
	}

	roomCh := make(chan *lksdk.Room)
	go pub.startUser(wsURL, token, user, roomCh)
	room := <-roomCh

	if room == nil {
		return nil, fmt.Errorf("failed to connect to room")
	}

	pub.room = room

	return pub, nil
}

func (p *LiveKitSDKPublisher) publishVideoTrack(rtpCodec webrtc.RTPCodecCapability) error {
	track, err := lksdk.NewLocalSampleTrack(rtpCodec)
	if err != nil {
		return fmt.Errorf("failed to create video track: %w", err)
	}

	pub, err := p.room.LocalParticipant.PublishTrack(track, &lksdk.TrackPublicationOptions{
		Name:   "video",
		Source: livekit.TrackSource_CAMERA,
	})
	if err != nil {
		return fmt.Errorf("failed to publish video track: %w", err)
	}

	p.videoTrackPub = pub
	logger.Infow("Video track published to LiveKit")

	// Correct handling of H264 RTP packets
	go func() {
		depacketizer := &codecs.H264Packet{}
		lastTimestamp := uint32(0)
		frameBuffer := []byte{}

		for {
			select {
			case <-p.ctx.Done():
				return
			case pkt := <-p.videoSampleProvider.samples:
				if pkt == nil {
					return
				}

				// Check for frame change by timestamp
				if pkt.Timestamp != lastTimestamp && len(frameBuffer) > 0 {
					// Send previous frame to LiveKit
					sample := media.Sample{
						Data:     frameBuffer,
						Duration: time.Millisecond * 33, // ~30fps
					}

					if err := track.WriteSample(sample, nil); err != nil {
						logger.Errorw("Error writing video sample to LiveKit", err)
					}

					frameBuffer = []byte{}
				}

				lastTimestamp = pkt.Timestamp

				// Depacketize H264 RTP
				payload, err := depacketizer.Unmarshal(pkt.Payload)
				if err != nil {
					logger.Errorw("Error depacketizing H264", err)
					continue
				}

				// Add NAL unit to current frame
				frameBuffer = append(frameBuffer, payload...)
			}
		}
	}()

	return nil
}

func (p *LiveKitSDKPublisher) publishAudioTrack(rtpCodec webrtc.RTPCodecCapability) error {
	track, err := lksdk.NewLocalSampleTrack(rtpCodec)
	if err != nil {
		return fmt.Errorf("failed to create audio track: %w", err)
	}

	pub, err := p.room.LocalParticipant.PublishTrack(track, &lksdk.TrackPublicationOptions{
		Name:   "audio",
		Source: livekit.TrackSource_MICROPHONE,
	})
	if err != nil {
		return fmt.Errorf("failed to publish audio track: %w", err)
	}

	p.audioTrackPub = pub
	logger.Infow("Audio track published to LiveKit")

	go func() {
		for {
			select {
			case <-p.ctx.Done():
				return
			case pkt := <-p.audioSampleProvider.samples:
				if pkt == nil {
					return
				}

				sample := media.Sample{
					Data:     pkt.Payload,
					Duration: time.Millisecond * 20,
				}

				if err := track.WriteSample(sample, nil); err != nil {
					logger.Errorw("Error writing audio sample to LiveKit", err)
				}
			}
		}
	}()

	return nil
}

func (p *LiveKitSDKPublisher) Close() {
	p.cancel()
	p.waitForUserDisconnect()
	p.videoSampleProvider.Close()
	p.audioSampleProvider.Close()
}

func (p *LiveKitSDKPublisher) startUser(wsURL string, token string, user string, roomCh chan *lksdk.Room) {
	roomCallback := &lksdk.RoomCallback{
		ParticipantCallback: lksdk.ParticipantCallback{
			OnTrackSubscribed: func(track *webrtc.TrackRemote, pub *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {

				pub.OnRTCP(func(packet rtcp.Packet) {
				})
			},
		},
	}

	room, err := lksdk.ConnectToRoomWithToken(wsURL, token, roomCallback, lksdk.WithAutoSubscribe(true))
	if err != nil {
		logger.Errorw("Error connecting to room", err)
		close(roomCh)
		if room != nil {
			room.Disconnect()
		}
		return
	}

	defer func() {
		room.Disconnect()
		p.userDisconnect()
	}()

	roomCh <- room
	close(roomCh)

	for _, participant := range room.GetParticipants() {
		for _, pub := range participant.Tracks() {
			if remotePub, ok := pub.(*lksdk.RemoteTrackPublication); ok {
				err := remotePub.SetSubscribed(true)
				if err != nil {
					logger.Errorw("Failed to subscribe to track", err, "participant", participant.Identity(), "trackID", remotePub.SID(), "kind", remotePub.Kind().String())
					continue
				}
				logger.Infow("Subscribed to existing track", "participant", participant.Identity(), "trackID", remotePub.SID(), "kind", remotePub.Kind().String())
			}
		}
	}

	<-p.ctx.Done()
	logger.Debugw("User context done, exiting", "user", user)
}

func (p *LiveKitSDKPublisher) waitForUserDisconnect() {
	<-p.userDisconnectCh
}

func (p *LiveKitSDKPublisher) userDisconnect() {
	p.disconnectOnce.Do(func() {
		close(p.userDisconnectCh)
	})
}
