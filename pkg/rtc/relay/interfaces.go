package relay

import (
	"context"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v3"

	"github.com/livekit/livekit-server/pkg/sfu/buffer"
)

type Relay interface {
	GetBufferFactory() *buffer.Factory
	// IsReady() bool
	Offer(signalFn func(signal []byte) ([]byte, error)) error
	Answer(request []byte) ([]byte, error)
	WriteRTCP(pkts []rtcp.Packet) error
	AddTrack(ctx context.Context, rtpParameters webrtc.RTPParameters, trackParameters TrackParameters, track webrtc.TrackLocal, mid string, trackMeta string) (*webrtc.RTPSender, error)
	OnReady(f func())
	OnTrack(f func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver, mid string, trackId string, streamId string, rid string, meta string))
	OnConnectionStateChange(f func(state webrtc.ICEConnectionState))
	Send(payload []byte) error
	SendReply(replyForID uint64, payload []byte) error
	SendAndExpectReply(payload []byte) (<-chan []byte, error)
	DebugInfo() map[string]interface{}
}
