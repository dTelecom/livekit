package relay

import (
	"github.com/pion/webrtc/v3"

	"github.com/livekit/livekit-server/pkg/sfu/buffer"
)

type RelayConfig struct {
	SettingEngine webrtc.SettingEngine
	ICEServers    []webrtc.ICEServer
	BufferFactory *buffer.Factory
}

type TrackParameters interface {
	ID() string
	StreamID() string
	Kind() webrtc.RTPCodecType
	Codec() webrtc.RTPCodecParameters
	PayloadType() webrtc.PayloadType
	RID() string
}
