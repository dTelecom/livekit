package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/livekit/protocol/logger"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v3"

	"github.com/livekit/livekit-server/pkg/sfu/buffer"
)

const (
	signalerLabel = "ion_sfu_relay_signaler"
)

type eventType string

const (
	eventTypeAddTrack eventType = "add_rack"
	eventTypeCustom   eventType = "custom"
)

var (
	ErrRelayNotReady    = errors.New("relay Peer is not ready")
	ErrRelaySignalError = errors.New("relay Peer signal state error")
)

type RelayConfig struct {
	SettingEngine webrtc.SettingEngine
	ICEServers    []webrtc.ICEServer
	BufferFactory *buffer.Factory
}

type offerAnswerSignal struct {
	ICECandidates    []webrtc.ICECandidate   `json:"iceCandidates,omitempty"`
	ICEParameters    webrtc.ICEParameters    `json:"iceParameters,omitempty"`
	DTLSParameters   webrtc.DTLSParameters   `json:"dtlsParameters,omitempty"`
	SCTPCapabilities webrtc.SCTPCapabilities `json:"sctpCapabilities,omitempty"`
}

type addTrackSignal struct {
	StreamID        string                     `json:"streamId"`
	TrackID         string                     `json:"trackId"`
	Mid             string                     `json:"mid"`
	Encodings       webrtc.RTPCodingParameters `json:"encodings,omitempty"`
	CodecParameters webrtc.RTPCodecParameters  `json:"codecParameters,omitempty"`
	Meta            string                     `json:"meta,omitempty"`
}

type dcEvent struct {
	ID         uint64    `json:"id"`
	ReplyForID *uint64   `json:"replyForID"`
	Type       eventType `json:"type"`
	Payload    []byte    `json:"payload"`
}

type TrackParameters interface {
	ID() string
	StreamID() string
	Kind() webrtc.RTPCodecType
	Codec() webrtc.RTPCodecParameters
	PayloadType() webrtc.PayloadType
	RID() string
}

type Relay struct {
	mu   sync.Mutex
	rand *rand.Rand

	bufferFactory *buffer.Factory

	me          *webrtc.MediaEngine
	api         *webrtc.API
	ice         *webrtc.ICETransport
	gatherer    *webrtc.ICEGatherer
	role        webrtc.ICERole
	dtls        *webrtc.DTLSTransport
	sctp        *webrtc.SCTPTransport
	signalingDC *webrtc.DataChannel
	dcIndex     uint16

	senders     []*webrtc.RTPSender
	receivers   []*webrtc.RTPReceiver
	localTracks []webrtc.TrackLocal

	pendingReplies sync.Map

	ready  bool
	logger logger.Logger

	onReady                 atomic.Value // func()
	onTrack                 atomic.Value // func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver, meta *TrackMeta)
	onConnectionStateChange atomic.Value // func(state webrtc.ICETransportState)
}

func NewRelay(logger logger.Logger, conf *RelayConfig) (*Relay, error) {
	conf.SettingEngine.BufferFactory = conf.BufferFactory.GetOrNew

	me := webrtc.MediaEngine{}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(&me), webrtc.WithSettingEngine(conf.SettingEngine))
	gatherer, err := api.NewICEGatherer(webrtc.ICEGatherOptions{ICEServers: conf.ICEServers})
	if err != nil {
		return nil, err
	}
	ice := api.NewICETransport(gatherer)
	dtls, err := api.NewDTLSTransport(ice, nil)
	sctp := api.NewSCTPTransport(dtls)
	if err != nil {
		return nil, err
	}

	r := &Relay{
		bufferFactory: conf.BufferFactory,
		rand:          rand.New(rand.NewSource(time.Now().UnixNano())),
		me:            &me,
		api:           api,
		ice:           ice,
		gatherer:      gatherer,
		dtls:          dtls,
		sctp:          sctp,
		logger:        logger,
	}

	sctp.OnDataChannel(func(channel *webrtc.DataChannel) {
		logger.Infow("OnDataChannel", "channelLabel", channel.Label())

		if channel.Label() == signalerLabel {
			r.signalingDC = channel
			channel.OnMessage(r.onSignalingDataChannelMessage)
			channel.OnOpen(func() {
				if f := r.onReady.Load(); f != nil {
					f.(func())()
				}
			})
			return
		}
	})

	ice.OnConnectionStateChange(func(state webrtc.ICETransportState) {
		if f := r.onConnectionStateChange.Load(); f != nil {
			f.(func(webrtc.ICETransportState))(state)
		}
	})

	ice.OnSelectedCandidatePairChange(func(pair *webrtc.ICECandidatePair) {
		r.logger.Infow("Relay selected candidate pair", "value", pair.String())
	})

	return r, nil
}

func (r *Relay) GetBufferFactory() *buffer.Factory {
	return r.bufferFactory
}

func (r *Relay) IsReady() bool {
	return r.ready
}

func (r *Relay) Offer(signalFn func(signal []byte) ([]byte, error)) error {
	if r.gatherer.State() == webrtc.ICEGathererStateNew {
		gatherFinished := make(chan struct{})
		r.gatherer.OnLocalCandidate(func(i *webrtc.ICECandidate) {
			if i == nil {
				close(gatherFinished)
			}
		})
		// Gather candidates
		if err := r.gatherer.Gather(); err != nil {
			return err
		}
		<-gatherFinished
	} else if r.gatherer.State() != webrtc.ICEGathererStateComplete {
		return ErrRelaySignalError
	}

	ls := &offerAnswerSignal{}

	var err error

	if ls.ICECandidates, err = r.gatherer.GetLocalCandidates(); err != nil {
		return err
	}
	if ls.ICEParameters, err = r.gatherer.GetLocalParameters(); err != nil {
		return err
	}
	if ls.DTLSParameters, err = r.dtls.GetLocalParameters(); err != nil {
		return err
	}
	ls.DTLSParameters.UseNullCipher = true

	ls.SCTPCapabilities = r.sctp.GetCapabilities()

	r.role = webrtc.ICERoleControlling
	data, err := json.Marshal(ls)

	remoteSignal, err := signalFn(data)
	if err != nil {
		return err
	}

	rs := &offerAnswerSignal{}

	if err = json.Unmarshal(remoteSignal, rs); err != nil {
		return err
	}

	if err = r.start(rs); err != nil {
		return err
	}

	if r.signalingDC, err = r.createDataChannel(signalerLabel); err != nil {
		return err
	}

	r.signalingDC.OnMessage(r.onSignalingDataChannelMessage)

	r.signalingDC.OnOpen(func() {
		if f := r.onReady.Load(); f != nil {
			f.(func())()
		}
	})

	return nil
}

func (r *Relay) Answer(request []byte) ([]byte, error) {
	if r.gatherer.State() == webrtc.ICEGathererStateNew {
		gatherFinished := make(chan struct{})
		r.gatherer.OnLocalCandidate(func(i *webrtc.ICECandidate) {
			if i == nil {
				close(gatherFinished)
			}
		})
		// Gather candidates
		if err := r.gatherer.Gather(); err != nil {
			return nil, err
		}
		<-gatherFinished
	} else if r.gatherer.State() != webrtc.ICEGathererStateComplete {
		return nil, ErrRelaySignalError
	}

	ls := &offerAnswerSignal{}

	var err error

	if ls.ICECandidates, err = r.gatherer.GetLocalCandidates(); err != nil {
		return nil, err
	}
	if ls.ICEParameters, err = r.gatherer.GetLocalParameters(); err != nil {
		return nil, err
	}
	if ls.DTLSParameters, err = r.dtls.GetLocalParameters(); err != nil {
		return nil, err
	}
	ls.DTLSParameters.UseNullCipher = true

	ls.SCTPCapabilities = r.sctp.GetCapabilities()

	r.role = webrtc.ICERoleControlled

	rs := &offerAnswerSignal{}
	if err = json.Unmarshal(request, rs); err != nil {
		return nil, err
	}

	go func() {
		if err = r.start(rs); err != nil {
			r.logger.Errorw("Error starting relay", err)
		}
	}()

	return json.Marshal(ls)
}

func (r *Relay) start(remoteSignal *offerAnswerSignal) error {
	if err := r.ice.SetRemoteCandidates(remoteSignal.ICECandidates); err != nil {
		return err
	}

	if err := r.ice.Start(r.gatherer, remoteSignal.ICEParameters, &r.role); err != nil {
		return err
	}

	if err := r.dtls.Start(remoteSignal.DTLSParameters); err != nil {
		return err
	}

	go func() {
		for {
			srtcpSession, err := r.dtls.GetSRTCPSession()
			if err != nil {
				r.logger.Warnw("undeclaredMediaProcessor failed to open SrtcpSession: %v", err)
				return
			}

			_, ssrc, err := srtcpSession.AcceptStream()
			if err != nil {
				r.logger.Warnw("Failed to accept RTCP %v", err)
				return
			}
			r.logger.Warnw("Incoming unhandled RTCP ssrc(%d), OnTrack will not be fired", nil, "ssrc", ssrc)
		}
	}()

	if err := r.sctp.Start(remoteSignal.SCTPCapabilities); err != nil {
		return err
	}
	r.ready = true
	return nil
}

func (r *Relay) WriteRTCP(pkts []rtcp.Packet) error {
	for _, pkt := range pkts {
		if pli, ok := pkt.(*rtcp.PictureLossIndication); ok {
			fmt.Printf("PictureLossIndication(MediaSSRC=%v, SenderSSRC=%v)\n", pli.MediaSSRC, pli.SenderSSRC)
		}
	}
	_, err := r.dtls.WriteRTCP(pkts)
	return err
}

func (r *Relay) AddTrack(ctx context.Context, rtpParameters webrtc.RTPParameters, trackParameters TrackParameters, localTrack webrtc.TrackLocal, mid string, trackMeta string) (*webrtc.RTPSender, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	codec := trackParameters.Codec()
	sdr, err := r.api.NewRTPSender(localTrack, r.dtls)
	if err != nil {
		return nil, fmt.Errorf("NewRTPSender error: %w", err)
	}
	if err = r.me.RegisterCodec(codec, trackParameters.Kind()); err != nil {
		return nil, fmt.Errorf("RegisterCodec error: %w", err)
	}

	s := &addTrackSignal{
		StreamID:        trackParameters.StreamID(),
		TrackID:         trackParameters.ID(),
		Mid:             mid,
		CodecParameters: trackParameters.Codec(),
		Encodings: webrtc.RTPCodingParameters{
			RID:         trackParameters.RID(),
			SSRC:        sdr.GetParameters().Encodings[0].SSRC,
			PayloadType: trackParameters.PayloadType(),
		},
		Meta: trackMeta,
	}
	pld, marshalErr := json.Marshal(s)
	if marshalErr != nil {
		return nil, fmt.Errorf("marshal error: %w", marshalErr)
	}

	event := dcEvent{
		ID:      r.rand.Uint64(),
		Type:    eventTypeAddTrack,
		Payload: pld,
	}

	replyCh, sendErr := r.send(event, true)
	if sendErr != nil {
		return nil, fmt.Errorf("send error: %w", marshalErr)
	}

	select {
	case <-replyCh:
		r.logger.Infow("add track reply received")
	case <-ctx.Done():
		return nil, fmt.Errorf("add track context err: %w", ctx.Err())
	}

	if err = sdr.Send(webrtc.RTPSendParameters{
		RTPParameters: rtpParameters,
		Encodings: []webrtc.RTPEncodingParameters{
			{
				webrtc.RTPCodingParameters{
					SSRC:        s.Encodings.SSRC,
					PayloadType: s.Encodings.PayloadType,
				},
			},
		},
	}); err != nil {
		r.logger.Errorw("Send RTPSender failed", err)
	}

	r.localTracks = append(r.localTracks, localTrack)
	r.senders = append(r.senders, sdr)

	return sdr, nil
}

func (r *Relay) onSignalingDataChannelMessage(msg webrtc.DataChannelMessage) {
	r.logger.Infow("onSignalingDataChannelMessage")

	event := &dcEvent{}
	if err := json.Unmarshal(msg.Data, event); err != nil {
		r.logger.Errorw("Error marshaling remote message", err)

		return
	}

	if event.ReplyForID != nil {
		r.logger.Infow("reply received")

		replyForID := *event.ReplyForID
		if replyCh, loaded := r.pendingReplies.LoadAndDelete(replyForID); loaded {
			r.logger.Infow("sending reply")
			replyCh.(chan []byte) <- event.Payload
			r.logger.Infow("reply sent")
		} else {
			r.logger.Warnw("undefined reply", nil, "replyForID", replyForID)
		}
	} else if event.Type == eventTypeAddTrack {
		r.logger.Infow("add track received")

		s := &addTrackSignal{}
		if err := json.Unmarshal(event.Payload, s); err != nil {
			r.logger.Errorw("Error unmarshal remote message", err)
			return
		}
		if err := r.handleAddTrack(s); err != nil {
			r.logger.Errorw("Error receiving remote track", err)
			return
		}

		replyEvent := dcEvent{
			ID:         r.rand.Uint64(),
			ReplyForID: &event.ID,
			Type:       event.Type,
		}

		if _, err := r.send(replyEvent, false); err != nil {
			r.logger.Errorw("Error replying message", err)
			return
		}
	} else if event.Type == eventTypeCustom {
		r.logger.Infow("custom received")
	}
}

func (r *Relay) handleAddTrack(s *addTrackSignal) error {
	var k webrtc.RTPCodecType
	switch {
	case strings.HasPrefix(s.CodecParameters.MimeType, "audio/"):
		k = webrtc.RTPCodecTypeAudio
	case strings.HasPrefix(s.CodecParameters.MimeType, "video/"):
		k = webrtc.RTPCodecTypeVideo
	default:
		k = webrtc.RTPCodecType(0)
	}
	if err := r.me.RegisterCodec(s.CodecParameters, k); err != nil {
		return err
	}

	recv, err := r.api.NewRTPReceiver(k, r.dtls)
	if err != nil {
		return err
	}

	if err = recv.Receive(webrtc.RTPReceiveParameters{Encodings: []webrtc.RTPDecodingParameters{
		{
			webrtc.RTPCodingParameters{
				// RID:         s.Encodings.RID,
				SSRC:        s.Encodings.SSRC,
				PayloadType: s.Encodings.PayloadType,
			},
		},
	}}); err != nil {
		return err
	} else {
		r.logger.Infow("RTPReceiver.Receive", "SSRC", s.Encodings.SSRC, "RID", s.Encodings.RID)
	}

	recv.SetRTPParameters(webrtc.RTPParameters{
		HeaderExtensions: nil,
		Codecs:           []webrtc.RTPCodecParameters{s.CodecParameters},
	})

	track := recv.Track()

	if f := r.onTrack.Load(); f != nil {
		f.(func(*webrtc.TrackRemote, *webrtc.RTPReceiver, string, string, string, string, string))(track, recv, s.Mid, s.TrackID, s.StreamID, s.Encodings.RID, s.Meta)
	}

	r.receivers = append(r.receivers, recv)

	return nil
}

func (r *Relay) createDataChannel(label string) (*webrtc.DataChannel, error) {
	idx := r.dcIndex
	r.dcIndex++
	dcParams := &webrtc.DataChannelParameters{
		Label:   label,
		ID:      &idx,
		Ordered: true,
	}
	return r.api.NewDataChannel(r.sctp, dcParams)
}

func (r *Relay) OnReady(f func()) {
	r.onReady.Store(f)
}

func (r *Relay) OnTrack(f func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver, mid string, trackId string, streamId string, rid string, meta string)) {
	r.onTrack.Store(f)
}

func (r *Relay) OnConnectionStateChange(f func(state webrtc.ICETransportState)) {
	r.onConnectionStateChange.Store(f)
}

func (r *Relay) Send(payload []byte) error {
	event := dcEvent{
		ID:      r.rand.Uint64(),
		Type:    eventTypeCustom,
		Payload: payload,
	}
	_, err := r.send(event, false)
	return err
}

func (r *Relay) SendReply(replyForID uint64, payload []byte) error {
	event := dcEvent{
		ID:         r.rand.Uint64(),
		ReplyForID: &replyForID,
		Type:       eventTypeCustom,
		Payload:    payload,
	}
	_, err := r.send(event, false)
	return err
}

func (r *Relay) SendAndExpectReply(payload []byte) (<-chan []byte, error) {
	event := dcEvent{
		ID:      r.rand.Uint64(),
		Type:    eventTypeCustom,
		Payload: payload,
	}
	return r.send(event, true)
}

func (r *Relay) send(event dcEvent, replyExpected bool) (<-chan []byte, error) {
	data, marshalErr := json.Marshal(event)
	if marshalErr != nil {
		return nil, fmt.Errorf("can not marshal DC event: %w", marshalErr)
	}
	var reply chan []byte
	if replyExpected {
		reply = make(chan []byte, 1)
		r.pendingReplies.Store(event.ID, reply)
	}
	if err := r.signalingDC.Send(data); err != nil {
		r.pendingReplies.Delete(event.ID)
		return nil, fmt.Errorf("can not send DC event: %w", err)
	}
	return reply, nil
}

func (r *Relay) DebugInfo() map[string]interface{} {
	iceConn := r.ice.GetConn()
	return map[string]interface{}{
		"BytesSent":     iceConn.BytesSent(),
		"BytesReceived": iceConn.BytesReceived(),
	}
}
