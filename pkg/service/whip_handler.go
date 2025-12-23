package service

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/protocol/logger"
	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v3"
)

type whipSession struct {
	publisher      *LiveKitSDKPublisher
	peerConnection *webrtc.PeerConnection
	CreatedAt      time.Time
}

type SessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*whipSession
}

func NewSessionManager() *SessionManager {
	sm := &SessionManager{
		mu:       sync.RWMutex{},
		sessions: make(map[string]*whipSession),
	}
	return sm
}

func (sm *SessionManager) AddSession(sessionID string, session *whipSession) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.sessions[sessionID] = session
}

func (sm *SessionManager) RemoveSession(sessionID string) {
	sm.mu.RLock()
	session, exists := sm.sessions[sessionID]
	sm.mu.RUnlock()

	if exists {
		if session.publisher != nil {
			session.publisher.Close()
		}
		if session.peerConnection != nil {
			session.peerConnection.Close()
		}

		sm.mu.Lock()
		delete(sm.sessions, sessionID)
		sm.mu.Unlock()
	}
}

func (sm *SessionManager) GetSession(sessionID string) (*whipSession, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	session, exists := sm.sessions[sessionID]
	return session, exists
}

type WhipHandler struct {
	clientProvider *ClientProvider
	sessionManager *SessionManager
	wsURL          string
}

func NewWhipHandler(clientProvider *ClientProvider, conf *config.Config) *WhipHandler {
	wsURL := ""
	if conf.Domain != "" {
		wsURL = "wss://" + conf.Domain
	} else {
		wsURL = "ws://localhost:" + strconv.Itoa(int(conf.Port))
	}

	sessionManager := NewSessionManager()
	return &WhipHandler{
		clientProvider: clientProvider,
		sessionManager: sessionManager,
		wsURL:          wsURL,
	}
}

func (s *WhipHandler) HandleWhipRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "*")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	claims := GetGrants(r.Context())
	token := GetToken(r.Context())

	if r.Method == http.MethodDelete {
		s.sessionManager.RemoveSession(token)
		w.WriteHeader(http.StatusOK)
		return
	}

	sdpOffer, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	err = s.createSession(w, s.prepareOffer(sdpOffer), token, string(claims.Name))
	if err != nil {
		logger.Errorw("failed to create session", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *WhipHandler) prepareOffer(sdpOffer []byte) string {
	if len(sdpOffer) > 0 && !(len(sdpOffer) >= 2 && sdpOffer[0] == 'v' && sdpOffer[1] == '=') {
		if i := bytes.Index(sdpOffer, []byte("\r\n\r\n")); i >= 0 && i+4 < len(sdpOffer) {
			sdpOffer = sdpOffer[i+4:]
		}
		if !(len(sdpOffer) >= 2 && sdpOffer[0] == 'v' && sdpOffer[1] == '=') {
			if i := bytes.Index(sdpOffer, []byte("\nv=")); i >= 0 && i+1 < len(sdpOffer) {
				sdpOffer = sdpOffer[i+1:]
			}
		}
	}
	return string(sdpOffer)
}

func (s *WhipHandler) createSession(res http.ResponseWriter, offer string, token string, user string) error {
	logger.Debugw("creating session", "offer", offer, "token", token, "user", user)
	mediaEngine := &webrtc.MediaEngine{}

	if err := s.registerCodecs(mediaEngine); err != nil {
		return fmt.Errorf("failed to register codecs: %w", err)
	}

	interceptorRegistry := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(mediaEngine, interceptorRegistry); err != nil {
		return fmt.Errorf("failed to register interceptors: %w", err)
	}

	api := webrtc.NewAPI(
		webrtc.WithMediaEngine(mediaEngine),
		webrtc.WithInterceptorRegistry(interceptorRegistry),
	)

	peerConnection, err := api.NewPeerConnection(webrtc.Configuration{ICEServers: []webrtc.ICEServer{
		{
			URLs: []string{"stun:stun.l.google.com:19302"},
		}}})
	if err != nil {
		return fmt.Errorf("failed to create peer connection: %w", err)
	}

	if _, err = peerConnection.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo); err != nil {
		return fmt.Errorf("failed to add transceiver for video: %w", err)
	}
	if _, err = peerConnection.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio); err != nil {
		return fmt.Errorf("failed to add transceiver for audio: %w", err)
	}

	publisher, err := StartLiveKitSDKPublisher(s.wsURL, token, user)
	if err != nil {
		return fmt.Errorf("failed to start livekit sdk publisher: %w", err)
	}

	s.sessionManager.AddSession(token, &whipSession{
		publisher:      publisher,
		peerConnection: peerConnection,
		CreatedAt:      time.Now(),
	})

	logger.Debugw("publisher started", "publisher", publisher)

	peerConnection.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		if track.Kind() == webrtc.RTPCodecTypeVideo {
			if err := publisher.publishVideoTrack(webrtc.RTPCodecCapability{
				MimeType:  webrtc.MimeTypeH264,
				ClockRate: 90000,
			}); err != nil {
				logger.Errorw("failed to publish video track: %w", err)
			}
		}

		if track.Kind() == webrtc.RTPCodecTypeAudio {
			if err := publisher.publishAudioTrack(webrtc.RTPCodecCapability{
				MimeType:  webrtc.MimeTypeOpus,
				ClockRate: 48000,
				Channels:  2,
			}); err != nil {
				logger.Errorw("failed to publish audio track: %w", err)
			}
		}

		// Manual sending PLI for requesting keyframes every 2 seconds
		go func() {
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()

			for range ticker.C {
				if err := peerConnection.WriteRTCP([]rtcp.Packet{
					&rtcp.PictureLossIndication{
						MediaSSRC: uint32(track.SSRC()),
					},
				}); err != nil {
					if !errors.Is(err, io.ErrClosedPipe) {
						logger.Errorw("Failed to send PLI", err)
					}
					return
				}
			}
		}()

		// RTCP reader for handling feedback
		go func() {
			for {
				_, _, err := receiver.ReadRTCP()
				if err != nil {
					if errors.Is(err, io.EOF) {
						logger.Debugw("EOF reading RTCP from publish peer connection")
						break
					}
					return
				}
			}
		}()

		go func() {
			for {
				pkt, _, err := track.ReadRTP()
				if err != nil {
					if errors.Is(err, io.EOF) {
						logger.Debugw("EOF reading RTP from publish peer connection")
						break
					}
					logger.Errorw("error reading RTP from publish peer connection", err)
					return
				}

				originalPkt := *pkt

				// Forward to LiveKit
				if publisher != nil {
					if track.Kind() == webrtc.RTPCodecTypeVideo {
						publisher.videoSampleProvider.PushSample(&originalPkt)
					} else if track.Kind() == webrtc.RTPCodecTypeAudio {
						publisher.audioSampleProvider.PushSample(&originalPkt)
					}
				}
			}
		}()
	})

	peerConnection.OnICEConnectionStateChange(func(connectionState webrtc.ICEConnectionState) {
		logger.Debugw("ICE Connection State has changed", "connectionState", connectionState.String())

		switch connectionState {
		case webrtc.ICEConnectionStateFailed,
			webrtc.ICEConnectionStateClosed,
			webrtc.ICEConnectionStateDisconnected:
			s.sessionManager.RemoveSession(token)
		}
	})

	s.writeAnswer(res, peerConnection, []byte(offer), "/whip")
	return nil
}

func (s *WhipHandler) writeAnswer(res http.ResponseWriter, peerConnection *webrtc.PeerConnection, offer []byte, path string) {
	if err := peerConnection.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer, SDP: string(offer),
	}); err != nil {
		http.Error(res, "Failed to set remote description", http.StatusInternalServerError)
		return
	}

	gatherComplete := webrtc.GatheringCompletePromise(peerConnection)

	answer, err := peerConnection.CreateAnswer(nil)
	if err != nil {
		http.Error(res, "Failed to create answer", http.StatusInternalServerError)
		return
	} else if err = peerConnection.SetLocalDescription(answer); err != nil {
		http.Error(res, "Failed to set local description", http.StatusInternalServerError)
		return
	}

	<-gatherComplete

	res.Header().Add("Location", path)
	res.WriteHeader(http.StatusCreated)

	fmt.Fprint(res, peerConnection.LocalDescription().SDP)
}

func (s *WhipHandler) registerCodecs(me *webrtc.MediaEngine) error {
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeH264,
			ClockRate:   90000,
			Channels:    0,
			SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
			RTCPFeedback: []webrtc.RTCPFeedback{
				{Type: "nack"},
				{Type: "nack", Parameter: "pli"},
				{Type: "goog-remb"},
			},
		},
		PayloadType: 125,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		return fmt.Errorf("RegisterCodec error: %w", err)
	}

	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeOpus,
			ClockRate: 48000,
			Channels:  2,
		},
		PayloadType: 111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return fmt.Errorf("RegisterCodec error: %w", err)
	}

	return nil
}
