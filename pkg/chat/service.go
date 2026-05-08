package chat

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/config"
)

// Service holds the per-node chat state. One instance lives in the LivekitServer.
type Service struct {
	cfg      config.ChatConfig
	presence *PresenceTracker
	disp     *Dispatcher
	lookup   ClientLookup
	log      logger.Logger
	upgrader websocket.Upgrader
}

// NewService is wired by Google Wire (see pkg/service/wire.go). Takes the
// shared presence tracker, dispatcher, and a ClientLookup adapter for chat
// auth (the real adapter wraps service.ClientProvider; tests inject a fake).
func NewService(
	cfg config.ChatConfig,
	presence *PresenceTracker,
	disp *Dispatcher,
	lookup ClientLookup,
) *Service {
	return &Service{
		cfg:      cfg,
		presence: presence,
		disp:     disp,
		lookup:   lookup,
		log:      logger.GetLogger(),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

// Handler returns the http.Handler to mount at /chat/ws. Wraps the WS upgrade
// in chat-token middleware. Returns 404 when chat is disabled.
func (s *Service) Handler() http.Handler {
	if !s.cfg.Enabled {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.NotFound(w, nil)
		})
	}
	return ChatTokenMiddleware(http.HandlerFunc(s.serveWS), s.lookup)
}

// ── frame shapes (chat-wire-contract.md §3) ─────────────────────────────────

type frameKind string

const (
	kindSend       frameKind = "chat_send"
	kindAck        frameKind = "chat_ack"  // reserved; unused in v1 (ack flows over HTTP)
	kindPing       frameKind = "chat_ping"
	kindEnvelope   frameKind = "chat_envelope"
	kindSendResult frameKind = "chat_send_result"
	kindPong       frameKind = "chat_pong"
)

type frameEnvelope struct {
	Kind frameKind `json:"kind"`
}

type chatSendIn struct {
	Kind      frameKind    `json:"kind"`
	ToUserID  string       `json:"to_user_id"`
	Ephemeral bool         `json:"ephemeral,omitempty"`
	MsgType   string       `json:"msg_type,omitempty"` // optional; default "normal"
	Targets   []SendTarget `json:"targets"`
}

// SendTarget already JSON-tagged in dispatch.go for re-use; here we just
// re-declare the wire struct for clarity (Go's struct tags would conflict if
// redeclared, so we reuse SendTarget directly).

type chatEnvelopeOut struct {
	Kind           frameKind `json:"kind"`
	EnvelopeUUID   string    `json:"envelope_uuid"`
	SenderUserID   string    `json:"sender_user_id"`
	SenderDeviceID string    `json:"sender_device_id"`
	Ciphertext     string    `json:"ciphertext"`
	MsgType        string    `json:"msg_type"`
}

type chatSendResultOut struct {
	Kind    frameKind    `json:"kind"`
	Results []SendResult `json:"results"`
}

type chatPong struct {
	Kind frameKind `json:"kind"`
}

// per-connection state
type wsConn struct {
	conn   *websocket.Conn
	writeM sync.Mutex
}

func (c *wsConn) writeJSON(v interface{}) error {
	c.writeM.Lock()
	defer c.writeM.Unlock()
	return c.conn.WriteJSON(v)
}

// ── WS handler ──────────────────────────────────────────────────────────────

func (s *Service) serveWS(w http.ResponseWriter, r *http.Request) {
	claims := GetClaims(r.Context())
	if claims == nil {
		http.Error(w, "missing chat claims", http.StatusInternalServerError)
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.log.Debugw("chat ws upgrade failed", "err", err)
		return
	}
	wc := &wsConn{conn: conn}

	apiKey := claims.Issuer
	userID := claims.Subject
	deviceID := claims.DeviceID
	webhookURL := claims.WebhookURL

	// Inbound envelopes from gossipsub → forward to WS, then ACK on the per-envelope
	// ack topic so the sender's node can mark this delivery as live.
	onEnvelope := func(payload []byte) {
		var p envelopePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			s.log.Debugw("chat ws: bad envelope payload", "err", err)
			return
		}
		out := chatEnvelopeOut{
			Kind:           kindEnvelope,
			EnvelopeUUID:   p.EnvelopeUUID,
			SenderUserID:   p.SenderUserID,
			SenderDeviceID: p.SenderDeviceID,
			Ciphertext:     p.Ciphertext,
			MsgType:        p.MsgType,
		}
		if err := wc.writeJSON(out); err != nil {
			s.log.Debugw("chat ws: write envelope failed", "err", err)
			return
		}
		// Best-effort ACK back to sender (own node also sees this, but harmless).
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		s.disp.PublishAck(ctx, p.EnvelopeUUID)
	}

	if err := s.presence.RegisterDevice(r.Context(), apiKey, userID, deviceID, onEnvelope); err != nil {
		s.log.Errorw("chat ws: register device failed", err, "user", userID, "device", deviceID)
		_ = conn.Close()
		return
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.presence.UnregisterDevice(ctx, apiKey, userID, deviceID)
		_ = conn.Close()
	}()

	// Read loop
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				s.log.Debugw("chat ws: read error", "err", err, "user", userID, "device", deviceID)
			}
			return
		}
		s.handleFrame(r.Context(), wc, claims, webhookURL, raw)
	}
}

func (s *Service) handleFrame(ctx context.Context, wc *wsConn, claims *ChatClaims, webhookURL string, raw []byte) {
	var env frameEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		s.log.Debugw("chat ws: bad frame", "err", err)
		return
	}
	switch env.Kind {
	case kindPing:
		_ = wc.writeJSON(chatPong{Kind: kindPong})

	case kindSend:
		var req chatSendIn
		if err := json.Unmarshal(raw, &req); err != nil {
			s.log.Debugw("chat ws: bad chat_send", "err", err)
			return
		}
		if err := s.validateSend(&req); err != nil {
			result := chatSendResultOut{
				Kind:    kindSendResult,
				Results: []SendResult{{Status: StatusError, Err: err.Error()}},
			}
			_ = wc.writeJSON(result)
			return
		}
		msgType := req.MsgType
		if msgType == "" {
			msgType = "normal"
		}
		out := s.disp.SendAll(
			ctx,
			claims.Issuer,
			claims.Subject,
			claims.DeviceID,
			req.ToUserID,
			req.Targets,
			msgType,
			req.Ephemeral,
			webhookURL,
		)
		_ = wc.writeJSON(chatSendResultOut{Kind: kindSendResult, Results: out})

	default:
		s.log.Debugw("chat ws: unknown frame kind", "kind", env.Kind)
	}
}

func (s *Service) validateSend(req *chatSendIn) error {
	if req.ToUserID == "" {
		return errors.New("to_user_id required")
	}
	if len(req.Targets) == 0 {
		return errors.New("targets must be non-empty")
	}
	if s.cfg.MaxTargetsPerSend > 0 && len(req.Targets) > s.cfg.MaxTargetsPerSend {
		return fmt.Errorf("too many targets: %d > %d", len(req.Targets), s.cfg.MaxTargetsPerSend)
	}
	maxBytes := s.cfg.MaxEnvelopeBytes
	if maxBytes == 0 {
		maxBytes = 65536
	}
	for _, t := range req.Targets {
		if t.DeviceID == "" {
			return errors.New("target.device_id required")
		}
		if t.EnvelopeUUID == "" {
			return errors.New("target.envelope_uuid required")
		}
		if t.Ciphertext == "" {
			return errors.New("target.ciphertext required")
		}
		// Size check: base64 decoded length is ~3/4 of the encoded length.
		decLen := base64.StdEncoding.DecodedLen(len(t.Ciphertext))
		if decLen > maxBytes {
			return fmt.Errorf("target ciphertext exceeds %d bytes", maxBytes)
		}
	}
	if req.MsgType != "" && req.MsgType != "prekey" && req.MsgType != "normal" {
		return fmt.Errorf("invalid msg_type: %q", req.MsgType)
	}
	return nil
}
