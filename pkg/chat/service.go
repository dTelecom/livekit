package chat

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

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

// ── frame shapes ────────────────────────────────────────────────────────────

type frameKind string

const (
	kindSend         frameKind = "chatSend"
	kindEnvelopeAck  frameKind = "chatEnvelopeAck"
	kindPing         frameKind = "chatPing"
	kindEnvelope     frameKind = "chatEnvelope"
	kindSendResult   frameKind = "chatSendResult"
	kindPong         frameKind = "chatPong"
)

// wsConnPendingCap bounds the per-wsConn pending-envelope set used for
// chatEnvelopeAck anti-spoof. Bursty senders to a slow receiver could
// otherwise grow the map without bound; on overflow the oldest entry is
// evicted, which effectively forces a webhook fallback for that envelope
// from the sender's POV (acceptable — the webhook is the durability layer).
const wsConnPendingCap = 256

type frameEnvelope struct {
	Kind frameKind `json:"kind"`
}

type chatSendIn struct {
	Kind      frameKind    `json:"kind"`
	ToUserID  string       `json:"toUserId"`
	Ephemeral bool         `json:"ephemeral,omitempty"`
	MsgType   string       `json:"msgType,omitempty"` // optional; default "normal"
	Targets   []SendTarget `json:"targets"`
}

// chatEnvelopeAckIn is the client-device ack for an inbound chatEnvelope.
// Sent after the SDK has decrypted and durably stored the envelope. The
// node validates the envelopeUuid against the per-wsConn pending set
// (populated when the node wrote a chatEnvelope to this WS) — unknown
// uuids are dropped silently to keep spoofers from waking arbitrary
// senders' inflight channels.
type chatEnvelopeAckIn struct {
	Kind           frameKind `json:"kind"`
	EnvelopeUUID   string    `json:"envelopeUuid"`
	SenderUserID   string    `json:"senderUserId"`
	SenderDeviceID string    `json:"senderDeviceId"`
}

// SendTarget already JSON-tagged in dispatch.go for re-use; here we just
// re-declare the wire struct for clarity (Go's struct tags would conflict if
// redeclared, so we reuse SendTarget directly).

type chatEnvelopeOut struct {
	Kind           frameKind `json:"kind"`
	EnvelopeUUID   string    `json:"envelopeUuid"`
	SenderUserID   string    `json:"senderUserId"`
	SenderDeviceID string    `json:"senderDeviceId"`
	Ciphertext     string    `json:"ciphertext"`
	MsgType        string    `json:"msgType"`
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

	// Pending envelopes the node has WRITTEN to this WS that haven't been
	// acked yet. Keyed by envelopeUuid → sender identity. Used to validate
	// inbound chatEnvelopeAck frames so a hostile client can't wake
	// arbitrary senders' inflight channels.
	pendingM         sync.Mutex
	pendingDelivered map[string]pendingEntry
	pendingOrder     []string // FIFO of envelopeUuids; oldest at index 0
}

type pendingEntry struct {
	senderUserID   string
	senderDeviceID string
}

func newWsConn(conn *websocket.Conn) *wsConn {
	return &wsConn{
		conn:             conn,
		pendingDelivered: make(map[string]pendingEntry, wsConnPendingCap),
		pendingOrder:     make([]string, 0, wsConnPendingCap),
	}
}

func (c *wsConn) writeJSON(v interface{}) error {
	c.writeM.Lock()
	defer c.writeM.Unlock()
	return c.conn.WriteJSON(v)
}

// pendingAdd records an envelopeUuid the node just wrote to this WS. If
// the set is at capacity, the oldest entry is evicted (FIFO).
func (c *wsConn) pendingAdd(envelopeUUID, senderUserID, senderDeviceID string) {
	c.pendingM.Lock()
	defer c.pendingM.Unlock()
	if _, exists := c.pendingDelivered[envelopeUUID]; exists {
		// Retry from sender lands here — the entry already exists.
		// No-op; don't reorder so eviction stays predictable.
		return
	}
	if len(c.pendingOrder) >= wsConnPendingCap {
		// Evict oldest. Sender will time out and webhook-fallback.
		oldest := c.pendingOrder[0]
		c.pendingOrder = c.pendingOrder[1:]
		delete(c.pendingDelivered, oldest)
	}
	c.pendingDelivered[envelopeUUID] = pendingEntry{senderUserID: senderUserID, senderDeviceID: senderDeviceID}
	c.pendingOrder = append(c.pendingOrder, envelopeUUID)
}

// pendingConsume validates an inbound chatEnvelopeAck against the pending
// set. Returns true (and removes the entry) if the envelopeUuid was in
// the set AND the claimed sender ids match what we wrote. Returns false
// for unknown uuids (silent spoof rejection) or sender-id mismatch.
func (c *wsConn) pendingConsume(envelopeUUID, senderUserID, senderDeviceID string) bool {
	c.pendingM.Lock()
	defer c.pendingM.Unlock()
	entry, ok := c.pendingDelivered[envelopeUUID]
	if !ok {
		return false
	}
	if entry.senderUserID != senderUserID || entry.senderDeviceID != senderDeviceID {
		return false
	}
	delete(c.pendingDelivered, envelopeUUID)
	for i, uuid := range c.pendingOrder {
		if uuid == envelopeUUID {
			c.pendingOrder = append(c.pendingOrder[:i], c.pendingOrder[i+1:]...)
			break
		}
	}
	return true
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
	wc := newWsConn(conn)

	apiKey := claims.Issuer
	userID := claims.Subject
	deviceID := claims.DeviceID
	webhookURL := claims.WebhookURL

	// Inbound mesh messages on this device's envelope topic. The kind
	// discriminator routes to one of three paths:
	//   envelope        → write to WS, record in pendingDelivered for ack
	//                     validation. ACK to sender NO LONGER fires here —
	//                     it fires when the client sends back chatEnvelopeAck
	//                     after durably storing the envelope.
	//   ack             → wake the SendOne goroutine waiting on this envelopeUuid
	//   presenceReply   → wake the QueryAnyLive goroutine waiting on this queryId
	//
	// Returns an error if the live-WS write failed. Used by DeliverLocal
	// (same-node fast path) to skip the retry tick when the write was bad;
	// cross-node path discards the error (sender finds out via no-ack).
	onEnvelope := func(payload []byte) error {
		var m meshMessage
		if err := json.Unmarshal(payload, &m); err != nil {
			s.log.Debugw("chat ws: bad mesh payload", "err", err)
			return err
		}
		switch m.Kind {
		case MeshKindEnvelope:
			out := chatEnvelopeOut{
				Kind:           kindEnvelope,
				EnvelopeUUID:   m.EnvelopeUUID,
				SenderUserID:   m.SenderUserID,
				SenderDeviceID: m.SenderDeviceID,
				Ciphertext:     m.Ciphertext,
				MsgType:        m.MsgType,
			}
			if err := wc.writeJSON(out); err != nil {
				s.log.Debugw("chat ws: write envelope failed", "err", err)
				return err
			}
			// Anti-spoof bookkeeping: record this envelopeUuid + sender ids
			// so the client's eventual chatEnvelopeAck can be validated.
			// PublishAck is NOT called here anymore — it fires from the
			// chatEnvelopeAck handler below, after the client confirms
			// durable storage on its end.
			wc.pendingAdd(m.EnvelopeUUID, m.SenderUserID, m.SenderDeviceID)
			return nil
		case MeshKindAck:
			s.disp.SignalAck(m.EnvelopeUUID)
			return nil
		case MeshKindPresenceReply:
			s.presence.SignalQueryReply(m.QueryID)
			return nil
		default:
			s.log.Debugw("chat ws: unknown mesh kind", "kind", m.Kind)
			return nil
		}
	}

	if err := s.presence.RegisterDevice(apiKey, userID, deviceID, onEnvelope); err != nil {
		s.log.Errorw("chat ws: register device failed", err, "user", userID, "device", deviceID)
		_ = conn.Close()
		return
	}
	defer func() {
		s.presence.UnregisterDevice(apiKey, userID, deviceID)
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
			s.log.Debugw("chat ws: bad chatSend", "err", err)
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

	case kindEnvelopeAck:
		var ack chatEnvelopeAckIn
		if err := json.Unmarshal(raw, &ack); err != nil {
			s.log.Debugw("chat ws: bad chatEnvelopeAck", "err", err)
			return
		}
		if ack.EnvelopeUUID == "" || ack.SenderUserID == "" || ack.SenderDeviceID == "" {
			s.log.Debugw("chat ws: chatEnvelopeAck missing required field")
			return
		}
		// Anti-spoof: only honor acks for envelopes the node actually wrote
		// to this WS, AND whose sender ids match what we wrote (otherwise a
		// hostile client could wake unrelated senders' inflight channels).
		if !wc.pendingConsume(ack.EnvelopeUUID, ack.SenderUserID, ack.SenderDeviceID) {
			s.log.Debugw("chat ws: chatEnvelopeAck rejected (unknown or mismatched)",
				"envelopeUuid", ack.EnvelopeUUID,
				"claimed_sender", ack.SenderUserID+"/"+ack.SenderDeviceID)
			return
		}
		// Signal the sender. SignalDelivered short-circuits to local
		// inflight if the sender is on this node; else publishes a
		// MeshKindAck on the sender's envelope topic (cross-node).
		s.disp.SignalDelivered(ctx, claims.Issuer, ack.SenderUserID, ack.SenderDeviceID, ack.EnvelopeUUID)

	default:
		s.log.Debugw("chat ws: unknown frame kind", "kind", env.Kind)
	}
}

func (s *Service) validateSend(req *chatSendIn) error {
	if req.ToUserID == "" {
		return errors.New("toUserId required")
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
			return errors.New("target.deviceId required")
		}
		if t.EnvelopeUUID == "" {
			return errors.New("target.envelopeUuid required")
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
		return fmt.Errorf("invalid msgType: %q", req.MsgType)
	}
	return nil
}
