package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	p2p_common "github.com/dTelecom/p2p-database/common"
	"github.com/dTelecom/p2p-database/pubsub"
	"github.com/google/uuid"
	"github.com/livekit/protocol/logger"
)

// EnvelopeHandler is called when a published envelope arrives for a locally
// hosted device. The handler runs on a goroutine the caller (service.go) owns.
// It receives the JSON-encoded envelope payload (the full ChatSend target
// re-emitted on the topic by the sender's node).
type EnvelopeHandler func(payload []byte)

// PresenceTracker is the node-internal answer to two questions:
//   1. "Do I host the live WS for (apiKey, userID, deviceID)?" — answered via
//      the localDevices map, used by the envelope-topic subscription handler.
//   2. "Does any node on the mesh host any live device of (apiKey, userID)?"
//      — answered lazily at fallback-decision time via a one-shot pubsub
//      request/reply on a per-user query topic.
//
// No HTTP traffic to any backend. No periodic heartbeats. Steady-state idle
// generates zero presence traffic.
type PresenceTracker struct {
	db        *pubsub.DB
	log       logger.Logger
	queryTTL  time.Duration

	mu sync.RWMutex
	// per-device live registration
	localDevices map[ChatUserKey]*localDevice
	// per-(apiKey,userID) set of hosted device IDs; powers query-topic subscription
	// reference counting and the local "yes I host any device of this user" reply.
	hostedUsers map[string]map[string]struct{}
	// in-flight queries the asker is waiting on; key = queryID; value receives replies.
	inflight map[string]chan struct{}
}

type localDevice struct {
	apiKey   string
	userID   string
	deviceID string
	onEnv    EnvelopeHandler
	envTopic string
}

// NewPresenceTracker wires the tracker against the shared p2p pubsub DB.
// `defaultQueryTTL` is the asker-side aggregation window for QueryAnyLive
// (typically conf.Chat.UserPresenceQueryTimeout, ~500ms).
func NewPresenceTracker(db *pubsub.DB, defaultQueryTTL time.Duration) *PresenceTracker {
	return &PresenceTracker{
		db:           db,
		log:          logger.GetLogger(),
		queryTTL:     defaultQueryTTL,
		localDevices: make(map[ChatUserKey]*localDevice),
		hostedUsers:  make(map[string]map[string]struct{}),
		inflight:     make(map[string]chan struct{}),
	}
}

// RegisterDevice records that this node holds a live WS for (apiKey, userID,
// deviceID). Subscribes to that device's envelope topic; if this is the first
// device of (apiKey, userID), also subscribes to the per-user query topic so
// we'll reply when other nodes ask.
func (p *PresenceTracker) RegisterDevice(
	ctx context.Context,
	apiKey, userID, deviceID string,
	onEnv EnvelopeHandler,
) error {
	if onEnv == nil {
		return errors.New("RegisterDevice: onEnv is nil")
	}
	envTopic := EnvelopeTopic(apiKey, userID, deviceID)
	dev := &localDevice{
		apiKey:   apiKey,
		userID:   userID,
		deviceID: deviceID,
		onEnv:    onEnv,
		envTopic: envTopic,
	}

	p.mu.Lock()
	key := EncodeUserKey(apiKey, userID, deviceID)
	if _, exists := p.localDevices[key]; exists {
		p.mu.Unlock()
		return fmt.Errorf("device already registered: %s/%s", userID, deviceID)
	}
	p.localDevices[key] = dev

	userTenantKey := EncodeTenantUserKey(apiKey, userID)
	devices := p.hostedUsers[userTenantKey]
	firstDeviceForUser := devices == nil
	if firstDeviceForUser {
		devices = make(map[string]struct{})
		p.hostedUsers[userTenantKey] = devices
	}
	devices[deviceID] = struct{}{}
	p.mu.Unlock()

	if err := p.db.Subscribe(ctx, envTopic, p.makeEnvelopeHandler(dev)); err != nil {
		// Roll back local state.
		p.unregisterLocked(apiKey, userID, deviceID)
		return fmt.Errorf("subscribe envelope topic: %w", err)
	}

	if firstDeviceForUser {
		queryTopic := UserPresenceQueryTopic(apiKey, userID)
		if err := p.db.Subscribe(ctx, queryTopic, p.makeQueryHandler(apiKey, userID)); err != nil {
			// Best-effort cleanup of the env subscription we just made; in practice
			// pubsub.Subscribe failures here would also fail above, so this branch
			// is defensive only.
			_ = p.db.Unsubscribe(ctx, envTopic)
			p.unregisterLocked(apiKey, userID, deviceID)
			return fmt.Errorf("subscribe user-presence-query topic: %w", err)
		}
	}

	return nil
}

// UnregisterDevice removes the device's live state and unsubscribes from its
// envelope topic. If this was the last device of (apiKey, userID), also
// unsubscribes from the per-user query topic.
func (p *PresenceTracker) UnregisterDevice(ctx context.Context, apiKey, userID, deviceID string) {
	p.mu.Lock()
	dev, lastDeviceForUser := p.unregisterLocked(apiKey, userID, deviceID)
	p.mu.Unlock()

	if dev != nil {
		if err := p.db.Unsubscribe(ctx, dev.envTopic); err != nil {
			p.log.Debugw("unsubscribe envelope topic failed (non-fatal)", "topic", dev.envTopic, "err", err)
		}
	}
	if lastDeviceForUser {
		topic := UserPresenceQueryTopic(apiKey, userID)
		if err := p.db.Unsubscribe(ctx, topic); err != nil {
			p.log.Debugw("unsubscribe user-presence-query topic failed (non-fatal)", "topic", topic, "err", err)
		}
	}
}

// hostsAnyDeviceOf reports whether this node holds at least one live device
// for (apiKey, userID). Used by the per-user-query-topic handler to decide
// whether to reply "yes" to an inbound query.
func (p *PresenceTracker) hostsAnyDeviceOf(apiKey, userID string) bool {
	key := EncodeTenantUserKey(apiKey, userID)
	p.mu.RLock()
	defer p.mu.RUnlock()
	devices := p.hostedUsers[key]
	return len(devices) > 0
}

// IsLocallyHosted reports whether this specific (apiKey, userID, deviceID) has
// a live WS on this node.
func (p *PresenceTracker) IsLocallyHosted(apiKey, userID, deviceID string) bool {
	key := EncodeUserKey(apiKey, userID, deviceID)
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.localDevices[key]
	return ok
}

// QueryAnyLive performs the lazy mesh user-presence query: "does any node
// host any live device of (apiKey, userID)?" Returns true if at least one
// reply arrives within timeout (or queryTTL if timeout==0).
//
// Short-circuits to true if we host a device locally (no network round-trip
// needed). Used at offline-fallback decision time to compute the `push: bool`
// flag on the webhook POST.
func (p *PresenceTracker) QueryAnyLive(ctx context.Context, apiKey, userID string, timeout time.Duration) bool {
	if p.hostsAnyDeviceOf(apiKey, userID) {
		return true
	}
	if timeout <= 0 {
		timeout = p.queryTTL
	}

	queryID := uuid.NewString()
	replyTopic := userPresenceReplyTopic(queryID)
	queryTopic := UserPresenceQueryTopic(apiKey, userID)

	// Buffered: we only need to know "any reply arrived." Buffered avoids
	// dropping the first reply if it lands before we start the wait below.
	hit := make(chan struct{}, 1)

	p.mu.Lock()
	p.inflight[queryID] = hit
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.inflight, queryID)
		p.mu.Unlock()
	}()

	subCtx, cancelSub := context.WithCancel(ctx)
	defer cancelSub()

	if err := p.db.Subscribe(subCtx, replyTopic, p.makeReplyHandler(queryID)); err != nil {
		p.log.Debugw("query subscribe failed", "err", err)
		return false
	}
	defer func() {
		// Best-effort unsubscribe with a fresh context (subCtx may be cancelled).
		uctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = p.db.Unsubscribe(uctx, replyTopic)
	}()

	q := userPresenceQuery{
		Type:       "query",
		QueryID:    queryID,
		ReplyTopic: replyTopic,
	}
	if _, err := p.db.Publish(ctx, queryTopic, q); err != nil {
		p.log.Debugw("query publish failed", "err", err)
		return false
	}

	select {
	case <-hit:
		return true
	case <-time.After(timeout):
		return false
	case <-ctx.Done():
		return false
	}
}

// ── pubsub message shapes ────────────────────────────────────────────────────

type userPresenceQuery struct {
	Type       string `json:"type"`        // "query"
	QueryID    string `json:"query_id"`
	ReplyTopic string `json:"reply_topic"` // unique per query
}

type userPresenceReply struct {
	Type    string `json:"type"`     // "reply"
	QueryID string `json:"query_id"`
}

func userPresenceReplyTopic(queryID string) string {
	return "chat:user-presence-reply:" + queryID
}

// ── internal handlers ────────────────────────────────────────────────────────

func (p *PresenceTracker) makeEnvelopeHandler(dev *localDevice) func(p2p_common.Event) {
	return func(ev p2p_common.Event) {
		// Skip self-published echoes. Sender's node publishes; a different node
		// hosting the recipient subscribes. If the same node hosts both sender
		// and recipient (rare in production), we'd want to deliver — so the
		// self-echo skip is keyed on "did THIS node publish to THIS topic for
		// somebody else's recipient?" which we can't disambiguate from the
		// peer ID alone. For v1 we accept potential self-echoes; the SDK
		// dedupes on envelope_uuid at the recipient side anyway.
		raw, err := json.Marshal(ev.Message)
		if err != nil {
			p.log.Errorw("envelope handler: marshal", err)
			return
		}
		dev.onEnv(raw)
	}
}

func (p *PresenceTracker) makeQueryHandler(apiKey, userID string) func(p2p_common.Event) {
	myPeerID := p.db.GetHost().ID().String()
	return func(ev p2p_common.Event) {
		// Don't reply to our own queries.
		if ev.FromPeerId == myPeerID {
			return
		}
		raw, err := json.Marshal(ev.Message)
		if err != nil {
			return
		}
		var q userPresenceQuery
		if err := json.Unmarshal(raw, &q); err != nil {
			return
		}
		if q.Type != "query" || q.QueryID == "" || q.ReplyTopic == "" {
			return
		}
		// Only reply if we currently host any device of (apiKey, userID).
		// Re-check under lock — registrations may have changed since we
		// subscribed.
		if !p.hostsAnyDeviceOf(apiKey, userID) {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		reply := userPresenceReply{Type: "reply", QueryID: q.QueryID}
		if _, err := p.db.Publish(ctx, q.ReplyTopic, reply); err != nil {
			p.log.Debugw("user-presence reply publish failed", "err", err)
		}
	}
}

func (p *PresenceTracker) makeReplyHandler(queryID string) func(p2p_common.Event) {
	return func(ev p2p_common.Event) {
		raw, err := json.Marshal(ev.Message)
		if err != nil {
			return
		}
		var r userPresenceReply
		if err := json.Unmarshal(raw, &r); err != nil {
			return
		}
		if r.Type != "reply" || r.QueryID != queryID {
			return
		}
		p.mu.RLock()
		hit := p.inflight[queryID]
		p.mu.RUnlock()
		if hit == nil {
			return
		}
		select {
		case hit <- struct{}{}:
		default:
			// already signalled
		}
	}
}

// unregisterLocked removes a device from the local maps and reports whether
// this was the last device of (apiKey, userID). Caller must hold p.mu.
func (p *PresenceTracker) unregisterLocked(apiKey, userID, deviceID string) (*localDevice, bool) {
	key := EncodeUserKey(apiKey, userID, deviceID)
	dev := p.localDevices[key]
	if dev == nil {
		return nil, false
	}
	delete(p.localDevices, key)

	userKey := EncodeTenantUserKey(apiKey, userID)
	devices := p.hostedUsers[userKey]
	if devices != nil {
		delete(devices, deviceID)
		if len(devices) == 0 {
			delete(p.hostedUsers, userKey)
			return dev, true
		}
	}
	return dev, false
}
