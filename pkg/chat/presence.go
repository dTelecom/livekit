package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	p2p_common "github.com/dTelecom/p2p-database/common"
	"github.com/google/uuid"
	"github.com/livekit/protocol/logger"
)

// EnvelopeHandler is called when a meshMessage arrives for a locally hosted
// device. The handler runs on a goroutine the caller (service.go) owns. It
// receives the JSON-encoded meshMessage payload and dispatches by `kind`.
// Returns an error if the live-WS write failed — used by DeliverLocal to
// distinguish "no local subscriber" from "subscriber exists but write
// failed," so the dispatcher can pick the right recovery path.
type EnvelopeHandler func(payload []byte) error

// PresenceTracker is the node-internal answer to two questions:
//   1. "Do I host the live WS for (apiKey, userID, deviceID)?"
//   2. "Does any node on the mesh host any live device of (apiKey, userID)?"
//
// (1) is answered locally via the localDevices map. (2) is answered lazily at
// fallback-decision time via a one-shot publish on a per-user query topic;
// replies arrive on the ASKER's envelope topic (multiplexed with chat
// envelopes via meshMessage.Kind), so no per-query reply topic is created.
//
// Steady-state idle generates zero presence traffic.
type PresenceTracker struct {
	db       pubsubAPI
	log      logger.Logger
	queryTTL time.Duration

	mu sync.RWMutex
	// per-device live registration
	localDevices map[ChatUserKey]*localDevice
	// per-(apiKey, userID) set of hosted device IDs; powers query-topic
	// subscription reference counting and the "yes I host any device" reply.
	hostedUsers map[string]map[string]struct{}
	// in-flight presence queries this node is awaiting; key = queryId.
	inflightQueries map[string]chan struct{}
}

type localDevice struct {
	apiKey   string
	userID   string
	deviceID string
	onEnv    EnvelopeHandler
	envTopic string
}

// presenceQueryMessage is the wire shape published on chat:user-presence-query
// topics. The replier publishes its reply on the asker's envelope topic, so we
// need the asker's identity in the query payload.
type presenceQueryMessage struct {
	QueryID       string `json:"queryId"`
	AskerUserID   string `json:"askerUserId"`
	AskerDeviceID string `json:"askerDeviceId"`
}

// NewPresenceTracker wires the tracker against the shared p2p pubsub DB.
// `db` accepts the pubsub.DB concrete type via the pubsubAPI interface
// (tests inject in-memory fakes).
func NewPresenceTracker(db pubsubAPI, defaultQueryTTL time.Duration) *PresenceTracker {
	return &PresenceTracker{
		db:              db,
		log:             logger.GetLogger(),
		queryTTL:        defaultQueryTTL,
		localDevices:    make(map[ChatUserKey]*localDevice),
		hostedUsers:     make(map[string]map[string]struct{}),
		inflightQueries: make(map[string]chan struct{}),
	}
}

// RegisterDevice records that this node holds a live WS for (apiKey, userID,
// deviceID). Subscribes to the device's envelope topic; if this is the first
// device of (apiKey, userID), also subscribes to that user's presence-query
// topic so we'll reply when other nodes ask.
//
// Subscribe is called with context.Background() — the listener lifetime is
// owned by Unsubscribe, not by request contexts. Mirrors room_communicator.go
// and node_provider.go.
func (p *PresenceTracker) RegisterDevice(
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

	if err := p.db.Subscribe(context.Background(), envTopic, p.makeEnvelopeHandler(dev)); err != nil {
		p.unregisterLocalState(apiKey, userID, deviceID)
		return fmt.Errorf("subscribe envelope topic: %w", err)
	}

	if firstDeviceForUser {
		queryTopic := UserPresenceQueryTopic(apiKey, userID)
		if err := p.db.Subscribe(context.Background(), queryTopic, p.makeQueryHandler(apiKey, userID)); err != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = p.db.Unsubscribe(ctx, envTopic)
			cancel()
			p.unregisterLocalState(apiKey, userID, deviceID)
			return fmt.Errorf("subscribe presence-query topic: %w", err)
		}
	}

	return nil
}

// UnregisterDevice removes the device's live state and unsubscribes from its
// envelope topic. If this was the last device of (apiKey, userID), also
// unsubscribes from the per-user presence-query topic.
func (p *PresenceTracker) UnregisterDevice(apiKey, userID, deviceID string) {
	p.mu.Lock()
	dev, lastDeviceForUser := p.unregisterLocalStateLocked(apiKey, userID, deviceID)
	p.mu.Unlock()

	if dev != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := p.db.Unsubscribe(ctx, dev.envTopic); err != nil {
			p.log.Debugw("unsubscribe envelope topic failed (non-fatal)", "topic", dev.envTopic, "err", err)
		}
		cancel()
	}
	if lastDeviceForUser {
		topic := UserPresenceQueryTopic(apiKey, userID)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := p.db.Unsubscribe(ctx, topic); err != nil {
			p.log.Debugw("unsubscribe presence-query topic failed (non-fatal)", "topic", topic, "err", err)
		}
		cancel()
	}
}

// DeliverLocal tries to deliver a meshMessage to a locally-hosted device by
// invoking its EnvelopeHandler synchronously. Returns:
//   - delivered: the device has a live local WS subscription.
//   - writeOK: the WS write succeeded (only meaningful when delivered=true;
//     false on marshal error or writeJSON error).
//
// The dispatcher uses the (delivered, writeOK) tuple to pick the recovery
// path: (true,true) → wait for client ack; (true,false) → don't waste the
// retry tick on a dead-but-not-yet-unregistered local WS, but still keep
// trying (a reconnected WS may register before the next tick); (false,*) →
// publish on gossipsub for cross-node delivery.
//
// Same-node delivery via gossipsub doesn't work because the underlying
// listener filters self-published messages — that's why DeliverLocal is a
// separate code path.
func (p *PresenceTracker) DeliverLocal(apiKey, userID, deviceID string, m meshMessage) (delivered, writeOK bool) {
	key := EncodeUserKey(apiKey, userID, deviceID)
	p.mu.RLock()
	dev, ok := p.localDevices[key]
	p.mu.RUnlock()
	if !ok {
		return false, false
	}
	payload, err := json.Marshal(m)
	if err != nil {
		p.log.Errorw("DeliverLocal: marshal meshMessage", err)
		return true, false
	}
	defer recoverHandler(p.log, "DeliverLocal")
	if err := dev.onEnv(payload); err != nil {
		p.log.Debugw("DeliverLocal: onEnv reported error", "err", err, "user", userID, "device", deviceID)
		return true, false
	}
	return true, true
}

// IsLocallyHosted reports whether this specific (apiKey, userID, deviceID)
// has a live WS on this node.
func (p *PresenceTracker) IsLocallyHosted(apiKey, userID, deviceID string) bool {
	key := EncodeUserKey(apiKey, userID, deviceID)
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.localDevices[key]
	return ok
}

// hostsAnyDeviceOf reports whether this node holds at least one live device
// for (apiKey, userID). Used by the presence-query handler.
func (p *PresenceTracker) hostsAnyDeviceOf(apiKey, userID string) bool {
	key := EncodeTenantUserKey(apiKey, userID)
	p.mu.RLock()
	defer p.mu.RUnlock()
	devices := p.hostedUsers[key]
	return len(devices) > 0
}

// QueryAnyLive performs the lazy mesh user-presence query: "does any node
// host any live device of (apiKey, targetUserID)?" Returns true if at least
// one reply arrives within timeout.
//
// Replies arrive on the ASKER's envelope topic (multiplexed via meshMessage
// kind=presenceReply). The asker is identified by askerUserID/askerDeviceID
// — that's the WS that triggered the query, and its envelope topic is
// already subscribed locally by service.go's serveWS.
//
// Short-circuits to true if we host any device of the target locally — no
// network round-trip needed.
func (p *PresenceTracker) QueryAnyLive(
	ctx context.Context,
	apiKey, askerUserID, askerDeviceID, targetUserID string,
	timeout time.Duration,
) bool {
	if p.hostsAnyDeviceOf(apiKey, targetUserID) {
		return true
	}
	if timeout <= 0 {
		timeout = p.queryTTL
	}

	queryID := uuid.NewString()
	hit := make(chan struct{}, 1)

	p.mu.Lock()
	p.inflightQueries[queryID] = hit
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.inflightQueries, queryID)
		p.mu.Unlock()
	}()

	queryTopic := UserPresenceQueryTopic(apiKey, targetUserID)
	q := presenceQueryMessage{
		QueryID:       queryID,
		AskerUserID:   askerUserID,
		AskerDeviceID: askerDeviceID,
	}
	if _, err := p.db.Publish(ctx, queryTopic, q); err != nil {
		p.log.Debugw("presence query publish failed", "err", err)
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

// SignalQueryReply is called by the envelope-topic handler in service.go
// when a meshMessage with kind=presenceReply arrives. Wakes the QueryAnyLive
// goroutine so it can return true.
func (p *PresenceTracker) SignalQueryReply(queryID string) {
	p.mu.RLock()
	hit := p.inflightQueries[queryID]
	p.mu.RUnlock()
	if hit == nil {
		return
	}
	select {
	case hit <- struct{}{}:
	default:
	}
}

// ── internal handlers ────────────────────────────────────────────────────────

func (p *PresenceTracker) makeEnvelopeHandler(dev *localDevice) func(p2p_common.Event) {
	return func(ev p2p_common.Event) {
		defer recoverHandler(p.log, "presence.envelopeHandler")
		raw, err := json.Marshal(ev.Message)
		if err != nil {
			p.log.Errorw("envelope handler: marshal", err)
			return
		}
		// Cross-node delivery: the writeJSON error is signaled to the sender
		// via lack-of-ack on the inflight channel (not here). Log + drop.
		if err := dev.onEnv(raw); err != nil {
			p.log.Debugw("envelope handler: onEnv reported error", "err", err, "user", dev.userID, "device", dev.deviceID)
		}
	}
}

func (p *PresenceTracker) makeQueryHandler(apiKey, userID string) func(p2p_common.Event) {
	return func(ev p2p_common.Event) {
		defer recoverHandler(p.log, "presence.queryHandler")
		raw, err := json.Marshal(ev.Message)
		if err != nil {
			return
		}
		var q presenceQueryMessage
		if err := json.Unmarshal(raw, &q); err != nil {
			return
		}
		if q.QueryID == "" || q.AskerUserID == "" || q.AskerDeviceID == "" {
			return
		}
		// Re-check under lock — registrations may have changed since subscribe.
		if !p.hostsAnyDeviceOf(apiKey, userID) {
			return
		}
		// Reply on the ASKER's envelope topic; their existing subscription
		// receives it. No per-query reply topic is created.
		replyTopic := EnvelopeTopic(apiKey, q.AskerUserID, q.AskerDeviceID)
		reply := meshMessage{
			Kind:    MeshKindPresenceReply,
			QueryID: q.QueryID,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := p.db.Publish(ctx, replyTopic, reply); err != nil {
			p.log.Debugw("presence reply publish failed", "err", err)
		}
	}
}

// unregisterLocalState removes the device from the local maps without
// touching pubsub subscriptions. Used during error rollback in RegisterDevice.
func (p *PresenceTracker) unregisterLocalState(apiKey, userID, deviceID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.unregisterLocalStateLocked(apiKey, userID, deviceID)
}

// unregisterLocalStateLocked is the locked variant. Returns the removed
// localDevice (or nil) and whether this was the last device for the user.
func (p *PresenceTracker) unregisterLocalStateLocked(apiKey, userID, deviceID string) (*localDevice, bool) {
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
