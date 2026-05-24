package chat

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/webhook"
)

// Per-target outbound send pipeline. Lives behind /chat/ws's chatSend handler.
//
// Live-delivery uses ONLY long-lived per-(user, device) topics — no transient
// per-envelope topics. Acknowledgements come back on the sender's own envelope
// topic, multiplexed via the meshMessage `kind` discriminator. Mirrors the
// directed-message pattern in pkg/p2p/room_communicator.go.

// meshMessage is the unified payload published on per-(user, device) envelope
// topics. One topic per device; `kind` discriminates intent.
type meshMessage struct {
	Kind            string `json:"kind"`
	EnvelopeUUID    string `json:"envelopeUuid,omitempty"`
	SenderUserID    string `json:"senderUserId,omitempty"`
	SenderDeviceID  string `json:"senderDeviceId,omitempty"`
	Ciphertext      string `json:"ciphertext,omitempty"`
	MsgType         string `json:"msgType,omitempty"`
	QueryID         string `json:"queryId,omitempty"`
}

// Discriminator values carried in meshMessage.Kind. Single-word lowercase
// where natural; lower-camelCase where compound.
const (
	MeshKindEnvelope      = "envelope"
	MeshKindAck           = "ack"
	MeshKindPresenceReply = "presenceReply"
)

// SendStatus is the per-target outcome reported back to the sending client
// in a chatSendResult frame.
type SendStatus string

const (
	StatusLive    SendStatus = "live"
	StatusStored  SendStatus = "stored"
	StatusDropped SendStatus = "dropped"
	StatusError   SendStatus = "error"
)

// SendTarget is one entry of an inbound chatSend frame's targets[] (after
// WS-frame JSON parse). Explicit JSON tags are required because the wire
// field names differ from the Go field names.
type SendTarget struct {
	DeviceID     string `json:"deviceId"`
	Ciphertext   string `json:"ciphertext"` // base64-encoded; passed through opaque
	EnvelopeUUID string `json:"envelopeUuid"`
}

// SendResult is per-target. Fed into a chatSendResult frame.
type SendResult struct {
	EnvelopeUUID string     `json:"envelopeUuid"`
	Status       SendStatus `json:"status"`
	Err          string     `json:"error,omitempty"`
}

// Dispatcher owns the outbound send pipeline. One per node.
type Dispatcher struct {
	db              pubsubAPI
	presence        *PresenceTracker
	notifier        webhook.Notifier
	log             logger.Logger
	fallbackTimeout time.Duration
	queryTimeout    time.Duration

	// inflight maps envelopeUuid -> channel signalled when an ACK arrives on
	// the sender's envelope topic. Bounded by the number of in-flight sends
	// on this node.
	inflightMu sync.Mutex
	inflight   map[string]chan struct{}
}

// pubsubRetryInterval is how often a non-acknowledged send republishes on
// the recipient's envelope topic within the fallbackTimeout window. Catches
// the case where the recipient was briefly disconnected when the initial
// publish landed and has since reconnected (its new subscription will see
// the next retry). 500ms × ~4 retries within the default 2s timeout.
const pubsubRetryInterval = 500 * time.Millisecond

// postWebhookPublishTimeout bounds the one final publish after a successful
// webhook POST. Catches the race where the recipient reconnected between
// the last retry and webhook completion — the live publish hits B's new
// subscription before B's drainPending /envelopes/pending runs (or the
// SDK's pre-decrypt dedup drops it if drainPending got there first).
const postWebhookPublishTimeout = 1 * time.Second

// NewDispatcher wires the dispatcher. notifier is the existing webhook.Notifier
// (signs with the node's wallet) used for offline-fallback POSTs.
// `db` accepts the pubsub.DB concrete type via the pubsubAPI interface so
// tests can inject an in-memory fake.
func NewDispatcher(
	db pubsubAPI,
	presence *PresenceTracker,
	notifier webhook.Notifier,
	fallbackTimeout, queryTimeout time.Duration,
) *Dispatcher {
	return &Dispatcher{
		db:              db,
		presence:        presence,
		notifier:        notifier,
		log:             logger.GetLogger(),
		fallbackTimeout: fallbackTimeout,
		queryTimeout:    queryTimeout,
		inflight:        make(map[string]chan struct{}),
	}
}

// SendOne handles one target's full lifecycle: deliver-or-publish + wait
// for client ack, retrying the publish every pubsubRetryInterval to catch
// recipients that reconnect mid-flow. On timeout: ephemeral=drop;
// non-ephemeral=webhook-fallback + one post-webhook publish.
//
// Live (`StatusLive`) means the recipient SDK explicitly acked via a
// chatEnvelopeAck frame — not "the node's WS write returned nil." See
// /Users/vf/x402/tasks/chat-client-ack.md for the full semantics.
func (d *Dispatcher) SendOne(
	ctx context.Context,
	apiKey string,
	senderUserID, senderDeviceID string,
	recipientUserID string,
	target SendTarget,
	msgType string,
	ephemeral bool,
	chatWebhookURL string,
) SendResult {
	if target.EnvelopeUUID == "" {
		return SendResult{EnvelopeUUID: target.EnvelopeUUID, Status: StatusError, Err: "missing envelopeUuid"}
	}

	msg := meshMessage{
		Kind:           MeshKindEnvelope,
		EnvelopeUUID:   target.EnvelopeUUID,
		SenderUserID:   senderUserID,
		SenderDeviceID: senderDeviceID,
		Ciphertext:     target.Ciphertext,
		MsgType:        msgType,
	}
	envTopic := EnvelopeTopic(apiKey, recipientUserID, target.DeviceID)

	// Ephemeral fast-path. Typing indicators (the only producer today —
	// see TypingManager) are throttled to ≤1/3s sender-side and have a
	// receiver-side visual TTL; a lost wire delivery just means the
	// indicator doesn't show for a few seconds until the next refresh.
	// Retrying for the full 2s deadline parks the receiving wsConn's
	// reader behind the SendAll handleFrame path (see service.go's
	// per-wsConn send pool) and produces the retry storms observed on
	// rapid bursts. Deliver once and report StatusDropped — the SDK's
	// status tracker treats StatusDropped as a no-op for ephemerals.
	if ephemeral {
		d.tryDeliverOrPublish(ctx, apiKey, recipientUserID, target.DeviceID, envTopic, msg)
		return SendResult{EnvelopeUUID: target.EnvelopeUUID, Status: StatusDropped}
	}

	// Register the in-flight ACK channel BEFORE any delivery attempt — so
	// an ACK that arrives between Publish and Select doesn't get dropped.
	ackCh := d.registerInflight(target.EnvelopeUUID)
	defer d.unregisterInflight(target.EnvelopeUUID)

	// Initial delivery attempt (same-node fast path or gossipsub publish).
	d.tryDeliverOrPublish(ctx, apiKey, recipientUserID, target.DeviceID, envTopic, msg)

	// Wait for the recipient's client-device ack, retrying the delivery
	// every pubsubRetryInterval within the fallbackTimeout window. Retries
	// catch recipients that reconnect mid-flow — their new subscription
	// will see the next retry.
	ticker := time.NewTicker(pubsubRetryInterval)
	defer ticker.Stop()
	deadline := time.After(d.fallbackTimeout)

waitLoop:
	for {
		select {
		case <-ackCh:
			return SendResult{EnvelopeUUID: target.EnvelopeUUID, Status: StatusLive}
		case <-ticker.C:
			d.tryDeliverOrPublish(ctx, apiKey, recipientUserID, target.DeviceID, envTopic, msg)
		case <-deadline:
			break waitLoop
		case <-ctx.Done():
			return SendResult{EnvelopeUUID: target.EnvelopeUUID, Status: StatusError, Err: ctx.Err().Error()}
		}
	}

	// Decide push: any other device of the recipient live anywhere on the mesh?
	push := !d.presence.QueryAnyLive(ctx, apiKey, senderUserID, senderDeviceID, recipientUserID, d.queryTimeout)

	body := fallbackBody{
		EnvelopeUUID:      target.EnvelopeUUID,
		RecipientUserID:   recipientUserID,
		RecipientDeviceID: target.DeviceID,
		SenderUserID:      senderUserID,
		SenderDeviceID:    senderDeviceID,
		Ciphertext:        target.Ciphertext,
		MsgType:           msgType,
		Push:              push,
	}
	if err := d.notifier.Notify(ctx, body, chatWebhookURL); err != nil {
		return SendResult{EnvelopeUUID: target.EnvelopeUUID, Status: StatusError, Err: fmt.Sprintf("fallback POST: %v", err)}
	}

	// Post-webhook publish: catches the recipient-reconnects-after-webhook
	// race. Best-effort — failure is fine because the webhook already
	// durably stored the envelope and B will drain on next reconnect. The
	// SDK's pre-decrypt dedup ensures B processes each envelopeUuid at
	// most once even if it lands via both live WS (this publish) and
	// /envelopes/pending (next reconnect's drain).
	pubCtx, cancel := context.WithTimeout(ctx, postWebhookPublishTimeout)
	if _, err := d.db.Publish(pubCtx, envTopic, msg); err != nil {
		d.log.Debugw("post-webhook publish failed (non-fatal)", "err", err, "envelopeUuid", target.EnvelopeUUID)
	}
	cancel()
	return SendResult{EnvelopeUUID: target.EnvelopeUUID, Status: StatusStored}
}

// tryDeliverOrPublish attempts to deliver `msg` to (recipientUserID,
// deviceID) via the same-node fast path; on cache miss (or local write
// failure) it publishes on the gossipsub envelope topic so any node
// hosting the recipient sees it.
//
// Note on the (delivered=true, writeOK=false) case: we skip the gossipsub
// publish even though delivery clearly didn't reach the client. Two
// reasons: (a) the pubsub library filters self-published messages at
// the listener level, so a publish from this node wouldn't reach our
// own local subscriber anyway; (b) if the local WS recovers before the
// next retry tick, DeliverLocal will succeed then; if it doesn't recover,
// the WS reader's UnregisterDevice will fire and the NEXT retry tick
// (or the post-webhook publish) will fall through to the publish branch.
func (d *Dispatcher) tryDeliverOrPublish(
	ctx context.Context,
	apiKey, recipientUserID, deviceID, envTopic string,
	msg meshMessage,
) {
	delivered, writeOK := d.presence.DeliverLocal(apiKey, recipientUserID, deviceID, msg)
	if delivered && writeOK {
		// Local write succeeded; await the client's chatEnvelopeAck on the
		// inflight channel.
		return
	}
	if delivered && !writeOK {
		// Local write failed (dead WS). Don't publish — same-node pubsub
		// is filtered. Wait for the WS to recover or unregister.
		return
	}
	// !delivered: recipient is not local. Publish on gossipsub so any
	// other node hosting the recipient sees it. Failure here is non-fatal
	// — the retry tick will try again.
	if _, err := d.db.Publish(ctx, envTopic, msg); err != nil {
		d.log.Debugw("publish failed (will retry)", "err", err, "envelopeUuid", msg.EnvelopeUUID)
	}
}

// fallbackBody is the JSON body POSTed to the sender's chatWebhookUrl when
// a target has no live subscriber within fallback_timeout. Receivers must
// verify the body's SHA-256 against the webhook JWT's `sha256` claim.
type fallbackBody struct {
	EnvelopeUUID      string `json:"envelopeUuid"`
	RecipientUserID   string `json:"recipientUserId"`
	RecipientDeviceID string `json:"recipientDeviceId"`
	SenderUserID      string `json:"senderUserId"`
	SenderDeviceID    string `json:"senderDeviceId"`
	Ciphertext        string `json:"ciphertext"`
	MsgType           string `json:"msgType"`
	Push              bool   `json:"push"`
}

// SendAll fans a parsed chatSend frame out across targets in parallel and
// returns per-target results in input order.
func (d *Dispatcher) SendAll(
	ctx context.Context,
	apiKey, senderUserID, senderDeviceID, recipientUserID string,
	targets []SendTarget,
	msgType string,
	ephemeral bool,
	chatWebhookURL string,
) []SendResult {
	results := make([]SendResult, len(targets))
	var wg sync.WaitGroup
	for i := range targets {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer recoverHandler(d.log, "SendOne")
			results[i] = d.SendOne(ctx, apiKey, senderUserID, senderDeviceID, recipientUserID,
				targets[i], msgType, ephemeral, chatWebhookURL)
		}(i)
	}
	wg.Wait()
	return results
}

// SignalAck is called by the envelope-topic handler in service.go when an ACK
// arrives on the sender's envelope topic. Wakes up the SendOne goroutine.
func (d *Dispatcher) SignalAck(envelopeUUID string) {
	d.inflightMu.Lock()
	ch, ok := d.inflight[envelopeUUID]
	d.inflightMu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- struct{}{}:
	default: // already signalled; drop
	}
}

// SignalDelivered is called from service.go when a client sends back a
// chatEnvelopeAck for an envelope the node delivered to its WS. Routes
// to the sender's inflight channel:
//   - same-node: signal the local channel directly (no mesh round-trip)
//   - cross-node: publish a MeshKindAck on the sender's envelope topic;
//     the sender's onEnvelope handler picks it up and calls SignalAck
//
// Replaces the previous "ACK on writeJSON success" trigger in
// service.go's onEnvelope — that path declared StatusLive whenever the
// node's WS write returned nil, which is optimistic at the TCP layer
// and races with the client tab closing mid-frame. See
// /Users/vf/x402/tasks/chat-client-ack.md.
func (d *Dispatcher) SignalDelivered(
	ctx context.Context,
	apiKey, senderUserID, senderDeviceID, envelopeUUID string,
) {
	if envelopeUUID == "" {
		return
	}
	// Same-node short-circuit: if the inflight channel is here, the
	// sender is on this node — wake them directly.
	d.inflightMu.Lock()
	ch, hasLocal := d.inflight[envelopeUUID]
	d.inflightMu.Unlock()
	if hasLocal {
		select {
		case ch <- struct{}{}:
		default: // already signalled (e.g. retry-publish caused two acks); drop
		}
		return
	}
	// Cross-node: publish on the sender's envelope topic. The sender's
	// node has a subscription there (its own /chat/ws connection) and
	// will deliver into SignalAck.
	d.PublishAck(ctx, apiKey, senderUserID, senderDeviceID, envelopeUUID)
}

// PublishAck publishes a MeshKindAck on the SENDER's envelope topic, so
// the sender's node (which subscribes there for its own /chat/ws conn)
// can wake SendOne's inflight channel via SignalAck.
//
// As of 2026-05-19 this is no longer called from onEnvelope (the
// optimistic "WS write succeeded → ack" path); it's called only from
// SignalDelivered's cross-node branch. Exported so SignalDelivered can
// use it without a method-internal helper.
func (d *Dispatcher) PublishAck(
	ctx context.Context,
	apiKey, senderUserID, senderDeviceID, envelopeUUID string,
) {
	if senderUserID == "" || senderDeviceID == "" {
		return
	}
	topic := EnvelopeTopic(apiKey, senderUserID, senderDeviceID)
	msg := meshMessage{
		Kind:         MeshKindAck,
		EnvelopeUUID: envelopeUUID,
	}
	if _, err := d.db.Publish(ctx, topic, msg); err != nil {
		d.log.Debugw("publish envelope ack failed", "err", err, "envelopeUuid", envelopeUUID)
	}
}

func (d *Dispatcher) registerInflight(envelopeUUID string) chan struct{} {
	ch := make(chan struct{}, 1)
	d.inflightMu.Lock()
	d.inflight[envelopeUUID] = ch
	d.inflightMu.Unlock()
	return ch
}

func (d *Dispatcher) unregisterInflight(envelopeUUID string) {
	d.inflightMu.Lock()
	delete(d.inflight, envelopeUUID)
	d.inflightMu.Unlock()
}

// recoverHandler wraps a goroutine boundary with panic recovery so a bad
// payload or unexpected nil doesn't kill the parent. Existing pubsub-using
// packages (e.g. pkg/p2p/room_communicator.go) lack recovery on handler
// goroutines — chat consciously adds it.
func recoverHandler(log logger.Logger, where string) {
	if r := recover(); r != nil {
		log.Errorw("chat handler panic recovered", fmt.Errorf("%v", r), "where", where)
	}
}
