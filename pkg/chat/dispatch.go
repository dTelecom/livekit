package chat

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dTelecom/p2p-database/pubsub"
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
	db              *pubsub.DB
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

// NewDispatcher wires the dispatcher. notifier is the existing webhook.Notifier
// (signs with the node's wallet) used for offline-fallback POSTs.
func NewDispatcher(
	db *pubsub.DB,
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

// SendOne handles one target's full lifecycle: same-node fast path, then
// publish + wait-for-ACK, then drop (ephemeral) or fallback-POST.
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

	// Same-node fast path: if the recipient device's WS is hosted on this
	// node, deliver directly. The pubsub library filters self-published
	// messages at the listener level, so a gossipsub publish would never
	// land on a same-node subscriber. Pattern mirrors traffic_manager.go and
	// node_provider.go.
	if d.presence.DeliverLocal(apiKey, recipientUserID, target.DeviceID, msg) {
		return SendResult{EnvelopeUUID: target.EnvelopeUUID, Status: StatusLive}
	}

	// Register the in-flight ACK channel BEFORE publishing — so an ACK that
	// arrives between Publish and Select doesn't get dropped on the floor.
	ackCh := d.registerInflight(target.EnvelopeUUID)
	defer d.unregisterInflight(target.EnvelopeUUID)

	envTopic := EnvelopeTopic(apiKey, recipientUserID, target.DeviceID)
	if _, err := d.db.Publish(ctx, envTopic, msg); err != nil {
		return SendResult{EnvelopeUUID: target.EnvelopeUUID, Status: StatusError, Err: fmt.Sprintf("publish: %v", err)}
	}

	select {
	case <-ackCh:
		return SendResult{EnvelopeUUID: target.EnvelopeUUID, Status: StatusLive}
	case <-time.After(d.fallbackTimeout):
		// No ACK; recipient device has no live WS anywhere on the mesh.
	case <-ctx.Done():
		return SendResult{EnvelopeUUID: target.EnvelopeUUID, Status: StatusError, Err: ctx.Err().Error()}
	}

	if ephemeral {
		return SendResult{EnvelopeUUID: target.EnvelopeUUID, Status: StatusDropped}
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
	return SendResult{EnvelopeUUID: target.EnvelopeUUID, Status: StatusStored}
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

// PublishAck is called by the recipient-side envelope handler after a WS
// delivery succeeds. Publishes the ACK on the SENDER's envelope topic so the
// sender's existing subscription receives it — no per-envelope topic.
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
