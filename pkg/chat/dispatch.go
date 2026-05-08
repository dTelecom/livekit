package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	p2p_common "github.com/dTelecom/p2p-database/common"
	"github.com/dTelecom/p2p-database/pubsub"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/webhook"
)

// Per-target outbound send orchestration. Owns the publish-to-envelope-topic +
// wait-for-ACK + decide-push + fallback-POST flow described in plan §5.5.
//
// Recipient side (the inbound envelope-topic handler) lives in service.go: it
// delivers the envelope to the local WS and then publishes an ACK on the
// per-envelope ACK topic.

// envelopePayload is the wire shape published on chat:envelope:<...> topics.
// Distinct from (but related to) the wire-contract ChatSend frame and the
// internal /api/chat/envelopes POST body — see chat-wire-contract.md.
type envelopePayload struct {
	EnvelopeUUID    string `json:"envelope_uuid"`
	SenderUserID    string `json:"sender_user_id"`
	SenderDeviceID  string `json:"sender_device_id"`
	Ciphertext      string `json:"ciphertext"` // base64 (caller passes through; we don't decode)
	MsgType         string `json:"msg_type"`   // "prekey" | "normal"
}

// envelopeAck is published on chat:envelope-ack:<envelope_uuid> by the
// recipient's node after WS delivery succeeds.
type envelopeAck struct {
	EnvelopeUUID string `json:"envelope_uuid"`
}

func envelopeAckTopic(envelopeUUID string) string {
	return "chat:envelope-ack:" + envelopeUUID
}

// SendStatus is the per-target outcome reported back to the sending client
// in a chat_send_result frame.
type SendStatus string

const (
	StatusLive    SendStatus = "live"
	StatusStored  SendStatus = "stored"
	StatusDropped SendStatus = "dropped"
	StatusError   SendStatus = "error"
)

// SendTarget is one entry of a ChatSend frame's targets[] (after WS-frame
// parsing). JSON tags MUST match chat-wire-contract.md §3 — the wire format
// uses snake_case, so the unmarshal needs explicit tags.
type SendTarget struct {
	DeviceID     string `json:"device_id"`
	Ciphertext   string `json:"ciphertext"` // base64-encoded; passed through opaque
	EnvelopeUUID string `json:"envelope_uuid"`
}

// SendResult is per-target. Fed into a chat_send_result frame.
type SendResult struct {
	EnvelopeUUID string     `json:"envelope_uuid"`
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
}

// NewDispatcher wires up the dispatcher. notifier is the existing
// webhook.Notifier (signs with the node's wallet). presence is shared with
// service.go's WS handler.
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
	}
}

// SendOne handles one target's full send lifecycle: publish + ACK wait +
// (drop | fallback POST). Returns the result the WS handler should put in the
// chat_send_result frame.
func (d *Dispatcher) SendOne(
	ctx context.Context,
	apiKey string,
	senderUserID, senderDeviceID string,
	recipientUserID string,
	target SendTarget,
	msgType string, // "prekey" | "normal"
	ephemeral bool,
	chatWebhookURL string, // from sender's chat-token claim
) SendResult {
	if target.EnvelopeUUID == "" {
		return SendResult{EnvelopeUUID: target.EnvelopeUUID, Status: StatusError, Err: "missing envelope_uuid"}
	}

	// Subscribe to the ACK topic BEFORE publishing — otherwise a fast ACK
	// might arrive before the subscription completes and be lost.
	ackTopic := envelopeAckTopic(target.EnvelopeUUID)
	ackCh := make(chan struct{}, 1)
	subCtx, cancelSub := context.WithCancel(ctx)
	defer cancelSub()

	if err := d.db.Subscribe(subCtx, ackTopic, makeAckHandler(target.EnvelopeUUID, ackCh)); err != nil {
		return SendResult{EnvelopeUUID: target.EnvelopeUUID, Status: StatusError, Err: fmt.Sprintf("ack subscribe: %v", err)}
	}
	defer func() {
		uctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = d.db.Unsubscribe(uctx, ackTopic)
	}()

	envTopic := EnvelopeTopic(apiKey, recipientUserID, target.DeviceID)
	payload := envelopePayload{
		EnvelopeUUID:   target.EnvelopeUUID,
		SenderUserID:   senderUserID,
		SenderDeviceID: senderDeviceID,
		Ciphertext:     target.Ciphertext,
		MsgType:        msgType,
	}
	if _, err := d.db.Publish(ctx, envTopic, payload); err != nil {
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

	// Decide push: any other device of this user live anywhere?
	push := !d.presence.QueryAnyLive(ctx, apiKey, recipientUserID, d.queryTimeout)

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

// fallbackBody is the JSON body POSTed to the sender's chat_webhook_url.
// Must match chat-wire-contract.md §2.9 exactly.
type fallbackBody struct {
	EnvelopeUUID      string `json:"envelope_uuid"`
	RecipientUserID   string `json:"recipient_user_id"`
	RecipientDeviceID string `json:"recipient_device_id"`
	SenderUserID      string `json:"sender_user_id"`
	SenderDeviceID    string `json:"sender_device_id"`
	Ciphertext        string `json:"ciphertext"`
	MsgType           string `json:"msg_type"`
	Push              bool   `json:"push"`
}

// SendAll fans a parsed ChatSend frame out across targets, in parallel. Returns
// per-target results in input order.
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
			results[i] = d.SendOne(ctx, apiKey, senderUserID, senderDeviceID, recipientUserID,
				targets[i], msgType, ephemeral, chatWebhookURL)
		}(i)
	}
	wg.Wait()
	return results
}

func makeAckHandler(envelopeUUID string, hit chan<- struct{}) func(p2p_common.Event) {
	return func(ev p2p_common.Event) {
		raw, err := json.Marshal(ev.Message)
		if err != nil {
			return
		}
		var a envelopeAck
		if err := json.Unmarshal(raw, &a); err != nil {
			return
		}
		if a.EnvelopeUUID != envelopeUUID {
			return
		}
		select {
		case hit <- struct{}{}:
		default:
		}
	}
}

// PublishAck is called by the recipient-side envelope handler after WS
// delivery succeeds, to tell the sender's node "live ACK." Exposed on the
// dispatcher so service.go can call it.
func (d *Dispatcher) PublishAck(ctx context.Context, envelopeUUID string) {
	a := envelopeAck{EnvelopeUUID: envelopeUUID}
	if _, err := d.db.Publish(ctx, envelopeAckTopic(envelopeUUID), a); err != nil {
		d.log.Debugw("publish envelope ack failed", "err", err, "envelope_uuid", envelopeUUID)
	}
}
