package chat

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	p2p_common "github.com/dTelecom/p2p-database/common"
)

// ── fakes ───────────────────────────────────────────────────────────────────

// fakePubsub is an in-memory stand-in for *pubsub.DB. Publishes record into
// a per-topic slice the test can inspect; subscribes register a handler
// invoked on every subsequent Publish on that topic (mimicking gossipsub
// fan-out across nodes, MINUS the self-publish filter — tests register
// distinct fakePubsubs per "node" to model the filter naturally).
type fakePubsub struct {
	mu          sync.Mutex
	subscribers map[string][]p2p_common.PubSubHandler
	publishLog  []publishRecord
	publishErr  error // when non-nil, Publish returns this without delivering
}

type publishRecord struct {
	topic string
	value interface{}
}

func newFakePubsub() *fakePubsub {
	return &fakePubsub{subscribers: make(map[string][]p2p_common.PubSubHandler)}
}

func (f *fakePubsub) Publish(_ context.Context, topic string, value interface{}) (p2p_common.Event, error) {
	f.mu.Lock()
	if f.publishErr != nil {
		err := f.publishErr
		f.mu.Unlock()
		return p2p_common.Event{}, err
	}
	f.publishLog = append(f.publishLog, publishRecord{topic: topic, value: value})
	subs := append([]p2p_common.PubSubHandler(nil), f.subscribers[topic]...)
	f.mu.Unlock()
	// Deliver synchronously so tests don't race; real pubsub is async but
	// the dispatcher's ack-wait already handles arbitrary timing.
	for _, h := range subs {
		h(p2p_common.Event{Message: value})
	}
	return p2p_common.Event{Message: value}, nil
}

func (f *fakePubsub) Subscribe(_ context.Context, topic string, handler p2p_common.PubSubHandler) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subscribers[topic] = append(f.subscribers[topic], handler)
	return nil
}

func (f *fakePubsub) Unsubscribe(_ context.Context, topic string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.subscribers, topic)
	return nil
}

func (f *fakePubsub) publishedOn(topic string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.publishLog {
		if r.topic == topic {
			n++
		}
	}
	return n
}

// fakeNotifier counts webhook POSTs and lets tests control success/failure.
type fakeNotifier struct {
	mu      sync.Mutex
	calls   []fakeNotifierCall
	failNow bool
}

type fakeNotifierCall struct {
	payload interface{}
	url     string
}

func (n *fakeNotifier) Notify(_ context.Context, payload interface{}, url string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.failNow {
		return errors.New("webhook fail")
	}
	n.calls = append(n.calls, fakeNotifierCall{payload: payload, url: url})
	return nil
}

func (n *fakeNotifier) callCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.calls)
}

// scriptedEnvelope is a programmable EnvelopeHandler that records each call
// and either returns nil (write OK) or an error (write fail). Tests use it
// as the localDevice.onEnv so they can simulate "WS open, write succeeds"
// vs "WS open, write fails."
type scriptedEnvelope struct {
	mu      sync.Mutex
	writes  []meshMessage
	writeErr error
	// When writeAcksSync is true, the handler immediately calls
	// disp.SignalDelivered as if the client had instantly acked. Used to
	// simulate a healthy client.
	writeAcksSync bool
	disp          *Dispatcher
	apiKey        string
}

func (s *scriptedEnvelope) handler(payload []byte) error {
	s.mu.Lock()
	var m meshMessage
	if err := json.Unmarshal(payload, &m); err != nil {
		s.mu.Unlock()
		return err
	}
	s.writes = append(s.writes, m)
	doAck := s.writeAcksSync && m.Kind == MeshKindEnvelope
	disp := s.disp
	apiKey := s.apiKey
	err := s.writeErr
	s.mu.Unlock()
	if doAck {
		// Fire ack on a goroutine so the caller (DeliverLocal) returns first.
		go disp.SignalDelivered(context.Background(), apiKey, m.SenderUserID, m.SenderDeviceID, m.EnvelopeUUID)
	}
	return err
}

// ── helpers ─────────────────────────────────────────────────────────────────

const (
	testAPIKey      = "test-api-key"
	testSender      = "alice"
	testSenderDev   = "alice-dev"
	testRecipient   = "bob"
	testRecipientDev = "bob-dev"
)

func newTestDispatcher(t *testing.T, fb time.Duration) (*Dispatcher, *PresenceTracker, *fakePubsub, *fakeNotifier) {
	t.Helper()
	ps := newFakePubsub()
	pres := NewPresenceTracker(ps, 200*time.Millisecond)
	notif := &fakeNotifier{}
	disp := NewDispatcher(ps, pres, notif, fb, 200*time.Millisecond)
	return disp, pres, ps, notif
}

// registerLocal registers (apiKey, userID, deviceID) on the presence
// tracker with the given scripted handler.
func registerLocal(t *testing.T, pres *PresenceTracker, apiKey, userID, deviceID string, s *scriptedEnvelope) {
	t.Helper()
	if err := pres.RegisterDevice(apiKey, userID, deviceID, s.handler); err != nil {
		t.Fatalf("RegisterDevice: %v", err)
	}
}

// sendOneAsync starts SendOne in a goroutine, returns a channel that
// emits the result. Tests use this when they need to manipulate state
// while SendOne is in its wait loop.
func sendOneAsync(disp *Dispatcher, target SendTarget) <-chan SendResult {
	ch := make(chan SendResult, 1)
	go func() {
		ch <- disp.SendOne(
			context.Background(),
			testAPIKey,
			testSender, testSenderDev,
			testRecipient,
			target,
			"normal",
			false, // ephemeral
			nil,   // notifyPush (nil = legacy default)
			"http://test/webhook",
		)
	}()
	return ch
}

func makeTarget(uuid string) SendTarget {
	return SendTarget{
		DeviceID:     testRecipientDev,
		Ciphertext:   "Y2lwaA==",
		EnvelopeUUID: uuid,
	}
}

// ── tests ───────────────────────────────────────────────────────────────────

// Same-node happy path: recipient WS acks instantly → StatusLive, no
// webhook fired, no retry-publishes (none needed).
func TestSendOne_SameNode_HappyPath(t *testing.T) {
	disp, pres, ps, notif := newTestDispatcher(t, 2*time.Second)
	scripted := &scriptedEnvelope{disp: disp, apiKey: testAPIKey, writeAcksSync: true}
	registerLocal(t, pres, testAPIKey, testRecipient, testRecipientDev, scripted)
	defer pres.UnregisterDevice(testAPIKey, testRecipient, testRecipientDev)

	res := disp.SendOne(context.Background(), testAPIKey, testSender, testSenderDev,
		testRecipient, makeTarget("uuid-1"), "normal", false, nil, "http://test/webhook")

	if res.Status != StatusLive {
		t.Fatalf("status=%q, want %q (err=%q)", res.Status, StatusLive, res.Err)
	}
	if notif.callCount() != 0 {
		t.Errorf("webhook fired %d times; expected 0", notif.callCount())
	}
	envTopic := EnvelopeTopic(testAPIKey, testRecipient, testRecipientDev)
	if n := ps.publishedOn(envTopic); n != 0 {
		t.Errorf("envelope-topic publishes=%d; expected 0 (local fast path)", n)
	}
}

// Same-node, recipient WS write fails. Sender shouldn't waste publishes
// on a same-node target (gossipsub filters self-publishes anyway), waits
// for the timeout, then falls back to webhook + post-webhook publish.
func TestSendOne_SameNode_WriteFails_FallsBack(t *testing.T) {
	disp, pres, ps, notif := newTestDispatcher(t, 300*time.Millisecond)
	scripted := &scriptedEnvelope{disp: disp, apiKey: testAPIKey, writeErr: errors.New("broken pipe")}
	registerLocal(t, pres, testAPIKey, testRecipient, testRecipientDev, scripted)
	defer pres.UnregisterDevice(testAPIKey, testRecipient, testRecipientDev)

	res := disp.SendOne(context.Background(), testAPIKey, testSender, testSenderDev,
		testRecipient, makeTarget("uuid-2"), "normal", false, nil, "http://test/webhook")

	if res.Status != StatusStored {
		t.Fatalf("status=%q, want %q (err=%q)", res.Status, StatusStored, res.Err)
	}
	if notif.callCount() != 1 {
		t.Errorf("webhook calls=%d; expected 1", notif.callCount())
	}
	envTopic := EnvelopeTopic(testAPIKey, testRecipient, testRecipientDev)
	// Post-webhook publish should have fired once.
	if n := ps.publishedOn(envTopic); n != 1 {
		t.Errorf("envelope-topic publishes=%d; expected 1 (post-webhook only)", n)
	}
}

// Same-node, write OK, never acks. Timeout → fallback. Retry-publishes
// should NOT fire on the envelope topic (DeliverLocal returned (true, true)
// each tick, so the publish branch is skipped).
func TestSendOne_SameNode_WriteOKButNoAck_FallsBack(t *testing.T) {
	disp, pres, ps, notif := newTestDispatcher(t, 700*time.Millisecond)
	// writeAcksSync=false → handler records the write but never acks.
	scripted := &scriptedEnvelope{disp: disp, apiKey: testAPIKey}
	registerLocal(t, pres, testAPIKey, testRecipient, testRecipientDev, scripted)
	defer pres.UnregisterDevice(testAPIKey, testRecipient, testRecipientDev)

	res := disp.SendOne(context.Background(), testAPIKey, testSender, testSenderDev,
		testRecipient, makeTarget("uuid-3"), "normal", false, nil, "http://test/webhook")

	if res.Status != StatusStored {
		t.Fatalf("status=%q, want %q (err=%q)", res.Status, StatusStored, res.Err)
	}
	if notif.callCount() != 1 {
		t.Errorf("webhook calls=%d; expected 1", notif.callCount())
	}
	envTopic := EnvelopeTopic(testAPIKey, testRecipient, testRecipientDev)
	// Same-node + write OK = no publishes during the wait; the post-webhook
	// publish fires once after fallback.
	if n := ps.publishedOn(envTopic); n != 1 {
		t.Errorf("envelope-topic publishes=%d; expected 1 (post-webhook only)", n)
	}
	// Scripted handler should have seen the initial write and 0 retries
	// (retry tick noticed delivered=true,writeOK=true and didn't publish).
	scripted.mu.Lock()
	writeCount := len(scripted.writes)
	scripted.mu.Unlock()
	if writeCount < 1 {
		t.Errorf("scripted writes=%d; expected ≥1 (at least the initial deliver)", writeCount)
	}
}

// Retry catches recipient that reconnects mid-flow: B starts not registered,
// the initial publish lands nowhere; B registers ~one retry tick in and
// the next retry's DeliverLocal succeeds → ack → StatusLive, no webhook.
func TestSendOne_RetryDeliversAfterReconnect(t *testing.T) {
	disp, pres, _, notif := newTestDispatcher(t, 2*time.Second)

	// Start the send first — B isn't local yet, so initial tryDeliverOrPublish
	// goes through the publish branch (nothing receives it).
	resultCh := sendOneAsync(disp, makeTarget("uuid-4"))

	// Register B ~one retry interval (500ms) in. The retry tick will see B
	// in localDevices and DeliverLocal will succeed.
	time.Sleep(750 * time.Millisecond)
	scripted := &scriptedEnvelope{disp: disp, apiKey: testAPIKey, writeAcksSync: true}
	registerLocal(t, pres, testAPIKey, testRecipient, testRecipientDev, scripted)
	defer pres.UnregisterDevice(testAPIKey, testRecipient, testRecipientDev)

	select {
	case res := <-resultCh:
		if res.Status != StatusLive {
			t.Fatalf("status=%q, want %q (err=%q)", res.Status, StatusLive, res.Err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("SendOne did not return within 3s")
	}
	if notif.callCount() != 0 {
		t.Errorf("webhook fired %d times; expected 0", notif.callCount())
	}
}

// Post-webhook publish catches the recipient who reconnects AFTER webhook
// completes. B not present during the whole 2s window → webhook fires →
// post-webhook publish lands on the envelope topic.
func TestSendOne_PostWebhookPublishCatchesReconnect(t *testing.T) {
	disp, _, ps, notif := newTestDispatcher(t, 300*time.Millisecond)

	res := disp.SendOne(context.Background(), testAPIKey, testSender, testSenderDev,
		testRecipient, makeTarget("uuid-5"), "normal", false, nil, "http://test/webhook")

	if res.Status != StatusStored {
		t.Fatalf("status=%q, want %q", res.Status, StatusStored)
	}
	if notif.callCount() != 1 {
		t.Errorf("webhook calls=%d; expected 1", notif.callCount())
	}
	// The final post-webhook publish should bring the topic's count up by
	// at least 1 (and may have retry-publishes from the wait loop too).
	envTopic := EnvelopeTopic(testAPIKey, testRecipient, testRecipientDev)
	if n := ps.publishedOn(envTopic); n < 1 {
		t.Errorf("envelope-topic publishes=%d; expected ≥1 (retries + post-webhook)", n)
	}
}

// boolPtr is a tiny test helper for the *bool notifyPush argument.
func boolPtr(b bool) *bool { return &b }

// fallbackBodyFromCall extracts the typed fallbackBody from the
// fakeNotifier's recorded payload. The notifier's Notify signature
// uses interface{} so the assert site has to type-narrow.
func fallbackBodyFromCall(t *testing.T, call fakeNotifierCall) fallbackBody {
	t.Helper()
	body, ok := call.payload.(fallbackBody)
	if !ok {
		t.Fatalf("notifier payload is not fallbackBody: %T", call.payload)
	}
	return body
}

// notifyPush:nil (legacy SDK that doesn't set the field). Webhook body
// must carry Push=true because the presence-based decision allows it
// (no recipient device live). Verifies the legacy default is "allow".
func TestSendOne_NotifyPush_NilDefaultsToAllow(t *testing.T) {
	disp, _, _, notif := newTestDispatcher(t, 300*time.Millisecond)

	res := disp.SendOne(context.Background(), testAPIKey, testSender, testSenderDev,
		testRecipient, makeTarget("np-nil"), "normal", false, nil /* notifyPush */, "http://test/webhook")

	if res.Status != StatusStored {
		t.Fatalf("status=%q, want %q", res.Status, StatusStored)
	}
	if notif.callCount() != 1 {
		t.Fatalf("webhook calls=%d; expected 1", notif.callCount())
	}
	body := fallbackBodyFromCall(t, notif.calls[0])
	if !body.Push {
		t.Errorf("body.Push=%v; expected true (nil notifyPush = legacy default = allow)", body.Push)
	}
}

// notifyPush=&true. Same outcome as nil — SDK explicitly opts in to
// push, presence decision is "no live device" → body.Push=true.
func TestSendOne_NotifyPush_ExplicitTrueAllowsPush(t *testing.T) {
	disp, _, _, notif := newTestDispatcher(t, 300*time.Millisecond)

	res := disp.SendOne(context.Background(), testAPIKey, testSender, testSenderDev,
		testRecipient, makeTarget("np-true"), "normal", false, boolPtr(true), "http://test/webhook")

	if res.Status != StatusStored {
		t.Fatalf("status=%q, want %q", res.Status, StatusStored)
	}
	if notif.callCount() != 1 {
		t.Fatalf("webhook calls=%d; expected 1", notif.callCount())
	}
	body := fallbackBodyFromCall(t, notif.calls[0])
	if !body.Push {
		t.Errorf("body.Push=%v; expected true (notifyPush=&true)", body.Push)
	}
}

// notifyPush=&false. SDK opts out — body.Push must be false even
// though the presence decision would otherwise allow it. This is the
// load-bearing case for content-aware push suppression. Without this
// override, edits / deletes / receipts / selfEcho would still wake the
// recipient via push when the peer is offline.
func TestSendOne_NotifyPush_ExplicitFalseSuppressesPush(t *testing.T) {
	disp, _, _, notif := newTestDispatcher(t, 300*time.Millisecond)

	res := disp.SendOne(context.Background(), testAPIKey, testSender, testSenderDev,
		testRecipient, makeTarget("np-false"), "normal", false, boolPtr(false), "http://test/webhook")

	// Envelope still goes to webhook (durable delivery preserved) — the
	// flag only suppresses the push at the backend's gating step.
	if res.Status != StatusStored {
		t.Fatalf("status=%q, want %q", res.Status, StatusStored)
	}
	if notif.callCount() != 1 {
		t.Fatalf("webhook calls=%d; expected 1 (durability is independent of push)", notif.callCount())
	}
	body := fallbackBodyFromCall(t, notif.calls[0])
	if body.Push {
		t.Errorf("body.Push=%v; expected false (notifyPush=&false)", body.Push)
	}
}

// Ephemeral envelopes fast-path: one delivery attempt, no retry loop,
// no wait for ack, no webhook, no post-webhook publish. The retry loop
// for ephemerals (typing) was producing retry storms that parked the
// receiver wsConn's reader behind the SendAll handleFrame path — and
// typing is throttled/refreshed sender-side, so a missed delivery
// resolves itself on the next refresh. Verify: returns immediately
// (well under fallbackTimeout), exactly one publish/deliver attempt.
func TestSendOne_Ephemeral_FastPath(t *testing.T) {
	// fallbackTimeout intentionally long — if the fast-path ever
	// regresses, the test would block on the retry loop instead of
	// returning quickly.
	disp, _, ps, notif := newTestDispatcher(t, 5*time.Second)

	envTopic := EnvelopeTopic(testAPIKey, testRecipient, testRecipientDev)
	start := time.Now()
	res := disp.SendOne(context.Background(), testAPIKey, testSender, testSenderDev,
		testRecipient, makeTarget("uuid-6"), "normal", true /* ephemeral */, nil /* notifyPush */, "http://test/webhook")
	elapsed := time.Since(start)

	if res.Status != StatusDropped {
		t.Fatalf("status=%q, want %q", res.Status, StatusDropped)
	}
	if notif.callCount() != 0 {
		t.Errorf("ephemeral webhook calls=%d; expected 0", notif.callCount())
	}
	// No retry loop → returns essentially instantly. Allow generous
	// slack for CI scheduling, but well under fallbackTimeout / first
	// retry tick (500ms).
	if elapsed > 250*time.Millisecond {
		t.Errorf("ephemeral SendOne took %v; expected near-immediate return (fast-path)", elapsed)
	}
	// Exactly one publish — no retries.
	if n := ps.publishedOn(envTopic); n != 1 {
		t.Errorf("envelope-topic publishes=%d; expected exactly 1 (fast-path, no retries)", n)
	}
}

// SignalDelivered same-node short-circuit: when the inflight channel is
// local, SignalDelivered signals it directly and does NOT publish a
// MeshKindAck on the sender's topic (no round-trip needed).
func TestSignalDelivered_SameNodeShortCircuit(t *testing.T) {
	disp, _, ps, _ := newTestDispatcher(t, 5*time.Second)
	ackCh := disp.registerInflight("uuid-7")
	defer disp.unregisterInflight("uuid-7")

	disp.SignalDelivered(context.Background(), testAPIKey, testSender, testSenderDev, "uuid-7")

	select {
	case <-ackCh:
		// expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("inflight channel not signalled")
	}
	senderTopic := EnvelopeTopic(testAPIKey, testSender, testSenderDev)
	if n := ps.publishedOn(senderTopic); n != 0 {
		t.Errorf("sender-topic publishes=%d; expected 0 (same-node short-circuit)", n)
	}
}

// SignalDelivered cross-node path: when there's no local inflight (the
// sender is on a different node), SignalDelivered publishes a MeshKindAck
// on the sender's envelope topic.
func TestSignalDelivered_CrossNodePublishesAck(t *testing.T) {
	disp, _, ps, _ := newTestDispatcher(t, 5*time.Second)
	// No inflight registered for uuid-8 → cross-node branch.

	disp.SignalDelivered(context.Background(), testAPIKey, testSender, testSenderDev, "uuid-8")

	senderTopic := EnvelopeTopic(testAPIKey, testSender, testSenderDev)
	if n := ps.publishedOn(senderTopic); n != 1 {
		t.Errorf("sender-topic publishes=%d; expected 1 (cross-node ack)", n)
	}
}

// SignalDelivered ignores empty envelopeUuid (defensive guard).
func TestSignalDelivered_EmptyUUID_NoOp(t *testing.T) {
	disp, _, ps, _ := newTestDispatcher(t, 5*time.Second)
	disp.SignalDelivered(context.Background(), testAPIKey, testSender, testSenderDev, "")
	if n := len(ps.publishLog); n != 0 {
		t.Errorf("publishes=%d; expected 0", n)
	}
}

// DeliverLocal returns (false, false) when the device isn't local.
func TestPresence_DeliverLocal_NotLocal(t *testing.T) {
	_, pres, _, _ := newTestDispatcher(t, 1*time.Second)
	delivered, writeOK := pres.DeliverLocal(testAPIKey, "missing", "missing-dev", meshMessage{Kind: MeshKindEnvelope})
	if delivered || writeOK {
		t.Errorf("delivered=%v writeOK=%v; expected (false, false)", delivered, writeOK)
	}
}

// DeliverLocal returns (true, true) when device is local AND handler returns nil.
func TestPresence_DeliverLocal_OK(t *testing.T) {
	_, pres, _, _ := newTestDispatcher(t, 1*time.Second)
	calls := int32(0)
	registerLocal(t, pres, testAPIKey, testRecipient, testRecipientDev, &scriptedEnvelope{})
	defer pres.UnregisterDevice(testAPIKey, testRecipient, testRecipientDev)
	// Register a second device with a recording handler to test the OK path
	// without the scriptedEnvelope sync-ack noise.
	if err := pres.RegisterDevice(testAPIKey, "bob2", "bob2-dev", func([]byte) error {
		atomic.AddInt32(&calls, 1)
		return nil
	}); err != nil {
		t.Fatalf("RegisterDevice: %v", err)
	}
	defer pres.UnregisterDevice(testAPIKey, "bob2", "bob2-dev")

	delivered, writeOK := pres.DeliverLocal(testAPIKey, "bob2", "bob2-dev", meshMessage{Kind: MeshKindEnvelope, EnvelopeUUID: "u"})
	if !delivered || !writeOK {
		t.Errorf("delivered=%v writeOK=%v; expected (true, true)", delivered, writeOK)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("handler called %d times; expected 1", calls)
	}
}

// DeliverLocal returns (true, false) when device is local but handler errors.
func TestPresence_DeliverLocal_WriteError(t *testing.T) {
	_, pres, _, _ := newTestDispatcher(t, 1*time.Second)
	if err := pres.RegisterDevice(testAPIKey, "bob3", "bob3-dev", func([]byte) error {
		return errors.New("write failed")
	}); err != nil {
		t.Fatalf("RegisterDevice: %v", err)
	}
	defer pres.UnregisterDevice(testAPIKey, "bob3", "bob3-dev")

	delivered, writeOK := pres.DeliverLocal(testAPIKey, "bob3", "bob3-dev", meshMessage{Kind: MeshKindEnvelope, EnvelopeUUID: "u"})
	if !delivered || writeOK {
		t.Errorf("delivered=%v writeOK=%v; expected (true, false)", delivered, writeOK)
	}
}

// wsConn.pendingDelivered: add → consume returns true, second consume
// returns false (consumed entry was removed).
func TestWsConn_PendingDelivered_AddConsume(t *testing.T) {
	wc := newWsConn(nil)
	wc.pendingAdd("uuid-A", "alice", "alice-dev")
	if !wc.pendingConsume("uuid-A", "alice", "alice-dev") {
		t.Error("first consume returned false; expected true")
	}
	if wc.pendingConsume("uuid-A", "alice", "alice-dev") {
		t.Error("second consume returned true; expected false (entry was removed)")
	}
}

// wsConn.pendingDelivered: consume with mismatched sender ids returns
// false (anti-spoof).
func TestWsConn_PendingDelivered_SenderMismatch(t *testing.T) {
	wc := newWsConn(nil)
	wc.pendingAdd("uuid-B", "alice", "alice-dev")
	if wc.pendingConsume("uuid-B", "alice", "alice-OTHER-dev") {
		t.Error("consume with wrong device returned true; expected false")
	}
	if wc.pendingConsume("uuid-B", "OTHER", "alice-dev") {
		t.Error("consume with wrong user returned true; expected false")
	}
	// Entry should still be present (mismatch didn't consume).
	if !wc.pendingConsume("uuid-B", "alice", "alice-dev") {
		t.Error("consume with correct ids returned false after mismatched attempts")
	}
}

// wsConn.pendingDelivered: at capacity, the oldest entry evicts. The
// evicted uuid then fails to consume (forces sender's webhook fallback).
func TestWsConn_PendingDelivered_CapacityEviction(t *testing.T) {
	wc := newWsConn(nil)
	for i := 0; i < wsConnPendingCap+5; i++ {
		wc.pendingAdd(uuidLike(i), "alice", "alice-dev")
	}
	// First five should be evicted.
	for i := 0; i < 5; i++ {
		if wc.pendingConsume(uuidLike(i), "alice", "alice-dev") {
			t.Errorf("uuid #%d should have been evicted but was consumable", i)
		}
	}
	// Sixth onward should still be present.
	if !wc.pendingConsume(uuidLike(5), "alice", "alice-dev") {
		t.Error("uuid #5 should be present but consume returned false")
	}
}

// Duplicate add (sender retry on same envelope) is a no-op: count
// stays the same, eviction order unchanged.
func TestWsConn_PendingDelivered_DuplicateAdd(t *testing.T) {
	wc := newWsConn(nil)
	wc.pendingAdd("uuid-X", "alice", "alice-dev")
	wc.pendingAdd("uuid-X", "alice", "alice-dev") // duplicate
	wc.pendingAdd("uuid-Y", "alice", "alice-dev")
	wc.pendingM.Lock()
	defer wc.pendingM.Unlock()
	if len(wc.pendingDelivered) != 2 {
		t.Errorf("map size=%d; expected 2", len(wc.pendingDelivered))
	}
	if len(wc.pendingOrder) != 2 {
		t.Errorf("order size=%d; expected 2", len(wc.pendingOrder))
	}
}

func uuidLike(i int) string {
	return "uuid-" + string(rune('A'+i%26)) + "-" + string(rune('0'+i/26))
}

// ── Cross-node + SendAll integration tests ──────────────────────────────────
//
// These wire two PresenceTrackers (simulating two nodes N1 and N2) onto the
// same fakePubsub. Envelopes published by N1's dispatcher are received by
// N2's subscription handler (the recipient's WS) which then sends the
// chatEnvelopeAck. The ack travels back via fakePubsub on the sender's
// envelope topic, hits N1's subscription, and signals N1's inflight
// channel. Models the real cross-node ack round-trip without a real
// gossipsub network.

// crossNodeRig sets up two notional nodes sharing one fakePubsub. The
// receiver-side PresenceTracker is the one B's WS would register with.
type crossNodeRig struct {
	ps       *fakePubsub
	notif    *fakeNotifier
	senderD  *Dispatcher // N1 — the dispatcher A's send goes through
	senderP  *PresenceTracker
	recvP    *PresenceTracker // N2 — registers B's WS handler
	recvDisp *Dispatcher       // N2's dispatcher (needed for cross-node ack to find inflight on N1)
}

func newCrossNodeRig(t *testing.T, fb time.Duration) *crossNodeRig {
	t.Helper()
	ps := newFakePubsub()
	notif := &fakeNotifier{}

	senderP := NewPresenceTracker(ps, 200*time.Millisecond)
	senderD := NewDispatcher(ps, senderP, notif, fb, 200*time.Millisecond)

	recvP := NewPresenceTracker(ps, 200*time.Millisecond)
	recvDisp := NewDispatcher(ps, recvP, notif, fb, 200*time.Millisecond)

	// The sender's node has a WS subscription on its envelope topic so the
	// MeshKindAck from the recipient lands there and wakes the inflight
	// channel via SignalAck. In the real code service.go's onEnvelope
	// closure dispatches MeshKindAck → SignalAck. We register a minimal
	// equivalent here.
	senderEnvTopic := EnvelopeTopic(testAPIKey, testSender, testSenderDev)
	if err := ps.Subscribe(context.Background(), senderEnvTopic, func(ev p2p_common.Event) {
		raw, err := jsonMarshalForTest(ev.Message)
		if err != nil {
			return
		}
		var m meshMessage
		if err := json.Unmarshal(raw, &m); err != nil {
			return
		}
		if m.Kind == MeshKindAck {
			senderD.SignalAck(m.EnvelopeUUID)
		}
	}); err != nil {
		t.Fatalf("sender envelope-topic subscribe: %v", err)
	}

	return &crossNodeRig{
		ps:       ps,
		notif:    notif,
		senderD:  senderD,
		senderP:  senderP,
		recvP:    recvP,
		recvDisp: recvDisp,
	}
}

// jsonMarshalForTest re-marshals an `interface{}` carried via fakePubsub.
// Real pubsub round-trips through JSON; we mimic that here.
func jsonMarshalForTest(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// Cross-node happy path: B (on N2) acks the envelope, the ack travels
// back via fakePubsub to N1, N1 returns StatusLive. No webhook.
func TestSendOne_CrossNode_HappyPath(t *testing.T) {
	rig := newCrossNodeRig(t, 2*time.Second)
	defer rig.recvP.UnregisterDevice(testAPIKey, testRecipient, testRecipientDev)

	// B's WS handler: writes succeed, immediately ack via recvDisp.
	bobHandler := func(payload []byte) error {
		var m meshMessage
		if err := json.Unmarshal(payload, &m); err != nil {
			return err
		}
		if m.Kind == MeshKindEnvelope {
			go rig.recvDisp.SignalDelivered(context.Background(), testAPIKey, m.SenderUserID, m.SenderDeviceID, m.EnvelopeUUID)
		}
		return nil
	}
	if err := rig.recvP.RegisterDevice(testAPIKey, testRecipient, testRecipientDev, bobHandler); err != nil {
		t.Fatalf("RegisterDevice: %v", err)
	}

	res := rig.senderD.SendOne(context.Background(), testAPIKey, testSender, testSenderDev,
		testRecipient, makeTarget("xn-1"), "normal", false, nil, "http://test/webhook")
	if res.Status != StatusLive {
		t.Fatalf("status=%q, want %q (err=%q)", res.Status, StatusLive, res.Err)
	}
	if rig.notif.callCount() != 0 {
		t.Errorf("webhook calls=%d; expected 0", rig.notif.callCount())
	}
}

// Cross-node no-ack: B's WS handler never acks. Sender retries within the
// 2s window (we'll see multiple publishes on B's topic), then falls back
// to webhook + post-webhook publish.
func TestSendOne_CrossNode_NoAck_FallsBack(t *testing.T) {
	rig := newCrossNodeRig(t, 700*time.Millisecond)
	defer rig.recvP.UnregisterDevice(testAPIKey, testRecipient, testRecipientDev)

	silentHandler := func(payload []byte) error { return nil }
	if err := rig.recvP.RegisterDevice(testAPIKey, testRecipient, testRecipientDev, silentHandler); err != nil {
		t.Fatalf("RegisterDevice: %v", err)
	}

	res := rig.senderD.SendOne(context.Background(), testAPIKey, testSender, testSenderDev,
		testRecipient, makeTarget("xn-2"), "normal", false, nil, "http://test/webhook")
	if res.Status != StatusStored {
		t.Fatalf("status=%q, want %q (err=%q)", res.Status, StatusStored, res.Err)
	}
	if rig.notif.callCount() != 1 {
		t.Errorf("webhook calls=%d; expected 1", rig.notif.callCount())
	}
	// Should have seen ≥2 publishes on B's envelope topic: the initial one
	// plus at least one retry (700ms window / 500ms tick = 1 retry) plus
	// the post-webhook publish.
	envTopic := EnvelopeTopic(testAPIKey, testRecipient, testRecipientDev)
	if n := rig.ps.publishedOn(envTopic); n < 2 {
		t.Errorf("envelope-topic publishes=%d; expected ≥2 (initial + retries + post-webhook)", n)
	}
}

// SendAll partial-success: two targets, one acks, one doesn't. Both
// results come back independently — one StatusLive, one StatusStored.
func TestSendAll_PartialSuccess(t *testing.T) {
	disp, pres, _, notif := newTestDispatcher(t, 700*time.Millisecond)

	// Target 1: acks instantly.
	live := &scriptedEnvelope{disp: disp, apiKey: testAPIKey, writeAcksSync: true}
	if err := pres.RegisterDevice(testAPIKey, testRecipient, "dev-live", live.handler); err != nil {
		t.Fatalf("register dev-live: %v", err)
	}
	defer pres.UnregisterDevice(testAPIKey, testRecipient, "dev-live")

	// Target 2: never acks.
	silent := &scriptedEnvelope{disp: disp, apiKey: testAPIKey}
	if err := pres.RegisterDevice(testAPIKey, testRecipient, "dev-silent", silent.handler); err != nil {
		t.Fatalf("register dev-silent: %v", err)
	}
	defer pres.UnregisterDevice(testAPIKey, testRecipient, "dev-silent")

	targets := []SendTarget{
		{DeviceID: "dev-live", Ciphertext: "Y2lwaA==", EnvelopeUUID: "sa-1"},
		{DeviceID: "dev-silent", Ciphertext: "Y2lwaA==", EnvelopeUUID: "sa-2"},
	}
	results := disp.SendAll(context.Background(), testAPIKey, testSender, testSenderDev,
		testRecipient, targets, "normal", false, nil, "http://test/webhook")

	if len(results) != 2 {
		t.Fatalf("results=%d; expected 2", len(results))
	}
	// Map by uuid for clearer assertions (order is preserved but be explicit).
	byUUID := map[string]SendResult{}
	for _, r := range results {
		byUUID[r.EnvelopeUUID] = r
	}
	if r := byUUID["sa-1"]; r.Status != StatusLive {
		t.Errorf("dev-live status=%q; expected %q", r.Status, StatusLive)
	}
	if r := byUUID["sa-2"]; r.Status != StatusStored {
		t.Errorf("dev-silent status=%q; expected %q", r.Status, StatusStored)
	}
	if notif.callCount() != 1 {
		t.Errorf("webhook calls=%d; expected 1 (only the silent target)", notif.callCount())
	}
}

// SendAll all-targets ack: every result is StatusLive, no webhook.
func TestSendAll_AllAck(t *testing.T) {
	disp, pres, _, notif := newTestDispatcher(t, 2*time.Second)

	for _, devID := range []string{"d1", "d2", "d3"} {
		s := &scriptedEnvelope{disp: disp, apiKey: testAPIKey, writeAcksSync: true}
		if err := pres.RegisterDevice(testAPIKey, testRecipient, devID, s.handler); err != nil {
			t.Fatalf("register %s: %v", devID, err)
		}
		defer pres.UnregisterDevice(testAPIKey, testRecipient, devID)
	}

	targets := []SendTarget{
		{DeviceID: "d1", Ciphertext: "Y2lwaA==", EnvelopeUUID: "aa-1"},
		{DeviceID: "d2", Ciphertext: "Y2lwaA==", EnvelopeUUID: "aa-2"},
		{DeviceID: "d3", Ciphertext: "Y2lwaA==", EnvelopeUUID: "aa-3"},
	}
	results := disp.SendAll(context.Background(), testAPIKey, testSender, testSenderDev,
		testRecipient, targets, "normal", false, nil, "http://test/webhook")

	for _, r := range results {
		if r.Status != StatusLive {
			t.Errorf("uuid=%s status=%q; expected %q", r.EnvelopeUUID, r.Status, StatusLive)
		}
	}
	if notif.callCount() != 0 {
		t.Errorf("webhook calls=%d; expected 0", notif.callCount())
	}
}
