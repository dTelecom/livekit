package chat

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v3/jwt"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/testutils"
)

// Exercise the real authenticated WebSocket handlers on separate service instances.
// Only the tenant registry, pub/sub transport and fallback notifier are in memory.
func TestChatWebSocketCrossServiceDeliveryAndAck(t *testing.T) {
	key, issuer := generateKey(t)
	lookup := fakeLookup{knownIss: issuer, knownKey: issuer}
	bus := newFakePubsub()
	notifier := &fakeNotifier{}

	connect := func(userID, deviceID string) chatTestDevice {
		t.Helper()
		presence := NewPresenceTracker(bus, 100*time.Millisecond)
		dispatcher := NewDispatcher(bus, presence, notifier, 3*time.Second, 100*time.Millisecond)
		service := NewService(config.ChatConfig{Enabled: true}, presence, dispatcher, lookup)
		mux := http.NewServeMux()
		mux.Handle("/chat/ws", service.Handler())
		server := httptest.NewServer(mux)
		t.Cleanup(server.Close)

		now := time.Now()
		token := signTestChat(t, key, ChatClaims{
			Type: "chat", Issuer: issuer, Subject: userID, DeviceID: deviceID,
			IssuedAt: now.Unix(), Expiry: now.Add(time.Minute).Unix(),
			WebhookURL: "https://chat-fixture.invalid/fallback", ChatSend: true, ChatReceive: true,
		})
		return dialChatDevice(t, "ws"+strings.TrimPrefix(server.URL, "http")+"/chat/ws", token, userID, deviceID)
	}

	const aliceDevice = "cbd84275-f481-49db-a3d1-445be2cdac41"
	const bobDevice = "b8f6ce7b-635f-423a-87e2-dd1300b87e7d"
	alice := connect("alice", aliceDevice)
	bob := connect("bob", bobDevice)
	checkChatDeliveryAndAck(t, alice, bob)
	require.Positive(t, bus.publishedOn(EnvelopeTopic(issuer, "bob", bobDevice)))
	require.Positive(t, bus.publishedOn(EnvelopeTopic(issuer, "alice", aliceDevice)))
	require.Zero(t, notifier.callCount())
}

type chatTestDevice struct {
	conn         *websocket.Conn
	user, device string
}

func dialChatDevice(t *testing.T, endpoint, token, user, device string) chatTestDevice {
	t.Helper()
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, resp, err := dialer.Dial(endpoint, http.Header{"Authorization": []string{"Bearer " + token}})
	if err != nil && resp != nil {
		t.Fatalf("chat upgrade at %s returned HTTP %d", endpoint, resp.StatusCode)
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	deadline := time.Now().Add(20 * time.Second)
	require.NoError(t, conn.SetReadDeadline(deadline))
	require.NoError(t, conn.SetWriteDeadline(deadline))
	// A pong confirms the node installed the local device subscriptions.
	require.NoError(t, conn.WriteJSON(frameEnvelope{Kind: kindPing}))
	var pong chatPong
	require.NoError(t, conn.ReadJSON(&pong))
	require.Equal(t, kindPong, pong.Kind)
	return chatTestDevice{conn: conn, user: user, device: device}
}

func checkChatDeliveryAndAck(t *testing.T, sender, recipient chatTestDevice) {
	t.Helper()
	target := SendTarget{
		DeviceID: recipient.device, EnvelopeUUID: uuid.NewString(),
		Ciphertext: "AAECA/7/",
	}
	require.NoError(t, sender.conn.WriteJSON(chatSendIn{
		Kind: kindSend, ToUserID: recipient.user, Targets: []SendTarget{target},
	}))

	var received chatEnvelopeOut
	require.NoError(t, recipient.conn.ReadJSON(&received))
	require.Equal(t, chatEnvelopeOut{
		Kind: kindEnvelope, EnvelopeUUID: target.EnvelopeUUID, Ciphertext: target.Ciphertext,
		SenderUserID: sender.user, SenderDeviceID: sender.device, MsgType: "normal",
	}, received)
	require.NoError(t, recipient.conn.WriteJSON(chatEnvelopeAckIn{
		Kind: kindEnvelopeAck, EnvelopeUUID: received.EnvelopeUUID,
		SenderUserID: received.SenderUserID, SenderDeviceID: received.SenderDeviceID,
	}))

	var result chatSendResultOut
	require.NoError(t, sender.conn.ReadJSON(&result))
	require.Equal(t, chatSendResultOut{Kind: kindSendResult, Results: []SendResult{
		{EnvelopeUUID: target.EnvelopeUUID, Status: StatusLive},
	}}, result)
}

// TestJoinedChatDeliveryAndAck sends one opaque envelope from the local native
// node to a second registered node. Supply short-lived, distinct
// test-device grants from the same registered tenant via these two file paths.
// The sender's grant must name an available host loopback port for the fallback
// sink. The recipient never sends an envelope and also requires a loopback URL.
func TestJoinedChatDeliveryAndAck(t *testing.T) {
	if testing.Short() {
		t.Skip("requires joined chat nodes")
	}
	a, b := os.Getenv("DTELECOM_CHAT_TOKEN_A_FILE"), os.Getenv("DTELECOM_CHAT_TOKEN_B_FILE")
	if a == "" && b == "" {
		t.Skip("requires two local joined nodes and two dedicated chat token files")
	}
	require.NotEmpty(t, a)
	require.NotEmpty(t, b)
	readGrant := func(path string) (string, *ChatClaims, *url.URL) {
		t.Helper()
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		token := strings.TrimSpace(string(raw))
		parsed, err := jwt.ParseSigned(token)
		require.NoError(t, err)
		var claimed struct {
			Issuer string `json:"iss"`
		}
		require.NoError(t, parsed.UnsafeClaimsWithoutVerification(&claimed))
		// Verify locally before using any claims. Each joined node then verifies
		// the same token against its configured tenant registry at WS upgrade.
		claims, err := VerifyChatToken(context.Background(), token, fakeLookup{knownIss: claimed.Issuer, knownKey: claimed.Issuer}, time.Now().Unix())
		require.NoError(t, err)
		u, err := url.Parse(claims.WebhookURL)
		require.NoError(t, err)
		require.True(t, u.Scheme == "http" && u.Hostname() == "127.0.0.1" && u.Port() != "" && u.User == nil && u.Path == "/chat-fixture-fallback" && u.RawQuery == "" && u.Fragment == "", "chat token must specify an isolated loopback fallback")
		return token, claims, u
	}
	at, ac, callback := readGrant(a)
	bt, bc, _ := readGrant(b)
	require.True(t, ac.Issuer == bc.Issuer && ac.Subject != bc.Subject && ac.DeviceID != bc.DeviceID && ac.ChatSend && bc.ChatReceive, "expected distinct test devices and users under the same tenant")
	listener, err := net.Listen("tcp4", callback.Host)
	require.NoError(t, err)
	var callbacks atomic.Int32
	sink := &http.Server{
		ReadHeaderTimeout: 2 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			callbacks.Add(1)
			w.WriteHeader(http.StatusNoContent)
		}),
	}
	go func() { _ = sink.Serve(listener) }()
	t.Cleanup(func() { _ = sink.Close() })
	chatEndpoint := func(variable, fallback string) string {
		t.Helper()
		origin := os.Getenv(variable)
		if origin == "" {
			origin = fallback
		}
		endpoint, err := testutils.SignalingOrigin(origin)
		require.NoError(t, err, variable)
		return endpoint + "/chat/ws"
	}
	alice := dialChatDevice(t, chatEndpoint("DTELECOM_SMOKE_NODE_A", "http://127.0.0.1:7880"), at, ac.Subject, ac.DeviceID)
	bob := dialChatDevice(t, chatEndpoint("DTELECOM_SMOKE_NODE_B", "http://127.0.0.1:7882"), bt, bc.Subject, bc.DeviceID)
	checkChatDeliveryAndAck(t, alice, bob)
	require.Zero(t, callbacks.Load())
	t.Log("node A sent to node B; recipient ACK returned live to node A; zero fallback deliveries")
}
