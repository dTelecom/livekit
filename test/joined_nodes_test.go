package test

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/livekit/livekit-server/pkg/testutils"
	testclient "github.com/livekit/livekit-server/test/client"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/utils"
)

var joinedSmoke = flag.Bool("joined-smoke", false, "exercise two operator-supplied joined test nodes")

// Run against a dedicated test tenant with the direct signaling origins in
// DTELECOM_SMOKE_NODE_A and DTELECOM_SMOKE_NODE_B. Set DTELECOM_SMOKE_TOKEN_A_FILE
// and DTELECOM_SMOKE_TOKEN_B_FILE to files containing the raw short-lived participant
// JWTs for two distinct identities in one dedicated room. Alternatively, supply a
// registered client issuer's Solana keygen JSON in DTELECOM_SMOKE_ISSUER_KEY_FILE
// to create a fresh room for each run. Optional DTELECOM_SMOKE_EXPECT_NODE_A and
// DTELECOM_SMOKE_EXPECT_NODE_B pin each endpoint's expected serving node ID.
// With pre-issued participants, DTELECOM_SMOKE_ADMIN_TOKEN_FILE optionally supplies
// that room's admin JWT for a metadata update and restore observed on both nodes.
//
//	go test -count=1 -run '^TestJoinedNodeSmoke$' ./test -args -joined-smoke
func TestJoinedNodeSmoke(t *testing.T) {
	if testing.Short() || !*joinedSmoke {
		t.Skip("requires -joined-smoke and a dedicated joined-node fixture")
	}
	endpoints := make([]string, 0, 2)
	for _, name := range []string{"DTELECOM_SMOKE_NODE_A", "DTELECOM_SMOKE_NODE_B"} {
		endpoint, err := testutils.SignalingOrigin(os.Getenv(name))
		require.NoError(t, err, name)
		endpoints = append(endpoints, endpoint)
	}
	require.NotEqual(t, endpoints[0], endpoints[1], "supply two node-specific origins")
	participants, err := smokeParticipants(os.Getenv("DTELECOM_SMOKE_ISSUER_KEY_FILE"), [2]string{
		os.Getenv("DTELECOM_SMOKE_TOKEN_A_FILE"), os.Getenv("DTELECOM_SMOKE_TOKEN_B_FILE"),
	})
	require.NoError(t, err)
	expectedNodes := [2]string{os.Getenv("DTELECOM_SMOKE_EXPECT_NODE_A"), os.Getenv("DTELECOM_SMOKE_EXPECT_NODE_B")}
	room := participants[0].room
	var adminToken string
	if path := os.Getenv("DTELECOM_SMOKE_ADMIN_TOKEN_FILE"); path != "" {
		adminToken, err = readSmokeAdminToken(path, participants[0])
		require.NoError(t, err)
	}
	type packet struct{ sender, payload string }
	var clients [2]*testclient.RTCClient
	var received [2]chan packet
	var nodes [2]string
	for i, endpoint := range endpoints {
		conn, join, err := dialJoinedNode(endpoint, participants[i])
		require.NoError(t, err, "join participant %d", i+1)
		t.Cleanup(func() { _ = conn.Close() })
		nodes[i] = join.GetServerInfo().GetNodeId()
		if expectedNodes[i] != "" {
			require.Equal(t, expectedNodes[i], nodes[i], "serving node for endpoint %d", i+1)
		}
		c, err := testclient.NewRTCClientFromJoin(conn, join)
		require.NoError(t, err)
		t.Cleanup(c.Stop)

		received[i] = make(chan packet, 1)
		inbox := received[i]
		c.OnDataReceived = func(data []byte, sender string) {
			select {
			case inbox <- packet{sender: sender, payload: string(data)}:
			default:
			}
		}
		clients[i] = c
		go c.Run()
	}
	require.NotEqual(t, nodes[0], nodes[1], "both endpoints routed to the same SFU")
	t.Logf("room %s joined on nodes %s and %s", room, nodes[0], nodes[1])
	waitUntilConnected(t, clients[0], clients[1])

	t.Run("participant profile from signed grant", func(t *testing.T) {
		for i, observer := range clients {
			publisher := clients[1-i]
			grant := participants[1-i]
			require.Eventually(t, func() bool {
				peer := observer.GetRemoteParticipant(publisher.ID())
				return peer.GetIdentity() == grant.identity && peer.GetName() == grant.name && peer.GetMetadata() == grant.metadata
			}, testutils.ConnectTimeout, 50*time.Millisecond, "initial participant profile did not match its signed grant")
		}
	})

	t.Run("media and reliable data", func(t *testing.T) {
		for _, c := range clients {
			writer, err := c.AddStaticTrack("audio/opus", "audio", "microphone")
			require.NoError(t, err)
			t.Cleanup(writer.Stop)
		}
		for i, c := range clients {
			peer := clients[1-i].ID()
			require.Eventually(t, func() bool {
				return c.BytesReceivedFrom(peer) >= 256
			}, testutils.ConnectTimeout, 50*time.Millisecond, "node %s did not receive RTP from %s", nodes[i], nodes[1-i])
		}
		for i, c := range clients {
			payload := fmt.Sprintf("%s-from-%d", room, i)
			err := c.PublishData([]byte(payload), livekit.DataPacket_RELIABLE)
			require.NoError(t, err)
			select {
			case got := <-received[1-i]:
				require.Equal(t, packet{sender: string(c.ID()), payload: payload}, got)
			case <-time.After(testutils.ConnectTimeout):
				t.Fatalf("reliable data from node %s did not reach %s", nodes[i], nodes[1-i])
			}
		}
	})

	if adminToken != "" {
		t.Run("room metadata across both nodes", func(t *testing.T) {
			original, ok := clients[0].RoomMetadata(room)
			require.True(t, ok, "missing initial room state on node A")
			remote, ok := clients[1].RoomMetadata(room)
			require.True(t, ok, "missing initial room state on node B")
			require.True(t, original == remote, "initial room metadata differs across nodes")

			update := func(value string) {
				t.Helper()
				err := updateSmokeRoomMetadata(endpoints[0], adminToken, room, value)
				require.NoError(t, err)
				for i, c := range clients {
					require.Eventually(t, func() bool {
						metadata, ok := c.RoomMetadata(room)
						return ok && metadata == value
					}, testutils.ConnectTimeout, 50*time.Millisecond, "metadata did not reach node %s", nodes[i])
				}
			}
			t.Cleanup(func() { update(original) })
			update(utils.NewGuid("compiler-smoke-metadata-"))
		})
	}
}

func readSmokeAdminToken(path string, participant smokeParticipant) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("reading room admin token file: %w", err)
	}
	token := strings.TrimSpace(string(data))
	verifier, err := auth.ParseAPIToken(token)
	if err != nil || verifier.APIKey() != participant.issuer {
		return "", errors.New("expected an admin token issued by the smoke room's tenant")
	}
	grants, err := verifier.Verify(participant.issuer)
	if err != nil || grants.Video == nil || !grants.Video.RoomAdmin || grants.Video.Room != participant.room {
		return "", errors.New("expected a valid admin grant for the smoke room")
	}
	return token, nil
}

func joinedRoomService(endpoint string) livekit.RoomService {
	// testutils.SignalingOrigin has normalized the scheme to ws or wss.
	origin := "http" + strings.TrimPrefix(endpoint, "ws")
	return livekit.NewRoomServiceJSONClient(origin, &http.Client{Timeout: testutils.ConnectTimeout})
}

func updateSmokeRoomMetadata(endpoint, token, room, value string) error {
	result, err := joinedRoomService(endpoint).UpdateRoomMetadata(contextWithToken(token), &livekit.UpdateRoomMetadataRequest{Room: room, Metadata: value})
	if err != nil {
		return err
	}
	if result.GetName() != room || result.GetMetadata() != value {
		return errors.New("admin endpoint did not confirm metadata on the requested room")
	}
	return nil
}

type smokeParticipant struct {
	room, identity, name, metadata, issuer, token string
}

func smokeParticipants(keyFile string, tokenFiles [2]string) ([2]smokeParticipant, error) {
	var participants [2]smokeParticipant
	var tokens [2]string
	if keyFile != "" {
		if tokenFiles[0] != "" || tokenFiles[1] != "" {
			return participants, errors.New("choose either the issuer key file or two participant token files")
		}
		key, err := solana.PrivateKeyFromSolanaKeygenFile(keyFile)
		if err != nil {
			return participants, fmt.Errorf("loading the dedicated test issuer: %w", err)
		}
		room := utils.NewGuid("compiler-smoke-")
		for i := range tokens {
			tokens[i], err = signSmokeToken(key, room, fmt.Sprintf("smoke-%d", i+1))
			if err != nil {
				return participants, err
			}
		}
	} else {
		if tokenFiles[0] == "" || tokenFiles[1] == "" {
			return participants, errors.New("supply both participant token files or the issuer key file")
		}
		for i, path := range tokenFiles {
			raw, err := os.ReadFile(path)
			if err != nil {
				return participants, fmt.Errorf("reading participant %d token file: %w", i+1, err)
			}
			tokens[i] = strings.TrimSpace(string(raw))
		}
	}
	for i, token := range tokens {
		p, err := verifySmokeToken(token)
		if err != nil {
			return participants, fmt.Errorf("participant %d: %w", i+1, err)
		}
		participants[i] = p
	}
	a, b := participants[0], participants[1]
	if a.room != b.room || a.issuer != b.issuer || a.identity == b.identity {
		return participants, errors.New("participants need the same issuer and room and distinct identities")
	}
	return participants, nil
}

func signSmokeToken(key solana.PrivateKey, room, identity string) (string, error) {
	return auth.NewAccessToken(key.PublicKey().String(), key.String()).
		AddGrant(&auth.VideoGrant{RoomJoin: true, Room: room}).
		SetIdentity(identity).SetName("display-" + identity).SetMetadata("metadata-" + identity).
		SetValidFor(5 * time.Minute).ToJWT()
}

func verifySmokeToken(token string) (smokeParticipant, error) {
	return verifyJoinedParticipantToken(token, true)
}

func verifyJoinedParticipantToken(token string, canSubscribe bool) (smokeParticipant, error) {
	v, err := auth.ParseAPIToken(token)
	if err != nil {
		return smokeParticipant{}, errors.New("could not parse participant JWT")
	}
	claims, err := v.Verify(v.APIKey())
	if err != nil {
		return smokeParticipant{}, errors.New("participant JWT failed signature or validity checks")
	}
	g := claims.Video
	if claims.Identity == "" || g == nil || !g.RoomJoin || g.Room == "" ||
		!g.GetCanPublish() || !g.GetCanPublishData() {
		return smokeParticipant{}, errors.New("participant needs an identity and room join, publish and data permissions")
	}
	if g.GetCanSubscribe() != canSubscribe {
		return smokeParticipant{}, fmt.Errorf("participant requires canSubscribe=%t for this scenario", canSubscribe)
	}
	return smokeParticipant{room: g.Room, identity: claims.Identity, name: claims.Name, metadata: claims.Metadata, issuer: v.APIKey(), token: token}, nil
}

func dialJoinedNode(endpoint string, participant smokeParticipant) (*websocket.Conn, *livekit.JoinResponse, error) {
	return dialJoinedNodeWithSubscription(endpoint, participant, true)
}

func dialJoinedNodeWithSubscription(endpoint string, participant smokeParticipant, autoSubscribe bool) (*websocket.Conn, *livekit.JoinResponse, error) {
	conn, err := testclient.NewWebSocketConn(endpoint, participant.token, &testclient.Options{AutoSubscribe: autoSubscribe})
	if err != nil {
		return nil, nil, err
	}
	join, err := readJoinedNodeResponse(conn, participant.room, participant.identity)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	return conn, join, nil
}

func readJoinedNodeResponse(conn *websocket.Conn, room, identity string) (*livekit.JoinResponse, error) {
	if err := conn.SetReadDeadline(time.Now().Add(testutils.ConnectTimeout)); err != nil {
		return nil, err
	}
	kind, data, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	var response livekit.SignalResponse
	if kind != websocket.BinaryMessage || proto.Unmarshal(data, &response) != nil {
		return nil, errors.New("expected a binary protobuf join response")
	}
	join := response.GetJoin()
	if join.GetRoom().GetName() != room || join.GetParticipant().GetIdentity() != identity ||
		join.GetParticipant().GetSid() == "" || join.GetServerInfo().GetNodeId() == "" {
		return nil, errors.New("join response missing serving node or matching room and participant")
	}
	return join, conn.SetReadDeadline(time.Time{})
}

// Exercise the signaling request and response locally. No fixture keys or endpoints are used.
func TestJoinedNodeSignaling(t *testing.T) {
	key, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	const room, identity = "fixture-room", "fixture-participant"
	const otherRoom = "other-fixture-room"
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rtc" || r.URL.Query().Get("protocol") != "7" {
			http.Error(w, "invalid signaling request", http.StatusBadRequest)
			return
		}
		token, err := auth.ParseAPIToken(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		if err != nil || token.APIKey() != key.PublicKey().String() {
			http.Error(w, "unexpected issuer", http.StatusUnauthorized)
			return
		}
		grants, err := token.Verify(key.PublicKey().String())
		if err != nil || grants.Identity != identity || grants.Video == nil || !grants.Video.RoomJoin ||
			(grants.Video.Room != room && grants.Video.Room != otherRoom) {
			http.Error(w, "invalid participant grants", http.StatusForbidden)
			return
		}
		wantSubscribe := "true"
		if grants.Video.Room == otherRoom {
			wantSubscribe = "false"
		}
		if r.URL.Query().Get("auto_subscribe") != wantSubscribe {
			http.Error(w, "wrong auto-subscription option", http.StatusBadRequest)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		data, err := proto.Marshal(&livekit.SignalResponse{Message: &livekit.SignalResponse_Join{
			Join: &livekit.JoinResponse{
				Participant: &livekit.ParticipantInfo{Identity: identity, Sid: "PA_test"},
				Room:        &livekit.Room{Name: room}, ServerInfo: &livekit.ServerInfo{NodeId: "node-a"},
				SubscriberPrimary: true,
			},
		}})
		if err != nil {
			t.Error(err)
			return
		}
		if err := conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
			t.Error(err)
			return
		}
		if grants.Video.Room == room {
			update, err := proto.Marshal(&livekit.SignalResponse{Message: &livekit.SignalResponse_RoomUpdate{
				RoomUpdate: &livekit.RoomUpdate{Room: &livekit.Room{Name: room, Metadata: "updated"}},
			}})
			if err != nil {
				t.Error(err)
				return
			}
			if err := conn.WriteMessage(websocket.BinaryMessage, update); err != nil {
				t.Error(err)
			}
		}
	}))
	defer server.Close()
	endpoint, err := testutils.SignalingOrigin(server.URL)
	require.NoError(t, err)
	credential := func(room string) smokeParticipant {
		raw, err := signSmokeToken(key, room, identity)
		require.NoError(t, err)
		p, err := verifySmokeToken(raw)
		require.NoError(t, err)
		return p
	}
	conn, join, err := dialJoinedNode(endpoint, credential(room))
	require.NoError(t, err)
	defer conn.Close()
	require.Equal(t, "node-a", join.ServerInfo.NodeId)
	client, err := testclient.NewRTCClientFromJoin(conn, join)
	require.NoError(t, err)
	defer client.Stop()
	require.Equal(t, livekit.ParticipantID("PA_test"), client.ID())
	metadata, ok := client.RoomMetadata(room)
	require.True(t, ok)
	require.Empty(t, metadata)
	_ = client.Run() // the signaling double closes the socket after the room update
	metadata, ok = client.RoomMetadata(room)
	require.True(t, ok)
	require.Equal(t, "updated", metadata)

	// A signed request for another room must reject this server's fixed room reply.
	_, _, err = dialJoinedNodeWithSubscription(endpoint, credential(otherRoom), false)
	require.ErrorContains(t, err, "matching room and participant")
}

func TestJoinedNodeOrigins(t *testing.T) {
	endpoint, err := testutils.SignalingOrigin("https://node.example:444/")
	require.NoError(t, err)
	require.Equal(t, "wss://node.example:444", endpoint)
	for _, invalid := range []string{"", "ftp://node.example", "https://user@node.example", "wss://node.example/rtc?token=unused"} {
		_, err := testutils.SignalingOrigin(invalid)
		require.Error(t, err)
	}
}

func TestJoinedNodeIssuerFile(t *testing.T) {
	key, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	// Solana keygen JSON encodes the private key as an array of 64 octets.
	var bytes [64]byte
	copy(bytes[:], key)
	data, err := json.Marshal(bytes)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "issuer.json")
	require.NoError(t, os.WriteFile(path, data, 0600))
	first, err := smokeParticipants(path, [2]string{})
	require.NoError(t, err)
	second, err := smokeParticipants(path, [2]string{})
	require.NoError(t, err)
	require.Equal(t, first[0].room, first[1].room)
	require.NotEqual(t, first[0].room, second[0].room)
	require.NotEqual(t, first[0].identity, first[1].identity)
}

func TestJoinedNodeTokenFiles(t *testing.T) {
	key, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	otherKey, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	dir := t.TempDir()
	files := [2]string{filepath.Join(dir, "a"), filepath.Join(dir, "b")}
	writeToken := func(path string, issuer solana.PrivateKey, room, identity string) {
		t.Helper()
		raw, err := signSmokeToken(issuer, room, identity)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, []byte(raw+"\n"), 0600))
	}
	writeToken(files[0], key, "call", "alice")
	writeToken(files[1], key, "call", "bob")
	participants, err := smokeParticipants("", files)
	require.NoError(t, err)
	require.Equal(t, "call", participants[0].room)
	require.Equal(t, "alice", participants[0].identity)
	require.Equal(t, "display-alice", participants[0].name)
	require.Equal(t, "metadata-alice", participants[0].metadata)
	require.Equal(t, "bob", participants[1].identity)
	require.Equal(t, key.PublicKey().String(), participants[0].issuer)

	for _, tc := range []struct {
		key            solana.PrivateKey
		room, identity string
	}{
		{key, "other-room", "bob"},
		{otherKey, "call", "bob"},
		{key, "call", "alice"},
	} {
		writeToken(files[1], tc.key, tc.room, tc.identity)
		_, err := smokeParticipants("", files)
		require.ErrorContains(t, err, "same issuer and room and distinct identities")
	}

	grant := &auth.VideoGrant{RoomJoin: true, Room: "call"}
	grant.SetCanSubscribe(false)
	restricted, err := auth.NewAccessToken(key.PublicKey().String(), key.String()).
		AddGrant(grant).SetIdentity("limited").ToJWT()
	require.NoError(t, err)
	_, err = verifySmokeToken(restricted)
	require.ErrorContains(t, err, "canSubscribe=true")
	limited, err := verifyJoinedParticipantToken(restricted, false)
	require.NoError(t, err)
	require.Equal(t, "limited", limited.identity)
	_, err = verifyJoinedParticipantToken(participants[0].token, false)
	require.ErrorContains(t, err, "canSubscribe=false")

	// A forged issuer claim cannot authenticate using another wallet's signature.
	raw, err := auth.NewAccessToken(key.PublicKey().String(), otherKey.String()).
		AddGrant(&auth.VideoGrant{RoomJoin: true, Room: "call"}).SetIdentity("bob").ToJWT()
	require.NoError(t, err)
	_, err = verifySmokeToken(raw)
	require.ErrorContains(t, err, "signature or validity")
	_, err = smokeParticipants("", [2]string{files[0], ""})
	require.ErrorContains(t, err, "both participant token files")
	_, err = smokeParticipants("unused-key-file", files)
	require.ErrorContains(t, err, "either")
}

func TestJoinedNodeRoomAdmin(t *testing.T) {
	key, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	const room = "fixture-room"
	p := smokeParticipant{room: room, issuer: key.PublicKey().String()}
	file := filepath.Join(t.TempDir(), "admin")
	writeGrant := func(grant *auth.VideoGrant) {
		t.Helper()
		token, err := auth.NewAccessToken(p.issuer, key.String()).AddGrant(grant).
			SetValidFor(5 * time.Minute).ToJWT()
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(file, []byte(token), 0600))
	}

	for _, grant := range []*auth.VideoGrant{{Room: room, RoomJoin: true}, {Room: "different", RoomAdmin: true}} {
		writeGrant(grant)
		_, err := readSmokeAdminToken(file, p)
		require.Error(t, err)
	}
	writeGrant(&auth.VideoGrant{Room: room, RoomAdmin: true})
	token, err := readSmokeAdminToken(file, p)
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != livekit.RoomServicePathPrefix+"UpdateRoomMetadata" ||
			r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "unexpected admin request", http.StatusBadRequest)
			return
		}
		var request livekit.UpdateRoomMetadataRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Room != room {
			http.Error(w, "invalid room request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(map[string]string{"name": request.Room, "metadata": request.Metadata})
		if err != nil {
			t.Error(err)
		}
	}))
	defer srv.Close()
	endpoint, err := testutils.SignalingOrigin(srv.URL)
	require.NoError(t, err)
	require.NoError(t, updateSmokeRoomMetadata(endpoint, token, room, "fixture-marker"))
	require.NoError(t, updateSmokeRoomMetadata(endpoint, token, room, ""))
}
