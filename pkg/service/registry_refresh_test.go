package service

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/config"
)

// Each server answers one read-only RPC method with an immutable snapshot.
// A nil snapshot models an unavailable upstream.
func registryFixture(t *testing.T, method string, result interface{}) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Method != method {
			t.Errorf("unexpected registry request, method %q", request.Method)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		if result == nil {
			http.Error(w, "fixture unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": request.ID, "result": result})
		if err != nil {
			t.Errorf("writing registry response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	return server.URL
}

func fixtureRegistryAccount(kind string, size int, parent, wallet solana.PublicKey) []byte {
	data := make([]byte, size)
	discriminator := sha256.Sum256([]byte("account:" + kind))
	copy(data[:8], discriminator[:8])
	copy(data[8:40], parent[:])
	copy(data[40:72], wallet[:])
	return data
}

func TestBootRegistryOutageAndRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	signer := solana.NewWallet()
	authority, program := solana.NewWallet().PublicKey(), solana.NewWallet().PublicKey()
	registry, _, err := findRegistryPDA(program, authority, "dtel-nodes")
	require.NoError(t, err)
	provider := &BootNodeProvider{
		WalletPrivateKey: signer.PrivateKey.String(), ContractAddress: program.String(),
		RegistryAuthority: authority.String(), PeerListenPort: 4200,
		nodeValues: make(map[solana.PublicKey]bootNodeMessage),
	}
	nodeA, nodeB := solana.NewWallet().PublicKey(), solana.NewWallet().PublicKey()
	var accounts []*rpc.KeyedAccount
	for wallet, domain := range map[solana.PublicKey]string{nodeA: "a.example", nodeB: "b.example"} {
		key, _, err := findRegistryEntryPDA(program, wallet, registry)
		require.NoError(t, err)
		data := fixtureRegistryAccount("NodeEntry", NodeEntrySize, registry, wallet)
		binary.LittleEndian.PutUint32(data[72:76], uint32(len(domain)))
		copy(data[76:], domain)
		data[80+len(domain)] = 1 // active, following the i32 online count
		accounts = append(accounts, &rpc.KeyedAccount{Pubkey: key, Account: &rpc.Account{Owner: program, Data: rpc.DataBytesOrJSONFromBytes(data)}})
	}
	healthyRPC := registryFixture(t, "getProgramAccounts", accounts)
	failingRPC := registryFixture(t, "getProgramAccounts", nil)
	addresses := map[string]string{"a.example": "192.0.2.1", "b.example": "192.0.2.2"}
	lookup := func(host string) ([]net.IP, error) {
		ip, ok := addresses[host]
		if !ok {
			return nil, errors.New("fixture DNS unavailable")
		}
		return []net.IP{net.IPv6loopback, net.ParseIP(ip)}, nil
	}
	check := func(want map[solana.PublicKey]string) {
		t.Helper()
		wallets, err := provider.GetAuthorizedWallets(ctx)
		require.NoError(t, err)
		nodes, err := provider.GetBootstrapNodes(ctx)
		require.NoError(t, err)
		require.Len(t, wallets, len(want))
		for key := range want {
			require.Contains(t, wallets, key)
		}
		endpoints := make(map[solana.PublicKey]string)
		for _, node := range nodes {
			require.Equal(t, provider.PeerListenPort, node.QUICPort)
			require.Equal(t, provider.PeerListenPort, node.TCPPort)
			endpoints[node.PublicKey] = node.IP
		}
		require.Equal(t, want, endpoints)
	}

	provider.NetworkHostHTTP = failingRPC
	require.Error(t, provider.refresh(ctx, lookup))
	check(map[solana.PublicKey]string{})

	provider.NetworkHostHTTP = healthyRPC
	require.NoError(t, provider.refresh(ctx, lookup))
	check(map[solana.PublicKey]string{nodeA: "192.0.2.1", nodeB: "192.0.2.2"})
	original := provider.nodeValues[nodeA]
	// A shorter remaining TTL makes accidental extension observable even
	// when several refreshes happen in the same second.
	original.TTL = time.Now().Add(time.Minute).Unix()
	provider.nodeValues[nodeA] = original

	provider.NetworkHostHTTP = failingRPC
	require.Error(t, provider.refresh(ctx, lookup))
	require.Equal(t, original, provider.nodeValues[nodeA])
	check(map[solana.PublicKey]string{nodeA: "192.0.2.1", nodeB: "192.0.2.2"})

	delete(addresses, "a.example")
	addresses["b.example"] = "192.0.2.22"
	provider.NetworkHostHTTP = healthyRPC
	require.NoError(t, provider.refresh(ctx, lookup))
	require.Equal(t, original, provider.nodeValues[nodeA])
	check(map[solana.PublicKey]string{nodeA: "192.0.2.1", nodeB: "192.0.2.22"})

	// Advance the cached observation past its TTL without waiting five minutes.
	original.TTL = time.Now().Add(-time.Second).Unix()
	provider.nodeValues[nodeA] = original
	require.NoError(t, provider.refresh(ctx, lookup))
	check(map[solana.PublicKey]string{nodeB: "192.0.2.22"})

	addresses["a.example"] = "192.0.2.11"
	require.NoError(t, provider.refresh(ctx, lookup))
	check(map[solana.PublicKey]string{nodeA: "192.0.2.11", nodeB: "192.0.2.22"})
}

func TestClientRegistryOutageAndRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	issuer, authority, program := solana.NewWallet().PublicKey(), solana.NewWallet().PublicKey(), solana.NewWallet().PublicKey()
	registry, _, err := findRegistryPDA(program, authority, "clients")
	require.NoError(t, err)
	data := fixtureRegistryAccount("ClientEntry", ClientEntrySize, registry, issuer)
	until := time.Now().Add(time.Hour).Unix()
	binary.LittleEndian.PutUint64(data[72:80], uint64(until))
	binary.LittleEndian.PutUint32(data[80:84], 10)
	healthy := registryFixture(t, "getAccountInfo", rpc.GetAccountInfoResult{Value: &rpc.Account{Owner: program, Data: rpc.DataBytesOrJSONFromBytes(data)}})
	outage := registryFixture(t, "getAccountInfo", nil)
	p := NewClientProvider(config.SolanaConfig{WalletPrivateKey: solana.NewWallet().PrivateKey.String(), ContractAddress: program.String(), RegistryAuthority: authority.String(), NetworkHostHTTP: outage})
	key := issuer.String()
	_, err = p.ClientByAddress(ctx, key)
	require.Error(t, err)
	p.NetworkHostHTTP = healthy
	want := Client{Key: key, Limit: 10, Until: until}
	got, err := p.ClientByAddress(ctx, key)
	require.NoError(t, err)
	require.Equal(t, want, got)

	p.NetworkHostHTTP = outage
	got, err = p.ClientByAddress(ctx, key)
	require.NoError(t, err)
	require.Equal(t, want, got)
	p.clientValues[key] = clientMessage{Client: want, TTL: time.Now().Add(-time.Second).Unix()}
	_, err = p.ClientByAddress(ctx, key)
	require.Error(t, err)
	list, err := p.List(ctx)
	require.NoError(t, err)
	require.Empty(t, list)

	p.NetworkHostHTTP = healthy
	got, err = p.ClientByAddress(ctx, key)
	require.NoError(t, err)
	require.Equal(t, want, got)
}
