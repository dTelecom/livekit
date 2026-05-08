package chat

import (
	"context"
	"crypto/ed25519"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/go-jose/go-jose/v3"
	"github.com/go-jose/go-jose/v3/jwt"
)

// fakeLookup is a test-only ClientLookup that returns a fixed pubkey, with a
// flag to simulate "wallet not in registry" for one of the tests.
type fakeLookup struct {
	knownIss string // base58 pubkey we recognize
	knownKey string // base58 pubkey returned as Client.Key
}

func (f fakeLookup) ClientByAddress(_ context.Context, address string) (LookupResult, error) {
	if address != f.knownIss {
		return LookupResult{}, errors.New("not in registry")
	}
	return LookupResult{Key: f.knownKey}, nil
}

// signTestChat mints a chat-token JWT signed by the given Ed25519 private
// key with the supplied claims. Bypasses protocol/auth.AccessToken because
// its ClaimGrants struct has no chat-specific fields.
func signTestChat(t *testing.T, priv ed25519.PrivateKey, c ChatClaims) string {
	t.Helper()
	sig, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA, Key: priv},
		(&jose.SignerOptions{}).WithType("JWT"))
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	tok, err := jwt.Signed(sig).Claims(&c).CompactSerialize()
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return tok
}

// generateKey returns a Solana-style 64-byte ed25519 key pair (seed||pub) and
// the base58 pubkey, matching how the LK_API_KEY/LK_API_SECRET pair is shaped.
func generateKey(t *testing.T) (priv ed25519.PrivateKey, pubB58 string) {
	t.Helper()
	pub, sk, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("gen: %v", err)
	}
	return sk, solana.PublicKey(pub).String()
}

func TestVerifyChatToken_HappyPath(t *testing.T) {
	priv, pub := generateKey(t)
	now := time.Now().Unix()
	claims := ChatClaims{
		Type:        "chat",
		Issuer:      pub,
		Subject:     "user-alice",
		DeviceID:    "dev-1",
		IssuedAt:    now,
		Expiry:      now + 3600,
		WebhookURL:  "http://x/api/chat/envelopes",
		ChatSend:    true,
		ChatReceive: true,
	}
	tok := signTestChat(t, priv, claims)

	lookup := fakeLookup{knownIss: pub, knownKey: pub}
	got, err := VerifyChatToken(context.Background(), tok, lookup, now)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if got.Subject != "user-alice" || got.DeviceID != "dev-1" {
		t.Errorf("claims: %+v", got)
	}
	if got.WebhookURL != claims.WebhookURL {
		t.Errorf("webhook url: %s", got.WebhookURL)
	}
}

func TestVerifyChatToken_RejectsRoomToken(t *testing.T) {
	priv, pub := generateKey(t)
	now := time.Now().Unix()
	claims := ChatClaims{
		// typ omitted (or set to wrong value) → must reject
		Issuer:     pub,
		Subject:    "u",
		DeviceID:   "d",
		IssuedAt:   now,
		Expiry:     now + 60,
		WebhookURL: "http://x",
	}
	tok := signTestChat(t, priv, claims)
	lookup := fakeLookup{knownIss: pub, knownKey: pub}
	_, err := VerifyChatToken(context.Background(), tok, lookup, now)
	if err == nil {
		t.Fatal("expected reject on missing typ=chat")
	}
}

func TestVerifyChatToken_Expired(t *testing.T) {
	priv, pub := generateKey(t)
	now := time.Now().Unix()
	claims := ChatClaims{
		Type: "chat", Issuer: pub, Subject: "u", DeviceID: "d",
		IssuedAt: now - 7200, Expiry: now - 3600, WebhookURL: "http://x",
	}
	tok := signTestChat(t, priv, claims)
	_, err := VerifyChatToken(context.Background(), tok, fakeLookup{knownIss: pub, knownKey: pub}, now)
	if err == nil {
		t.Fatal("expected reject on expired token")
	}
}

func TestVerifyChatToken_UnknownSigner(t *testing.T) {
	priv, pub := generateKey(t)
	now := time.Now().Unix()
	claims := ChatClaims{
		Type: "chat", Issuer: pub, Subject: "u", DeviceID: "d",
		IssuedAt: now, Expiry: now + 60, WebhookURL: "http://x",
	}
	tok := signTestChat(t, priv, claims)
	// lookup doesn't recognize this issuer
	_, err := VerifyChatToken(context.Background(), tok, fakeLookup{knownIss: "DIFFERENT", knownKey: "DIFFERENT"}, now)
	if err == nil {
		t.Fatal("expected reject for unregistered signer")
	}
}

func TestVerifyChatToken_ForgedSig(t *testing.T) {
	priv, pub := generateKey(t)
	_, otherPub := generateKey(t)
	now := time.Now().Unix()
	claims := ChatClaims{
		Type: "chat", Issuer: pub, Subject: "u", DeviceID: "d",
		IssuedAt: now, Expiry: now + 60, WebhookURL: "http://x",
	}
	tok := signTestChat(t, priv, claims)
	// Lookup returns a DIFFERENT pubkey — sig won't verify.
	_, err := VerifyChatToken(context.Background(), tok, fakeLookup{knownIss: pub, knownKey: otherPub}, now)
	if err == nil {
		t.Fatal("expected reject when registry returns wrong pubkey")
	}
}

func TestVerifyChatToken_MissingFields(t *testing.T) {
	priv, pub := generateKey(t)
	now := time.Now().Unix()
	for _, tc := range []struct {
		name   string
		mutate func(*ChatClaims)
	}{
		{"no sub", func(c *ChatClaims) { c.Subject = "" }},
		{"no did", func(c *ChatClaims) { c.DeviceID = "" }},
		{"no webhook", func(c *ChatClaims) { c.WebhookURL = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := ChatClaims{
				Type: "chat", Issuer: pub, Subject: "u", DeviceID: "d",
				IssuedAt: now, Expiry: now + 60, WebhookURL: "http://x",
			}
			tc.mutate(&c)
			tok := signTestChat(t, priv, c)
			_, err := VerifyChatToken(context.Background(), tok, fakeLookup{knownIss: pub, knownKey: pub}, now)
			if err == nil {
				t.Fatalf("expected reject for missing field")
			}
		})
	}
}

func TestChatTokenMiddleware(t *testing.T) {
	priv, pub := generateKey(t)
	now := time.Now().Unix()
	claims := ChatClaims{
		Type: "chat", Issuer: pub, Subject: "u", DeviceID: "d",
		IssuedAt: now, Expiry: now + 60, WebhookURL: "http://x",
		ChatSend: true, ChatReceive: true,
	}
	tok := signTestChat(t, priv, claims)
	lookup := fakeLookup{knownIss: pub, knownKey: pub}

	var seenClaims *ChatClaims
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenClaims = GetClaims(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	h := ChatTokenMiddleware(inner, lookup)

	t.Run("happy path: bearer header", func(t *testing.T) {
		seenClaims = nil
		req := httptest.NewRequest("GET", "/chat/ws", nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d", rec.Code)
		}
		if seenClaims == nil || seenClaims.Subject != "u" {
			t.Errorf("claims not stamped: %+v", seenClaims)
		}
	})

	t.Run("happy path: access_token query param", func(t *testing.T) {
		seenClaims = nil
		req := httptest.NewRequest("GET", "/chat/ws?access_token="+tok, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d", rec.Code)
		}
		if seenClaims == nil {
			t.Error("expected claims via access_token fallback")
		}
	})

	t.Run("missing auth", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/chat/ws", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("expected 401, got %d", rec.Code)
		}
	})

	t.Run("bearer without prefix", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/chat/ws", nil)
		req.Header.Set("Authorization", tok) // no "Bearer "
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("expected 401, got %d", rec.Code)
		}
	})

	t.Run("bad token", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/chat/ws", nil)
		req.Header.Set("Authorization", "Bearer not.a.jwt")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("expected 401, got %d", rec.Code)
		}
	})
}
