package chat

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/go-jose/go-jose/v3/jwt"
)

// ClientLookup is the minimal slice of pkg/service.ClientProvider that the
// chat auth path needs. Defined here as an interface so chat tests can inject
// a fake without depending on the Solana registry.
//
// Production wires the existing concrete `*service.ClientProvider`. Its
// `ClientByAddress(ctx, addr) (Client, error)` returns Client.Key (the base58
// pubkey of a registered tenant wallet).
type ClientLookup interface {
	ClientByAddress(ctx context.Context, address string) (LookupResult, error)
}

// LookupResult is the chat-side view of a ClientProvider lookup result. Slim
// — we only need the public key to verify EdDSA. The production adapter
// converts service.Client to this.
type LookupResult struct {
	Key string // base58-encoded Ed25519 public key
}

// ChatClaims is the body of a chat-token JWT. Verified cryptographically
// against the issuer's wallet pubkey from the Solana client registry.
type ChatClaims struct {
	Type           string `json:"typ"`              // must be "chat"
	Issuer         string `json:"iss"`              // base58 tenant pubkey
	Subject        string `json:"sub"`              // tenant-local user id
	DeviceID       string `json:"did"`              // device id (UUID)
	IssuedAt       int64  `json:"iat"`
	Expiry         int64  `json:"exp"`
	WebhookURL     string `json:"chatWebhookUrl"` // where node POSTs offline-fallback envelopes
	ChatSend       bool   `json:"chatSend"`
	ChatReceive    bool   `json:"chatReceive"`
}

// chatClaimsKey is the unexported context key for stashing verified claims.
type chatClaimsKey struct{}

// chatTokenKey stashes the original raw JWT for downstream code that needs to
// re-present it (none in v1, but useful for debugging).
type chatTokenKey struct{}

// GetClaims returns the verified ChatClaims attached by ChatTokenMiddleware,
// or nil if none.
func GetClaims(ctx context.Context) *ChatClaims {
	v, _ := ctx.Value(chatClaimsKey{}).(*ChatClaims)
	return v
}

// GetToken returns the raw JWT string used for this request, or empty.
func GetToken(ctx context.Context) string {
	v, _ := ctx.Value(chatTokenKey{}).(string)
	return v
}

// VerifyChatToken parses + verifies a chat-token JWT against the Solana-registry
// signer, enforces typ/iat/exp, and returns the claims. Used both by the WS
// upgrade middleware and directly by tests.
//
// `now` is injected for deterministic tests; pass time.Now().Unix() in prod.
func VerifyChatToken(
	ctx context.Context,
	rawJWT string,
	lookup ClientLookup,
	now int64,
) (*ChatClaims, error) {
	tok, err := jwt.ParseSigned(rawJWT)
	if err != nil {
		return nil, fmt.Errorf("parse jwt: %w", err)
	}

	// Inspect issuer without verifying first — same pattern as
	// pkg/service/auth.go:80 (production room-token flow).
	var iss struct {
		Iss string `json:"iss"`
	}
	if err := tok.UnsafeClaimsWithoutVerification(&iss); err != nil {
		return nil, fmt.Errorf("read iss: %w", err)
	}
	if iss.Iss == "" {
		return nil, errors.New("missing iss claim")
	}

	res, err := lookup.ClientByAddress(ctx, iss.Iss)
	if err != nil {
		return nil, fmt.Errorf("lookup signer %s: %w", iss.Iss, err)
	}
	if res.Key == "" {
		return nil, fmt.Errorf("signer %s not in registry", iss.Iss)
	}

	pubBytes, err := solana.PublicKeyFromBase58(res.Key)
	if err != nil {
		return nil, fmt.Errorf("decode pubkey: %w", err)
	}
	pubKey := ed25519.PublicKey(pubBytes[:])

	claims := ChatClaims{}
	if err := tok.Claims(pubKey, &claims); err != nil {
		return nil, fmt.Errorf("verify signature: %w", err)
	}

	if claims.Type != "chat" {
		return nil, fmt.Errorf("expected typ=chat, got %q (likely a room token presented to chat endpoint)", claims.Type)
	}
	if claims.Issuer != iss.Iss {
		return nil, errors.New("iss mismatch between header and verified body")
	}
	if claims.Subject == "" {
		return nil, errors.New("missing sub claim")
	}
	if claims.DeviceID == "" {
		return nil, errors.New("missing did claim")
	}
	if claims.WebhookURL == "" {
		return nil, errors.New("missing chatWebhookUrl claim")
	}
	if claims.Expiry == 0 || now >= claims.Expiry {
		return nil, errors.New("token expired")
	}
	if claims.IssuedAt > now+60 {
		return nil, errors.New("token issued in the future (clock skew >60s)")
	}

	return &claims, nil
}

// ChatTokenMiddleware wraps an http.Handler with chat-token verification.
// Used as a per-route decorator on /chat/ws (mirrors how server.go wraps
// rtcservice with refuseIfShuttingDown).
//
// On success: stamps *ChatClaims and the raw token into the request context,
// then calls next. On failure: writes 401 and short-circuits.
func ChatTokenMiddleware(next http.Handler, lookup ClientLookup) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, err := extractBearer(r)
		if err != nil {
			httpError(w, http.StatusUnauthorized, err)
			return
		}
		claims, err := VerifyChatToken(r.Context(), token, lookup, time.Now().Unix())
		if err != nil {
			httpError(w, http.StatusUnauthorized, err)
			return
		}
		ctx := context.WithValue(r.Context(), chatClaimsKey{}, claims)
		ctx = context.WithValue(ctx, chatTokenKey{}, token)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func extractBearer(r *http.Request) (string, error) {
	h := r.Header.Get("Authorization")
	if h == "" {
		// WebSocket clients in browsers can't easily set headers on the upgrade,
		// so accept ?access_token=… as a fallback (matches existing room-token
		// path at pkg/service/auth.go:59).
		if t := r.URL.Query().Get("access_token"); t != "" {
			return t, nil
		}
		return "", errors.New("missing Authorization header")
	}
	if !strings.HasPrefix(h, "Bearer ") {
		return "", errors.New(`Authorization must start with "Bearer "`)
	}
	return strings.TrimSpace(h[len("Bearer "):]), nil
}

func httpError(w http.ResponseWriter, status int, err error) {
	http.Error(w, err.Error(), status)
}
