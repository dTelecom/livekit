package service

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"

	"github.com/livekit/livekit-server/pkg/config"
)

func TestAuthMiddleware(t *testing.T) {
	wallet := solana.NewWallet()
	api := wallet.PublicKey().String()
	provider := NewClientProvider(config.SolanaConfig{})
	provider.clientValues[api] = clientMessage{
		Client: Client{Key: api},
		TTL:    time.Now().Add(time.Minute).Unix(),
	}
	m := NewAPIKeyAuthMiddleware(provider)
	var grants *auth.ClaimGrants
	var issuer livekit.ApiKey
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		grants = GetGrants(r.Context())
		issuer = GetApiKey(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	orig := &auth.VideoGrant{Room: "abcdefg", RoomJoin: true}
	// ensure that the original claim could be retrieved
	at := auth.NewAccessToken(api, wallet.PrivateKey.String()).
		AddGrant(orig)
	token, err := at.ToJWT()
	require.NoError(t, err)

	r := httptest.NewRequest(http.MethodGet, "/rtc/validate", nil)
	w := httptest.NewRecorder()
	SetAuthorizationToken(r, token)
	m.ServeHTTP(w, r, handler)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, livekit.ApiKey(api), issuer)
	require.NotNil(t, grants)
	require.EqualValues(t, orig, grants.Video)

	// no authorization == no claims
	grants = nil
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/rtc/validate", nil)
	m.ServeHTTP(w, r, handler)
	require.Nil(t, grants)
	require.Equal(t, http.StatusOK, w.Code)

	// incorrect authorization: error
	grants = nil
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/rtc/validate", nil)
	SetAuthorizationToken(r, "invalid token")
	m.ServeHTTP(w, r, handler)
	require.Nil(t, grants)
	require.Equal(t, http.StatusUnauthorized, w.Code)
}
