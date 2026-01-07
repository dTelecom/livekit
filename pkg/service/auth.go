package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/twitchtv/twirp"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
)

const (
	authorizationHeader = "Authorization"
	bearerPrefix        = "Bearer "
	accessTokenParam    = "access_token"
)

type grantsKey struct{}
type apiKeyKey struct{}
type limitKey struct{}
type tokenKey struct{}

var (
	ErrPermissionDenied          = errors.New("permissions denied")
	ErrMissingAuthorization      = errors.New("invalid authorization header. Must start with " + bearerPrefix)
	ErrInvalidAuthorizationToken = errors.New("invalid authorization token")
)

// authentication middleware
type APIKeyAuthMiddleware struct {
	clientProvider *ClientProvider
}

func NewAPIKeyAuthMiddleware(clientProvider *ClientProvider) *APIKeyAuthMiddleware {
	return &APIKeyAuthMiddleware{
		clientProvider: clientProvider,
	}
}

func (m *APIKeyAuthMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	if r.URL != nil && r.URL.Path == "/rtc/validate" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
	}

	var tokenParseError error
	var parsedToken *auth.APIKeyTokenVerifier
	var token string
	var skipAuth bool = false

	if r.URL != nil && r.URL.Path == "/whip" && r.Method == http.MethodDelete {
		skipAuth = true
	}

	// attempt to find from request params
	authTokenParams := r.FormValue(accessTokenParam)
	if authTokenParams != "" {
		parsedToken, tokenParseError = auth.ParseAPIToken(authTokenParams)
		if tokenParseError == nil {
			token = authTokenParams
		}
	}

	if parsedToken == nil {
		authHeader := r.Header.Get(authorizationHeader)
		if authHeader != "" {
			if strings.HasPrefix(authHeader, bearerPrefix) {
				authTokenHeader := authHeader[len(bearerPrefix):]
				parsedToken, tokenParseError = auth.ParseAPIToken(authTokenHeader)
				if tokenParseError == nil {
					token = authTokenHeader
				}
			}
		}
	}

	if parsedToken != nil {
		apiKey := parsedToken.APIKey()

		client, err := m.clientProvider.ClientByAddress(r.Context(), apiKey)
		if err != nil {
			handleError(w, http.StatusUnauthorized, errors.New(fmt.Sprintf("wallet %s not exists in contract, err: %s", apiKey, err)))
			return
		}

		if client.Key == "" {
			handleError(w, http.StatusUnauthorized, errors.New(fmt.Sprintf("wallet %s not exists in contract", apiKey)))
			return
		}

		grants, err := parsedToken.Verify(client.Key)
		if err != nil {
			handleError(w, http.StatusUnauthorized, fmt.Errorf("invalid token: %s, error: %s", apiKey, err))
			return
		}

		// set grants in context
		ctx := context.WithValue(r.Context(), grantsKey{}, grants)
		ctx = context.WithValue(ctx, apiKeyKey{}, apiKey)
		ctx = context.WithValue(ctx, limitKey{}, client.Limit)
		ctx = context.WithValue(ctx, tokenKey{}, token)

		r = r.WithContext(ctx)
	} else {
		if skipAuth == false {
			if tokenParseError != nil {
				handleError(w, http.StatusUnauthorized, ErrInvalidAuthorizationToken)
				return
			}
		}
	}

	next.ServeHTTP(w, r)
}

func GetGrants(ctx context.Context) *auth.ClaimGrants {
	val := ctx.Value(grantsKey{})
	claims, ok := val.(*auth.ClaimGrants)
	if !ok {
		return nil
	}
	return claims
}

func GetApiKey(ctx context.Context) livekit.ApiKey {
	val := ctx.Value(apiKeyKey{})
	apiKey, ok := val.(string)
	if !ok {
		return ""
	}
	return livekit.ApiKey(apiKey)
}

func GetLimit(ctx context.Context) int64 {
	val := ctx.Value(limitKey{})
	limit, ok := val.(int64)
	if !ok {
		return 0
	}
	return limit
}

func GetToken(ctx context.Context) string {
	val := ctx.Value(tokenKey{})
	token, ok := val.(string)
	if !ok {
		return ""
	}
	return token
}

func WithGrants(ctx context.Context, grants *auth.ClaimGrants) context.Context {
	return context.WithValue(ctx, grantsKey{}, grants)
}

func SetAuthorizationToken(r *http.Request, token string) {
	r.Header.Set(authorizationHeader, bearerPrefix+token)
}

func EnsureJoinPermission(ctx context.Context) (name livekit.RoomName, err error) {
	claims := GetGrants(ctx)
	if claims == nil || claims.Video == nil {
		err = ErrPermissionDenied
		return
	}

	if claims.Video.RoomJoin {
		name = livekit.RoomName(claims.Video.Room)
	} else {
		err = ErrPermissionDenied
	}
	return
}

func EnsureAdminPermission(ctx context.Context, room livekit.RoomName) error {
	claims := GetGrants(ctx)
	if claims == nil || claims.Video == nil {
		return ErrPermissionDenied
	}

	if !claims.Video.RoomAdmin || room != livekit.RoomName(claims.Video.Room) {
		return ErrPermissionDenied
	}

	return nil
}

func EnsureCreatePermission(ctx context.Context) error {
	claims := GetGrants(ctx)
	if claims == nil || claims.Video == nil || !claims.Video.RoomCreate {
		return ErrPermissionDenied
	}
	return nil
}

func EnsureListPermission(ctx context.Context) error {
	claims := GetGrants(ctx)
	if claims == nil || claims.Video == nil || !claims.Video.RoomList {
		return ErrPermissionDenied
	}
	return nil
}

func EnsureRecordPermission(ctx context.Context) error {
	claims := GetGrants(ctx)
	if claims == nil || claims.Video == nil || !claims.Video.RoomRecord {
		return ErrPermissionDenied
	}
	return nil
}

func EnsureIngressAdminPermission(ctx context.Context) error {
	claims := GetGrants(ctx)
	if claims == nil || claims.Video == nil || !claims.Video.IngressAdmin {
		return ErrPermissionDenied
	}
	return nil
}

// wraps authentication errors around Twirp
func twirpAuthError(err error) error {
	return twirp.NewError(twirp.Unauthenticated, err.Error())
}
