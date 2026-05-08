// Package chat implements a stateless live-mesh layer for E2E-encrypted 1:1
// chat. The node routes opaque ciphertext envelopes between currently-connected
// (user, device) WebSockets via gossipsub; offline fallback is delegated to a
// tenant backend referenced by each chat token's webhook URL claim. No durable
// storage on the node.
package chat

import (
	"fmt"
	"strings"

	"github.com/jxskiss/base62"
)

// ChatUserKey identifies a single (tenant, user, device) tuple. Used as the
// suffix of gossipsub topic strings (chat:envelope:<key>) and as the index
// for node-local presence maps. Mirrors pkg/utils/roomKey.go's pattern but
// extended with a deviceID segment.
type ChatUserKey string

// EncodeUserKey produces a ChatUserKey from (apiKey, userID, deviceID).
// The encoding is base62 per segment, joined with '|'. Cross-tenant keys
// can never collide because apiKey is part of the encoded prefix.
func EncodeUserKey(apiKey, userID, deviceID string) ChatUserKey {
	return ChatUserKey(encode(apiKey, userID, deviceID))
}

// EncodeTenantUserKey is the per-USER (not per-device) key, used for the
// mesh user-presence-query topic. Answer to "do I host any device of this
// user?" is the only question that needs this granularity.
func EncodeTenantUserKey(apiKey, userID string) string {
	return encode(apiKey, userID)
}

// ParseUserKey is the inverse of EncodeUserKey.
func ParseUserKey(k ChatUserKey) (apiKey, userID, deviceID string, err error) {
	parts, err := decode(string(k))
	if err != nil {
		return
	}
	if len(parts) != 3 {
		err = fmt.Errorf("invalid chat user key: %s (expected 3 segments, got %d)", k, len(parts))
		return
	}
	apiKey, userID, deviceID = parts[0], parts[1], parts[2]
	return
}

func encode(parts ...string) string {
	encoded := make([]string, 0, len(parts))
	for _, s := range parts {
		encoded = append(encoded, base62.EncodeToString([]byte(s)))
	}
	return strings.Join(encoded, "|")
}

func decode(s string) ([]string, error) {
	split := strings.Split(s, "|")
	out := make([]string, 0, len(split))
	for _, seg := range split {
		dec, err := base62.DecodeString(seg)
		if err != nil {
			return nil, fmt.Errorf("base62 decode segment %q: %w", seg, err)
		}
		out = append(out, string(dec))
	}
	return out, nil
}
