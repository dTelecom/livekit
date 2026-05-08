package chat

import "fmt"

// Gossipsub topic prefixes. Topic strings include base62-encoded apiKey so
// cross-tenant streams are physically separate.
const (
	envelopeTopicPrefix      = "chat:envelope:"
	userPresenceQueryPrefix  = "chat:user-presence-query:"
)

// EnvelopeTopic returns the per-(user,device) gossipsub topic for live
// envelope delivery. Sender's node publishes; the node hosting that device's
// live WS subscribes and forwards the envelope to the WS.
func EnvelopeTopic(apiKey, userID, deviceID string) string {
	return envelopeTopicPrefix + string(EncodeUserKey(apiKey, userID, deviceID))
}

// UserPresenceQueryTopic returns the per-USER gossipsub topic used for the
// lazy "does any node host any live device of this user?" mesh query, run
// only at offline-fallback decision time.
func UserPresenceQueryTopic(apiKey, userID string) string {
	return userPresenceQueryPrefix + EncodeTenantUserKey(apiKey, userID)
}

// ParseEnvelopeTopic is the inverse of EnvelopeTopic. Used when a node
// receives a published envelope and needs to know which local WS owns it.
func ParseEnvelopeTopic(topic string) (apiKey, userID, deviceID string, err error) {
	suffix, ok := trimPrefix(topic, envelopeTopicPrefix)
	if !ok {
		err = fmt.Errorf("not an envelope topic: %s", topic)
		return
	}
	return ParseUserKey(ChatUserKey(suffix))
}

func trimPrefix(s, prefix string) (string, bool) {
	if len(s) < len(prefix) || s[:len(prefix)] != prefix {
		return "", false
	}
	return s[len(prefix):], true
}
