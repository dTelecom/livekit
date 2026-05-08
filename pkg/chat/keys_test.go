package chat

import "testing"

func TestEncodeUserKeyRoundtrip(t *testing.T) {
	for _, tc := range []struct {
		name, api, user, dev string
	}{
		{"basic", "TENANT-PUBKEY-ABC", "user-alice", "550e8400-e29b-41d4-a716-446655440000"},
		{"unicode user", "tenant", "ユーザー", "dev-1"},
		{"with separator chars in id", "T|with|pipes", "u/with/slashes", "d.with.dots"},
		{"empty-ish", "a", "b", "c"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := EncodeUserKey(tc.api, tc.user, tc.dev)
			gotAPI, gotUser, gotDev, err := ParseUserKey(k)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if gotAPI != tc.api || gotUser != tc.user || gotDev != tc.dev {
				t.Errorf("roundtrip mismatch: got (%q, %q, %q) want (%q, %q, %q)",
					gotAPI, gotUser, gotDev, tc.api, tc.user, tc.dev)
			}
		})
	}
}

func TestEncodeUserKeyTenantIsolation(t *testing.T) {
	// Same userID/deviceID under two different api keys must produce different keys.
	a := EncodeUserKey("tenant-A", "alice", "dev1")
	b := EncodeUserKey("tenant-B", "alice", "dev1")
	if a == b {
		t.Errorf("cross-tenant collision: %s == %s", a, b)
	}
}

func TestParseUserKeyInvalid(t *testing.T) {
	_, _, _, err := ParseUserKey(ChatUserKey("only-two|segments"))
	if err == nil {
		t.Error("expected error for 2-segment key")
	}
}

func TestEnvelopeTopicParse(t *testing.T) {
	topic := EnvelopeTopic("api-X", "user-Y", "dev-Z")
	gotAPI, gotUser, gotDev, err := ParseEnvelopeTopic(topic)
	if err != nil {
		t.Fatalf("parse envelope topic: %v", err)
	}
	if gotAPI != "api-X" || gotUser != "user-Y" || gotDev != "dev-Z" {
		t.Errorf("parse mismatch: %s/%s/%s", gotAPI, gotUser, gotDev)
	}
}

func TestUserPresenceQueryTopic(t *testing.T) {
	a := UserPresenceQueryTopic("apiK", "alice")
	b := UserPresenceQueryTopic("apiK", "bob")
	c := UserPresenceQueryTopic("apiK2", "alice")
	if a == b || a == c || b == c {
		t.Errorf("presence query topics should be distinct per (api,user): %s %s %s", a, b, c)
	}
}
