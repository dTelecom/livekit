package routing

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/livekit"
)

func TestUtils_ParticipantKey(t *testing.T) {
	// encode/decode empty
	encoded := ParticipantKey("", "")
	roomKey, identity, err := parseParticipantKey(encoded)
	require.NoError(t, err)
	require.Equal(t, livekit.RoomKey(""), roomKey)
	require.Equal(t, livekit.ParticipantIdentity(""), identity)

	// decode invalid
	_, _, err = parseParticipantKey("abcd")
	require.Error(t, err)

	// encode/decode without delimiter
	encoded = ParticipantKey("room1", "identity1")
	roomKey, identity, err = parseParticipantKey(encoded)
	require.NoError(t, err)
	require.Equal(t, livekit.RoomKey("room1"), roomKey)
	require.Equal(t, livekit.ParticipantIdentity("identity1"), identity)

	// encode/decode with delimiter in roomKey
	encoded = ParticipantKey("room1|alter_room1", "identity1")
	roomKey, identity, err = parseParticipantKey(encoded)
	require.NoError(t, err)
	require.Equal(t, livekit.RoomKey("room1|alter_room1"), roomKey)
	require.Equal(t, livekit.ParticipantIdentity("identity1"), identity)

	// encode/decode with delimiter in identity
	encoded = ParticipantKey("room1", "identity1|alter-identity1")
	roomKey, identity, err = parseParticipantKey(encoded)
	require.NoError(t, err)
	require.Equal(t, livekit.RoomKey("room1"), roomKey)
	require.Equal(t, livekit.ParticipantIdentity("identity1|alter-identity1"), identity)

	// encode/decode with delimiter in both and multiple delimiters in both
	encoded = ParticipantKey("room1|alter_room1|again_room1", "identity1|alter-identity1|again-identity1")
	roomKey, identity, err = parseParticipantKey(encoded)
	require.NoError(t, err)
	require.Equal(t, livekit.RoomKey("room1|alter_room1|again_room1"), roomKey)
	require.Equal(t, livekit.ParticipantIdentity("identity1|alter-identity1|again-identity1"), identity)
}
