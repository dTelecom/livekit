package rtc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
)

func TestRemovingRelayedParticipantReportsDisconnectedToRoom(t *testing.T) {
	room := newRoomWithParticipants(t, testRoomOpts{})
	t.Cleanup(room.Close)
	p, err := NewRelayedParticipant(RelayedParticipantParams{
		Identity: "remote", SID: "PA_remote", Logger: logger.GetLogger(),
	})
	require.NoError(t, err)
	require.NoError(t, room.Join(p, nil, &ParticipantOptions{}, nil))
	require.False(t, p.IsDisconnected())

	closed := false
	p.OnClose(func(types.LocalParticipant, map[livekit.TrackID]livekit.ParticipantID) {
		closed = true
	})
	// The room manager persists participants that report themselves connected in this callback.
	var finalUpdate types.LocalParticipant
	room.OnParticipantChanged(func(changed types.LocalParticipant) {
		if closed {
			finalUpdate = changed
		}
	})
	room.RemoveParticipant(p.Identity(), p.ID(), types.ParticipantCloseReasonServiceRequestDeleteRoom)
	require.True(t, closed)
	require.Same(t, p, finalUpdate)
	require.Equal(t, livekit.ParticipantInfo_DISCONNECTED, finalUpdate.State())
	require.True(t, finalUpdate.IsDisconnected(), "a disconnected peer would be stored again after its close callback")
}
