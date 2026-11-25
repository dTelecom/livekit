package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/p2p"
	"github.com/livekit/livekit-server/pkg/utils"
	"github.com/livekit/protocol/livekit"
)

func TestLocalStoreRoomInternal(t *testing.T) {
	ctx := context.Background()
	store := NewLocalStore("node", nil)
	key := utils.RoomKey("myroom", "tenant")
	room := &livekit.Room{Name: "myroom", Sid: "room-id", Key: string(key)}
	internal := &livekit.RoomInternal{TrackEgress: &livekit.AutoTrackEgress{Filepath: "egress"}}

	// An existing room's communicator is reused on metadata updates. Its transport
	// is outside the storage test; opening it requires a joined-node fixture.
	communicator := &p2p.RoomCommunicatorImpl{}
	store.roomCommunicators[key] = communicator
	require.NoError(t, store.StoreRoom(ctx, room, key, internal))
	got, gotInternal, gotCommunicator, err := store.LoadRoom(ctx, key, true)
	require.NoError(t, err)
	require.Equal(t, room.Sid, got.Sid)
	require.Equal(t, internal.TrackEgress.Filepath, gotInternal.TrackEgress.Filepath)
	require.Same(t, communicator, gotCommunicator)

	_, gotInternal, _, err = store.LoadRoom(ctx, key, false)
	require.NoError(t, err)
	require.Nil(t, gotInternal)

	require.NoError(t, store.StoreRoom(ctx, room, key, nil))
	_, gotInternal, _, err = store.LoadRoom(ctx, key, true)
	require.NoError(t, err)
	require.Nil(t, gotInternal)
}

func TestValidateExistingRoomWhenAutoCreateDisabled(t *testing.T) {
	ctx := context.Background()
	store := NewLocalStore("node", nil)
	key := utils.RoomKey("myroom", "tenant")
	store.rooms[key] = &livekit.Room{Name: "myroom", Key: string(key)}
	conf, err := config.NewConfig("rtc:\n  node_ip: 127.0.0.1\nroom:\n  auto_create: false", true, nil, nil)
	require.NoError(t, err)
	allocator, err := NewRoomAllocator(conf, nil, store)
	require.NoError(t, err)
	require.NoError(t, allocator.ValidateCreateRoom(ctx, key))
	require.ErrorIs(t, allocator.ValidateCreateRoom(ctx, utils.RoomKey("myroom", "other-tenant")), ErrRoomNotFound)
}

func TestParticipantPersistence(t *testing.T) {
	ctx := context.Background()
	rs := NewLocalStore("node", nil)

	roomKey := utils.RoomKey("room1", "tenant")

	p := &livekit.ParticipantInfo{
		Sid:      "PA_test",
		Identity: "test",
		State:    livekit.ParticipantInfo_ACTIVE,
		Tracks: []*livekit.TrackInfo{
			{
				Sid:  "track1",
				Type: livekit.TrackType_AUDIO,
				Name: "audio",
			},
		},
	}

	// create the participant
	require.NoError(t, rs.StoreParticipant(ctx, roomKey, p))

	// result should match
	pGet, err := rs.LoadParticipant(ctx, roomKey, livekit.ParticipantIdentity(p.Identity))
	require.NoError(t, err)
	require.Equal(t, p.Identity, pGet.Identity)
	require.Equal(t, len(p.Tracks), len(pGet.Tracks))
	require.Equal(t, p.Tracks[0].Sid, pGet.Tracks[0].Sid)

	// The same participant identity in another tenant has a separate record.
	otherKey := utils.RoomKey("room1", "other-tenant")
	_, err = rs.LoadParticipant(ctx, otherKey, livekit.ParticipantIdentity(p.Identity))
	require.ErrorIs(t, err, ErrParticipantNotFound)

	// list should return one participant
	participants, err := rs.ListParticipants(ctx, roomKey)
	require.NoError(t, err)
	require.Len(t, participants, 1)

	// deleting participant should return to normal
	require.NoError(t, rs.DeleteParticipant(ctx, roomKey, livekit.ParticipantIdentity(p.Identity)))

	participants, err = rs.ListParticipants(ctx, roomKey)
	require.NoError(t, err)
	require.Len(t, participants, 0)

	// shouldn't be able to get it
	_, err = rs.LoadParticipant(ctx, roomKey, livekit.ParticipantIdentity(p.Identity))
	require.ErrorIs(t, err, ErrParticipantNotFound)
}

func TestLocalStoreRoomLock(t *testing.T) {
	store := NewLocalStore("node", nil)
	ctx := context.Background()
	key := utils.RoomKey("myroom", "tenant")
	token, err := store.LockRoom(ctx, key, time.Second)
	require.NoError(t, err)
	// The local implementation holds a process-wide mutex until explicit unlock.
	require.False(t, store.globalLock.TryLock())
	require.NoError(t, store.UnlockRoom(ctx, key, token))
	require.True(t, store.globalLock.TryLock())
	store.globalLock.Unlock()
}
