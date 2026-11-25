package service

import (
	"context"
	"errors"
	"testing"

	"github.com/dTelecom/p2p-database/common"
	"github.com/livekit/livekit-server/pkg/p2p"
	"github.com/livekit/protocol/livekit"
	"github.com/stretchr/testify/require"
)

type occupiedRoomTopic struct {
	p2p.RoomPubSub
	occupied     bool
	unsubscribed bool
}

func (db *occupiedRoomTopic) Subscribe(context.Context, string, common.PubSubHandler) error {
	if db.occupied {
		return errors.New("already subscribed")
	}
	return nil
}
func (db *occupiedRoomTopic) Unsubscribe(context.Context, string) error {
	db.unsubscribed = true
	return nil
}

func TestRoomSubscriptionFailurePreservesOwner(t *testing.T) {
	ctx := context.Background()
	store := NewLocalStore("node", nil)
	db := &occupiedRoomTopic{occupied: true}
	store.db = db
	err := store.StoreRoom(ctx, &livekit.Room{Name: "room"}, "room-key", &livekit.RoomInternal{})
	require.ErrorContains(t, err, "already subscribed")
	require.False(t, db.unsubscribed, "a failed subscriber must not remove another subscription")
	_, _, _, err = store.LoadRoom(ctx, "room-key", true)
	require.ErrorIs(t, err, ErrRoomNotFound)

	db.occupied = false
	require.NoError(t, store.StoreRoom(ctx, &livekit.Room{Name: "room"}, "room-key", nil))
	_, _, communicator, err := store.LoadRoom(ctx, "room-key", true)
	require.NoError(t, err)
	require.NotNil(t, communicator)
	require.NoError(t, store.DeleteRoom(ctx, "room-key"))
	require.True(t, db.unsubscribed, "removing the successful retry must release its subscription")
}
