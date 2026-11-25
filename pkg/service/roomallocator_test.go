package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/livekit"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/routing/routingfakes"
	"github.com/livekit/livekit-server/pkg/service"
	"github.com/livekit/livekit-server/pkg/service/servicefakes"
	"github.com/livekit/livekit-server/pkg/utils"
)

func TestCreateRoom(t *testing.T) {
	t.Run("ensure default room settings are applied", func(t *testing.T) {
		conf, err := config.NewConfig("rtc:\n  node_ip: 127.0.0.1", true, nil, nil)
		require.NoError(t, err)

		node := &livekit.Node{Stats: &livekit.NodeStats{UpdatedAt: time.Now().Unix()}}

		ra := newTestRoomAllocator(t, conf, node)

		room, err := ra.CreateRoom(context.Background(), &livekit.CreateRoomRequest{Name: "myroom"}, "tenant")
		require.NoError(t, err)
		require.Equal(t, conf.Room.EmptyTimeout, room.EmptyTimeout)
		require.NotEmpty(t, room.EnabledCodecs)
		require.Equal(t, string(utils.RoomKey("myroom", "tenant")), room.Key)
	})

	t.Run("reject new participants when track limit has been reached", func(t *testing.T) {
		conf, err := config.NewConfig("rtc:\n  node_ip: 127.0.0.1", true, nil, nil)
		require.NoError(t, err)
		conf.Limit.NumTracks = 10

		node := &livekit.Node{Stats: &livekit.NodeStats{UpdatedAt: time.Now().Unix()}}
		node.Stats.NumTracksIn = 100
		node.Stats.NumTracksOut = 100

		ra := newTestRoomAllocator(t, conf, node)

		_, err = ra.CreateRoom(context.Background(), &livekit.CreateRoomRequest{Name: "low-limit-room"}, "tenant")
		require.ErrorIs(t, err, routing.ErrNodeLimitReached)
	})

	t.Run("reject new participants when bandwidth limit has been reached", func(t *testing.T) {
		conf, err := config.NewConfig("rtc:\n  node_ip: 127.0.0.1", true, nil, nil)
		require.NoError(t, err)
		conf.Limit.BytesPerSec = 100

		node := &livekit.Node{Stats: &livekit.NodeStats{UpdatedAt: time.Now().Unix()}}
		node.Stats.BytesInPerSec = 1000
		node.Stats.BytesOutPerSec = 1000

		ra := newTestRoomAllocator(t, conf, node)

		_, err = ra.CreateRoom(context.Background(), &livekit.CreateRoomRequest{Name: "low-limit-room"}, "tenant")
		require.ErrorIs(t, err, routing.ErrNodeLimitReached)
	})
}

func newTestRoomAllocator(t *testing.T, conf *config.Config, node *livekit.Node) service.RoomAllocator {
	t.Helper()
	store := &servicefakes.FakeObjectStore{}
	store.LoadRoomReturns(nil, nil, nil, service.ErrRoomNotFound)
	router := &routingfakes.FakeRouter{}

	router.GetNodeForRoomReturns(node, nil)

	ra, err := service.NewRoomAllocator(conf, router, store)
	require.NoError(t, err)
	return ra
}
