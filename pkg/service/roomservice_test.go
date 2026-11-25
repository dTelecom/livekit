package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twitchtv/twirp"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing/routingfakes"
	"github.com/livekit/livekit-server/pkg/service"
	"github.com/livekit/livekit-server/pkg/service/servicefakes"
)

func TestDeleteRoom(t *testing.T) {
	t.Run("normal deletion", func(t *testing.T) {
		svc := newTestRoomService(config.RoomConfig{})
		grant := &auth.ClaimGrants{
			Video: &auth.VideoGrant{
				RoomCreate: true,
			},
		}
		ctx := service.WithGrants(context.Background(), grant)
		svc.store.LoadRoomReturns(nil, nil, nil, service.ErrRoomNotFound)
		_, err := svc.DeleteRoom(ctx, &livekit.DeleteRoomRequest{
			Room: "testroom",
		})
		require.NoError(t, err)
	})

	t.Run("missing permissions", func(t *testing.T) {
		svc := newTestRoomService(config.RoomConfig{})
		grant := &auth.ClaimGrants{
			Video: &auth.VideoGrant{},
		}
		ctx := service.WithGrants(context.Background(), grant)
		_, err := svc.DeleteRoom(ctx, &livekit.DeleteRoomRequest{
			Room: "testroom",
		})
		require.Error(t, err)
	})
}

func TestMetaDataLimits(t *testing.T) {
	ctx := service.WithGrants(context.Background(), &auth.ClaimGrants{
		Video: &auth.VideoGrant{Room: "testroom", RoomAdmin: true},
	})
	for _, tc := range []struct {
		name            string
		limit           uint32
		metadata        string
		wantRoomInvalid bool
	}{
		{"above limit", 5, "abcdef", true},
		{"at limit", 5, "abcde", false},
		{"unlimited", 0, "abcdef", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := newTestRoomService(config.RoomConfig{MaxMetadataSize: tc.limit})
			svc.store.LoadRoomReturns(nil, nil, nil, service.ErrRoomNotFound)
			t.Run("room", func(t *testing.T) {
				_, err := svc.UpdateRoomMetadata(ctx, &livekit.UpdateRoomMetadataRequest{
					Room: "testroom", Metadata: tc.metadata,
				})
				if tc.wantRoomInvalid {
					var terr twirp.Error
					require.ErrorAs(t, err, &terr)
					require.Equal(t, twirp.InvalidArgument, terr.Code())
					require.Zero(t, svc.router.WriteRoomRTCCallCount())
				} else {
					require.NoError(t, err)
					require.Equal(t, 1, svc.router.WriteRoomRTCCallCount())
				}
			})
			t.Run("participant", func(t *testing.T) {
				_, err := svc.UpdateParticipant(ctx, &livekit.UpdateParticipantRequest{
					Room: "testroom", Identity: "123", Metadata: tc.metadata,
				})
				require.NoError(t, err)
				require.Equal(t, 1, svc.router.WriteParticipantRTCCallCount())
				_, _, _, msg := svc.router.WriteParticipantRTCArgsForCall(0)
				require.Equal(t, tc.metadata, msg.GetUpdateParticipant().GetMetadata())
			})
		})
	}
}

func newTestRoomService(conf config.RoomConfig) *TestRoomService {
	router := &routingfakes.FakeRouter{}
	allocator := &servicefakes.FakeRoomAllocator{}
	store := &servicefakes.FakeServiceStore{}
	svc, err := service.NewRoomService(conf,
		config.APIConfig{ExecutionTimeout: time.Second, CheckInterval: time.Millisecond},
		router, allocator, store, nil)
	if err != nil {
		panic(err)
	}
	return &TestRoomService{
		RoomService: *svc,
		router:      router,
		allocator:   allocator,
		store:       store,
	}
}

type TestRoomService struct {
	service.RoomService
	router    *routingfakes.FakeRouter
	allocator *servicefakes.FakeRoomAllocator
	store     *servicefakes.FakeServiceStore
}
