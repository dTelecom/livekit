package service

import (
	"context"
	"time"

	"github.com/thoas/go-funk"

	"github.com/livekit/protocol/livekit"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/utils"
)

// A rooms service that supports a single node
type RoomService struct {
	roomConf       config.RoomConfig
	apiConf        config.APIConfig
	router         routing.MessageRouter
	roomAllocator  RoomAllocator
	roomStore      ServiceStore
	egressLauncher rtc.EgressLauncher
}

func NewRoomService(
	roomConf config.RoomConfig,
	apiConf config.APIConfig,
	router routing.MessageRouter,
	roomAllocator RoomAllocator,
	serviceStore ServiceStore,
	egressLauncher rtc.EgressLauncher,
) (svc *RoomService, err error) {
	svc = &RoomService{
		roomConf:       roomConf,
		apiConf:        apiConf,
		router:         router,
		roomAllocator:  roomAllocator,
		roomStore:      serviceStore,
		egressLauncher: egressLauncher,
	}
	return
}

func (s *RoomService) DeleteRoom(ctx context.Context, req *livekit.DeleteRoomRequest) (*livekit.DeleteRoomResponse, error) {
	apiKey := GetApiKey(ctx)
	roomKey := utils.RoomKey(livekit.RoomName(req.Room), apiKey)

	AppendLogFields(ctx, "room", req.Room)
	if err := EnsureCreatePermission(ctx); err != nil {
		return nil, twirpAuthError(err)
	}
	err := s.router.WriteRoomRTC(ctx, roomKey, &livekit.RTCNodeMessage{
		Message: &livekit.RTCNodeMessage_DeleteRoom{
			DeleteRoom: req,
		},
	})
	if err != nil {
		return nil, err
	}

	// we should not return until when the room is confirmed deleted
	err = s.confirmExecution(func() error {
		_, _, _, err := s.roomStore.LoadRoom(ctx, roomKey, false)
		if err == nil {
			return ErrOperationFailed
		} else if err != ErrRoomNotFound {
			return err
		} else {
			return nil
		}
	})
	if err != nil {
		return nil, err
	}

	return &livekit.DeleteRoomResponse{}, nil
}

func (s *RoomService) RemoveParticipant(ctx context.Context, req *livekit.RoomParticipantIdentity) (*livekit.RemoveParticipantResponse, error) {
	apiKey := GetApiKey(ctx)
	roomKey := utils.RoomKey(livekit.RoomName(req.Room), apiKey)

	AppendLogFields(ctx, "room", req.Room, "participant", req.Identity)
	err := s.writeParticipantMessage(ctx, livekit.RoomName(req.Room), roomKey, livekit.ParticipantIdentity(req.Identity), &livekit.RTCNodeMessage{
		Message: &livekit.RTCNodeMessage_RemoveParticipant{
			RemoveParticipant: req,
		},
	})
	if err != nil {
		return nil, err
	}

	err = s.confirmExecution(func() error {
		_, err := s.roomStore.LoadParticipant(ctx, roomKey, livekit.ParticipantIdentity(req.Identity))
		if err == ErrParticipantNotFound {
			return nil
		} else if err != nil {
			return err
		} else {
			return ErrOperationFailed
		}
	})
	if err != nil {
		return nil, err
	}

	return &livekit.RemoveParticipantResponse{}, nil
}

func (s *RoomService) MutePublishedTrack(ctx context.Context, req *livekit.MuteRoomTrackRequest) (*livekit.MuteRoomTrackResponse, error) {
	apiKey := GetApiKey(ctx)
	roomKey := utils.RoomKey(livekit.RoomName(req.Room), apiKey)

	AppendLogFields(ctx, "room", req.Room, "participant", req.Identity, "track", req.TrackSid, "muted", req.Muted)

	err := s.writeParticipantMessage(ctx, livekit.RoomName(req.Room), roomKey, livekit.ParticipantIdentity(req.Identity), &livekit.RTCNodeMessage{
		Message: &livekit.RTCNodeMessage_MuteTrack{
			MuteTrack: req,
		},
	})
	if err != nil {
		return nil, err
	}

	var track *livekit.TrackInfo
	err = s.confirmExecution(func() error {
		p, err := s.roomStore.LoadParticipant(ctx, roomKey, livekit.ParticipantIdentity(req.Identity))
		if err != nil {
			return err
		}
		// ensure track is muted
		t := funk.Find(p.Tracks, func(t *livekit.TrackInfo) bool {
			return t.Sid == req.TrackSid
		})
		var ok bool
		track, ok = t.(*livekit.TrackInfo)
		if !ok {
			return ErrTrackNotFound
		}
		if track.Muted != req.Muted {
			return ErrOperationFailed
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	res := &livekit.MuteRoomTrackResponse{
		Track: track,
	}
	return res, nil
}

func (s *RoomService) writeParticipantMessage(ctx context.Context, roomName livekit.RoomName, roomKey livekit.RoomKey, identity livekit.ParticipantIdentity, msg *livekit.RTCNodeMessage) error {
	if err := EnsureAdminPermission(ctx, roomName); err != nil {
		return twirpAuthError(err)
	}

	return s.router.WriteParticipantRTC(ctx, roomKey, identity, msg)
}

func (s *RoomService) confirmExecution(f func() error) error {
	expired := time.After(s.apiConf.ExecutionTimeout)
	var err error
	for {
		select {
		case <-expired:
			return err
		default:
			err = f()
			if err == nil {
				return nil
			}
			time.Sleep(s.apiConf.CheckInterval)
		}
	}
}

func (s *RoomService) ListParticipants(ctx context.Context, req *livekit.ListParticipantsRequest) (*livekit.ListParticipantsResponse, error) {
  //RecordRequest(ctx, req)

  AppendLogFields(ctx, "room", req.Room)
  if err := EnsureAdminPermission(ctx, livekit.RoomName(req.Room)); err != nil {
    return nil, twirpAuthError(err)
  }

  roomKey := utils.RoomKey(livekit.RoomName(req.Room), GetApiKey(ctx))
  participants, err := s.roomStore.ListParticipants(ctx, roomKey)
  if err != nil {
    return nil, err
  }

  res := &livekit.ListParticipantsResponse{
    Participants: participants,
  }
  //RecordResponse(ctx, res)
  return res, nil
}

func (s *RoomService) GetParticipant(ctx context.Context, req *livekit.RoomParticipantIdentity) (*livekit.ParticipantInfo, error) {
	//RecordRequest(ctx, req)

	AppendLogFields(ctx, "room", req.Room, "participant", req.Identity)
	if err := EnsureAdminPermission(ctx, livekit.RoomName(req.Room)); err != nil {
		return nil, twirpAuthError(err)
	}

	roomKey := utils.RoomKey(livekit.RoomName(req.Room), GetApiKey(ctx))
	participant, err := s.roomStore.LoadParticipant(ctx, roomKey, livekit.ParticipantIdentity(req.Identity))
	if err != nil {
		return nil, err
	}

	//RecordResponse(ctx, participant)
	return participant, nil
}


func (s *RoomService) UpdateParticipant(ctx context.Context, req *livekit.UpdateParticipantRequest) (*livekit.ParticipantInfo, error) {
	//RecordRequest(ctx, redactUpdateParticipantRequest(req))

	AppendLogFields(ctx, "room", req.Room, "participant", req.Identity)

	// if !s.limitConf.CheckParticipantNameLength(req.Name) {
	// 	return nil, twirp.InvalidArgumentError(ErrNameExceedsLimits.Error(), strconv.Itoa(s.limitConf.MaxParticipantNameLength))
	// }

	// if !s.limitConf.CheckMetadataSize(req.Metadata) {
	// 	return nil, twirp.InvalidArgumentError(ErrMetadataExceedsLimits.Error(), strconv.Itoa(int(s.limitConf.MaxMetadataSize)))
	// }

	// if !s.limitConf.CheckAttributesSize(req.Attributes) {
	// 	return nil, twirp.InvalidArgumentError(ErrAttributeExceedsLimits.Error(), strconv.Itoa(int(s.limitConf.MaxAttributesSize)))
	// }

	// if err := EnsureAdminPermission(ctx, livekit.RoomName(req.Room)); err != nil {
	// 	return nil, twirpAuthError(err)
	// }

	// if os, ok := s.roomStore.(OSSServiceStore); ok {
	// 	found, err := os.HasParticipant(ctx, livekit.RoomName(req.Room), livekit.ParticipantIdentity(req.Identity))
	// 	if err != nil {
	// 		return nil, err
	// 	} else if !found {
	// 		return nil, ErrParticipantNotFound
	// 	}
	// }

	// res, err := s.participantClient.UpdateParticipant(ctx, s.topicFormatter.ParticipantTopic(ctx, livekit.RoomName(req.Room), livekit.ParticipantIdentity(req.Identity)), req)
	//RecordResponse(ctx, res)
	//return res, err
	return nil, nil
}

func (s *RoomService) UpdateSubscriptions(ctx context.Context, req *livekit.UpdateSubscriptionsRequest) (*livekit.UpdateSubscriptionsResponse, error) {
	//RecordRequest(ctx, req)

	trackSIDs := append(make([]string, 0), req.TrackSids...)
	for _, pt := range req.ParticipantTracks {
		trackSIDs = append(trackSIDs, pt.TrackSids...)
	}
	AppendLogFields(ctx, "room", req.Room, "participant", req.Identity, "trackID", trackSIDs)

	if err := EnsureAdminPermission(ctx, livekit.RoomName(req.Room)); err != nil {
		return nil, twirpAuthError(err)
	}

	//res, err := s.participantClient.UpdateSubscriptions(ctx, s.topicFormatter.ParticipantTopic(ctx, livekit.RoomName(req.Room), livekit.ParticipantIdentity(req.Identity)), req)
	//RecordResponse(ctx, res)
	return nil, nil
}

func (s *RoomService) SendData(ctx context.Context, req *livekit.SendDataRequest) (*livekit.SendDataResponse, error) {
	//RecordRequest(ctx, redactSendDataRequest(req))

	roomName := livekit.RoomName(req.Room)
	AppendLogFields(ctx, "room", roomName, "size", len(req.Data))
	if err := EnsureAdminPermission(ctx, roomName); err != nil {
		return nil, twirpAuthError(err)
	}

	// nonce is either absent or 128-bit UUID
	// if len(req.Nonce) != 0 && len(req.Nonce) != 16 {
	// 	return nil, twirp.NewError(twirp.InvalidArgument, fmt.Sprintf("nonce should be 16-bytes or not present, got: %d bytes", len(req.Nonce)))
	// }

	//res, err := s.roomClient.SendData(ctx, s.topicFormatter.RoomTopic(ctx, livekit.RoomName(req.Room)), req)
	//RecordResponse(ctx, res)
	//return res, err
	return nil, nil
}
