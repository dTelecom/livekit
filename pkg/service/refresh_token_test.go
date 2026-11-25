package service

import (
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/rtc/types/typesfakes"
	"github.com/livekit/protocol/auth"
)

func TestRefreshTokenReflectsCurrentParticipantGrants(t *testing.T) {
	participant := &typesfakes.FakeLocalParticipant{}
	participant.IdentityReturns("alice")
	video := &auth.VideoGrant{Room: "call", RoomJoin: true}
	video.SetCanPublish(false)
	video.SetCanPublishData(false)
	video.SetCanSubscribe(true)
	participant.ClaimGrantsReturns(&auth.ClaimGrants{
		Identity: "alice", Name: "Alice", Metadata: "updated", Video: video,
	})
	manager := &RoomManager{config: &config.Config{}}
	require.NoError(t, manager.refreshToken(participant))
	require.Zero(t, participant.SendRefreshTokenCallCount())

	signer, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	issuer := signer.PublicKey().String()
	manager.config.Keys = map[string]string{issuer: signer.String()}
	require.NoError(t, manager.refreshToken(participant))
	require.Equal(t, 1, participant.SendRefreshTokenCallCount())
	token, err := auth.ParseAPIToken(participant.SendRefreshTokenArgsForCall(0))
	require.NoError(t, err)
	require.Equal(t, issuer, token.APIKey())
	claims, err := token.Verify(issuer)
	require.NoError(t, err)
	require.Equal(t, "alice", claims.Identity)
	require.Equal(t, "Alice", claims.Name)
	require.Equal(t, "updated", claims.Metadata)
	require.Equal(t, video, claims.Video)
}
