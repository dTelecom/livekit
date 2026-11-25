package test

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twitchtv/twirp"

	"github.com/livekit/livekit-server/pkg/config"
	serverlogger "github.com/livekit/livekit-server/pkg/logger"
	"github.com/livekit/livekit-server/pkg/telemetry/prometheus"
	testclient "github.com/livekit/livekit-server/test/client"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
)

func init() {
	serverlogger.InitFromConfig(config.LoggingConfig{
		Config: logger.Config{Level: "debug"},
	})

	prometheus.Init("test", livekit.NodeType_SERVER, "test")
}

func contextWithToken(token string) context.Context {
	header := make(http.Header)
	testclient.SetAuthorizationToken(header, token)
	tctx, err := twirp.WithHTTPRequestHeaders(context.Background(), header)
	if err != nil {
		panic(err)
	}
	return tctx
}

func waitUntilConnected(t *testing.T, clients ...*testclient.RTCClient) {
	t.Helper()
	wg := sync.WaitGroup{}
	for i := range clients {
		c := clients[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.WaitUntilConnected()
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
}

func publishJoinedTracks(t *testing.T, publisher *testclient.RTCClient) {
	t.Helper()
	for _, track := range []struct{ mime, id string }{{"audio/opus", "audio"}, {"video/vp8", "video"}} {
		writer, err := publisher.AddStaticTrack(track.mime, track.id, "webcam")
		require.NoError(t, err)
		t.Cleanup(writer.Stop)
	}
}
