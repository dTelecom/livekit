package p2p

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dTelecom/p2p-database/common"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

type testRoomHost struct {
	host.Host
	id peer.ID
}

func (h testRoomHost) ID() peer.ID { return h.id }

type roomTestTransport struct {
	host     testRoomHost
	handler  common.PubSubHandler
	outbound chan common.Event
	closes   int
}

func (db *roomTestTransport) GetHost() host.Host { return db.host }
func (db *roomTestTransport) Subscribe(_ context.Context, _ string, f common.PubSubHandler) error {
	db.handler = f
	return nil
}
func (db *roomTestTransport) Unsubscribe(context.Context, string) error {
	db.closes++
	return nil
}
func (db *roomTestTransport) Publish(ctx context.Context, _ string, value interface{}) (common.Event, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return common.Event{}, err
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(b, &payload); err != nil {
		return common.Event{}, err
	}
	event := common.Event{ID: "fixture-event", FromPeerId: db.host.ID().String(), Message: payload}
	select {
	case db.outbound <- event:
		return event, nil
	case <-ctx.Done():
		return common.Event{}, ctx.Err()
	}
}

func newRoomEndpoint(t *testing.T, name string) (*RoomCommunicatorImpl, *roomTestTransport) {
	t.Helper()
	db := &roomTestTransport{host: testRoomHost{id: peer.ID(name)}, outbound: make(chan common.Event, 8)}
	room, err := NewRoomCommunicatorImpl("room", db)
	require.NoError(t, err)
	t.Cleanup(room.Close)
	return room, db
}

func TestRoomSignalsAtStartup(t *testing.T) {
	a, dbA := newRoomEndpoint(t, "node-a")
	b, dbB := newRoomEndpoint(t, "node-b")

	// A subscription is already active while the room manager constructs the
	// RTC room. Deliver an ordinary first offer to each end during this gap.
	type signal struct {
		message  interface{}
		from, id string
	}
	gotA, gotB := make(chan signal, 2), make(chan signal, 2)
	for _, end := range []struct {
		db    *roomTestTransport
		other *RoomCommunicatorImpl
	}{{dbA, b}, {dbB, a}} {
		event := common.Event{ID: "first-offer", FromPeerId: end.other.PeerId(),
			Message: map[string]interface{}{"to": end.db.host.ID().String(), "type": "signal", "payload": "offer"}}
		returned := make(chan struct{})
		go func() { end.db.handler(event); close(returned) }()
		select {
		case <-returned:
			t.Fatal("directed signal was consumed before its room handler was installed")
		case <-end.db.outbound:
			t.Fatal("room advertised before its signal handler was installed")
		case <-time.After(25 * time.Millisecond):
		}
	}

	register := func(room *RoomCommunicatorImpl, incoming chan<- signal) {
		room.OnMessage(func(message interface{}, from string, id string) {
			incoming <- signal{message, from, id}
			if message == "offer" {
				_, err := room.SendMessage(from, "answer")
				if err != nil {
					t.Errorf("sending reply: %v", err)
				}
			}
		})
	}
	register(a, gotA)
	register(b, gotB)

	// Deliver the answers from the callbacks to the opposite nodes. The ad
	// and first reply can arrive in either order after registration.
	for _, end := range []struct {
		from, to *roomTestTransport
	}{{dbA, dbB}, {dbB, dbA}} {
		var sawAd, sawAnswer bool
		for i := 0; i < 2; i++ {
			select {
			case e := <-end.from.outbound:
				switch e.Message.(map[string]interface{})["type"] {
				case "ad":
					sawAd = true
				case "signal":
					sawAnswer = true
					end.to.handler(e)
				}
			case <-time.After(time.Second):
				t.Fatal("missing room advertisement or relay answer")
			}
		}
		require.True(t, sawAd)
		require.True(t, sawAnswer)
	}
	for _, end := range []struct {
		got   <-chan signal
		other *RoomCommunicatorImpl
	}{{gotA, b}, {gotB, a}} {
		var received []signal
		for i := 0; i < 2; i++ {
			select {
			case got := <-end.got:
				received = append(received, got)
			case <-time.After(time.Second):
				t.Fatal("missing relay signal")
			}
		}
		require.ElementsMatch(t, []signal{
			{"offer", end.other.PeerId(), "first-offer"},
			{"answer", end.other.PeerId(), "fixture-event"},
		}, received)
	}

}

func TestRoomClosesBeforeSignalHandler(t *testing.T) {
	room, db := newRoomEndpoint(t, "empty-room")
	returned := make(chan struct{})
	go func() {
		db.handler(common.Event{FromPeerId: "other", Message: map[string]interface{}{"type": "ad"}})
		close(returned)
	}()
	room.Close()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("closing an unoccupied room left its listener waiting")
	}
	room.Close()
	require.Equal(t, 1, db.closes)
	select {
	case <-db.outbound:
		t.Fatal("closed room advertised")
	default:
	}
}
