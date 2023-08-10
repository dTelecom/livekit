package p2p

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	p2p_database "github.com/dTelecom/p2p-realtime-database"
	logging "github.com/ipfs/go-log/v2"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/pkg/errors"
)

const (
	dbPrefix                   = "livekit_room_"
	prefixIncomingMessageTopic = "incoming_messages_"
	prefixPeerKey              = "node_"

	roomMessagesTopicFmt     = "room_messages_%v"
	incomingMessagesTopicFmt = "incoming_messages_%v_%v"
	pingMessage              = "ping"
	pongMessage              = "pong"
	adMessage                = "ad"
)

type RoomCommunicatorImpl struct {
	room *livekit.Room

	ctx    context.Context
	cancel context.CancelFunc

	roomDatabase *p2p_database.DB
	mainDatabase *p2p_database.DB

	peers           map[string]struct{}
	peerHandlers    []func(peerId string)
	messageHandlers []func(message interface{}, fromPeerId string, eventId string)

	mu sync.Mutex
}

func NewRoomCommunicatorImpl(room *livekit.Room, mainDatabase *p2p_database.DB, p2pDbConfig p2p_database.Config) (*RoomCommunicatorImpl, error) {
	ctx, cancel := context.WithCancel(context.Background())

	roomCommunicator := &RoomCommunicatorImpl{
		room:         room,
		ctx:          ctx,
		cancel:       cancel,
		mainDatabase: mainDatabase,
		peers:        make(map[string]struct{}),
	}

	_ = logging.SetLogLevel("*", "error")
	p2pDbConfig.DatabaseName = dbPrefix + room.Key
	if err := roomCommunicator.init(p2pDbConfig); err != nil {
		return nil, errors.Wrap(err, "cannot init room communicator")
	}

	return roomCommunicator, nil
}

func (c *RoomCommunicatorImpl) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := c.roomDatabase.Disconnect(ctx)
	if err != nil {
		logger.Errorw("fail disconnect db "+c.roomDatabase.Name, err)
	}

	c.cancel()
}

func (c *RoomCommunicatorImpl) init(cfg p2p_database.Config) error {
	var (
		db  *p2p_database.DB
		err error
	)

	cfg.NewKeyCallback = func(k string) {
		log.Printf("New key added %v", k)

		k = strings.TrimPrefix(k, "/")
		if !strings.HasPrefix(k, prefixPeerKey) {
			return
		}

		log.Printf("New key added trim %v", k)

		peerId := strings.TrimPrefix(k, prefixPeerKey)
		if peerId == db.GetHost().ID().String() {
			return
		}

		log.Printf("New key added peer %v", peerId)

		c.checkPeer(peerId)
	}

	db, err = p2p_database.Connect(c.ctx, cfg, logging.Logger(dbPrefix+c.room.Key))
	if err != nil {
		return errors.Wrap(err, "cannot connect to database")
	}
	c.roomDatabase = db

	subErr := c.roomDatabase.Subscribe(c.ctx, p2pTopicName(db.GetHost().ID().String()), c.incomingMessageHandler)
	if subErr != nil {
		return errors.Wrap(subErr, "cannot subscribe to topic")
	}

	incomingMessagesTopic := formatIncomingMessagesTopic(c.room.Key, c.roomDatabase.GetHost().ID().String())
	if err := c.roomDatabase.Subscribe(c.ctx, incomingMessagesTopic, c.incomingMessageHandler); err != nil {
		return errors.Wrap(err, "cannot subscribe to incoming messages topic")
	}
	log.Printf("subscribed to topic %v", incomingMessagesTopic)

	roomMessagesTopic := formatRoomMessageTopic(c.room.Key)
	if err := c.roomDatabase.Subscribe(c.ctx, roomMessagesTopic, c.roomMessageHandler); err != nil {
		return errors.Wrap(err, "cannot subscribe to room messages topic")
	}
	log.Printf("subscribed to topic %v", roomMessagesTopic)

	go func() {
		for {
			if _, err := c.roomDatabase.Publish(c.ctx, roomMessagesTopic, adMessage); err != nil {
				if c.ctx.Err() == nil {
					log.Fatalf("cannot publish ad message: %v", err)
				} else {
					return
				}
			}
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}
	}()

	return nil
}

func (c *RoomCommunicatorImpl) checkPeer(peerId string) {
	c.mu.Lock()
	if _, ok := c.peers[peerId]; ok {
		c.mu.Unlock()
	} else {
		log.Printf("New key added peer not exist %v", peerId)
		c.peers[peerId] = struct{}{}
		for _, peerHandler := range c.peerHandlers {
			go peerHandler(peerId)
		}
		c.mu.Unlock()
		log.Printf("New key added peer added %v", peerId)

		incomingMessageTopic := formatIncomingMessagesTopic(c.room.Key, peerId)
		if _, err := c.roomDatabase.Publish(c.ctx, incomingMessageTopic, pingMessage); err != nil {
			log.Printf("cannot send ping message for node %s in db %s: %s", peerId, c.room.Key, err)
		} else {
			log.Printf("PING message sent to %v", peerId)
		}
	}
}

func (c *RoomCommunicatorImpl) incomingMessageHandler(event p2p_database.Event) {
	if event.Message == pingMessage {
		log.Println("PING message received")
		incomingMessageTopic := formatIncomingMessagesTopic(c.room.Key, event.FromPeerId)
		if _, err := c.roomDatabase.Publish(c.ctx, incomingMessageTopic, pongMessage); err != nil {
			log.Printf("cannot send pong message for node %s in db %s: %s", event.FromPeerId, c.room.Key, err)
		} else {
			log.Printf("PONG message sent to %v", event.FromPeerId)
		}
	} else if event.Message == pongMessage {
		log.Printf("PONG message received from %v", event.FromPeerId)
	} else if event.Message == adMessage {
		c.checkPeer(event.FromPeerId)
	} else {
		c.mu.Lock()
		for _, messageHandler := range c.messageHandlers {
			go messageHandler(event.Message, event.FromPeerId, event.ID)
		}
		c.mu.Unlock()
	}
}

func (c *RoomCommunicatorImpl) roomMessageHandler(event p2p_database.Event) {
	if event.Message == adMessage {
		c.checkPeer(event.FromPeerId)

		incomingMessageTopic := formatIncomingMessagesTopic(c.room.Key, event.FromPeerId)
		if _, err := c.roomDatabase.Publish(c.ctx, incomingMessageTopic, adMessage); err != nil {
			log.Printf("cannot send ad message for node %s in db %s: %s", event.FromPeerId, c.room.Key, err)
		} else {
			log.Printf("ad message sent to %v", event.FromPeerId)
		}
	} else {
		log.Printf("unknown room message from peer: %v", event.FromPeerId)
	}
}

func (c *RoomCommunicatorImpl) ForEachPeer(peerHandler func(peerId string)) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.peerHandlers = append(c.peerHandlers, peerHandler)
	for peer := range c.peers {
		peerHandler(peer)
	}
}

func (c *RoomCommunicatorImpl) SendMessage(peerId string, message interface{}) (string, error) {
	incomingMessagesTopic := formatIncomingMessagesTopic(c.room.Key, peerId)
	event, err := c.roomDatabase.Publish(c.ctx, incomingMessagesTopic, message)
	if err != nil {
		return "", errors.Wrap(err, "publish error")
	}
	return event.ID, nil
}

func (c *RoomCommunicatorImpl) OnMessage(messageHandler func(message interface{}, fromPeerId string, eventId string)) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.messageHandlers = append(c.messageHandlers, messageHandler)
}

func formatRoomMessageTopic(roomKey string) string {
	return fmt.Sprintf(roomMessagesTopicFmt, roomKey)
}

func formatIncomingMessagesTopic(roomKey, peerId string) string {
	return fmt.Sprintf(incomingMessagesTopicFmt, roomKey, peerId)
}

func p2pTopicName(peerId string) string {
	return prefixIncomingMessageTopic + peerId
}
