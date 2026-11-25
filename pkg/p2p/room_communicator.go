package p2p

import (
	"context"
	"fmt"
	"go.uber.org/atomic"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/livekit/protocol/logger"

	p2p_common "github.com/dTelecom/p2p-database/common"
	"github.com/livekit/protocol/livekit"
	"github.com/pkg/errors"
)

const (
	roomMessagesTopicFmt = "room_messages_%v"
)

type roomCommunicatorMessage struct {
	To      string      `json:"to"`
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
}

// RoomPubSub is the room-signaling surface of the shared P2P database.
type RoomPubSub interface {
	Subscribe(context.Context, string, p2p_common.PubSubHandler) error
	Unsubscribe(context.Context, string) error
	Publish(context.Context, string, interface{}) (p2p_common.Event, error)
	GetHost() host.Host
}

type RoomCommunicatorImpl struct {
	key string

	ctx          context.Context
	cancel       context.CancelFunc
	closed       atomic.Bool
	handlerReady chan struct{}

	mainDatabase RoomPubSub
	logger       logger.Logger

	peers           map[string]struct{}
	peerHandlers    []func(peerId string)
	messageHandlers []func(message interface{}, fromPeerId string, eventId string)

	mu sync.Mutex
}

func NewRoomCommunicatorImpl(key livekit.RoomKey, mainDatabase RoomPubSub) (*RoomCommunicatorImpl, error) {
	ctx, cancel := context.WithCancel(context.Background())

	roomCommunicator := &RoomCommunicatorImpl{
		key:          string(key),
		ctx:          ctx,
		cancel:       cancel,
		mainDatabase: mainDatabase,
		peers:        make(map[string]struct{}),
		handlerReady: make(chan struct{}),
		logger:       logger.GetLogger(),
	}

	if err := roomCommunicator.init(); err != nil {
		cancel()
		return nil, err
	}
	return roomCommunicator, nil
}

func (c *RoomCommunicatorImpl) Close() {
	if c.closed.Swap(true) {
		return
	}

	c.cancel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	roomMessagesTopic := formatRoomMessageTopic(c.key)
	err := c.mainDatabase.Unsubscribe(ctx, roomMessagesTopic)
	if err != nil {
		c.logger.Errorw("Failed to unsubscribe from topic", err, "topic", roomMessagesTopic, "roomKey", c.key)
	} else {
		c.logger.Debugw("Unsubscribed from room messages topic", "topic", roomMessagesTopic, "roomKey", c.key)
	}
}

func (c *RoomCommunicatorImpl) init() error {

	roomMessagesTopic := formatRoomMessageTopic(c.key)
	if err := c.mainDatabase.Subscribe(c.ctx, roomMessagesTopic, c.roomMessageHandler); err != nil {
		return errors.Wrap(err, "cannot subscribe to room messages topic")
	}
	c.logger.Debugw("Subscribed to room messages topic", "topic", roomMessagesTopic, "roomKey", c.key)

	go func() {
		select {
		case <-c.handlerReady:
		case <-c.ctx.Done():
			return
		}
		for {
			c.publishAd(roomMessagesTopic)
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

func (c *RoomCommunicatorImpl) publishAd(roomMessagesTopic string) {
	if c.closed.Load() {
		c.logger.Debugw("Room communicator closed, skipping publish ad", "roomKey", c.key)
		return
	}

	ctx, cancel := context.WithTimeout(c.ctx, time.Second*15)
	defer cancel()

	data := &roomCommunicatorMessage{
		To:      "",
		Type:    "ad",
		Payload: nil,
	}

	if _, err := c.mainDatabase.Publish(ctx, roomMessagesTopic, data); err != nil {
		c.logger.Errorw("Failed to publish ad message", err, "topic", roomMessagesTopic, "roomKey", c.key)
	} else {
		c.logger.Debugw("Published ad message", "topic", roomMessagesTopic, "roomKey", c.key)
	}
}

func (c *RoomCommunicatorImpl) checkPeer(peerId string) {
	c.mu.Lock()
	if _, ok := c.peers[peerId]; ok {
		c.mu.Unlock()
	} else {
		c.logger.Infow("Discovered new peer", "peerId", peerId, "roomKey", c.key)
		c.peers[peerId] = struct{}{}
		for _, peerHandler := range c.peerHandlers {
			go peerHandler(peerId)
		}
		c.mu.Unlock()
		roomMessagesTopic := formatRoomMessageTopic(c.key)
		c.publishAd(roomMessagesTopic)
	}
}

func (c *RoomCommunicatorImpl) roomMessageHandler(event p2p_common.Event) {
	// The DB consumes this subscription serially. Preserve the first event
	// until the RTC room can handle it, or release the listener on close.
	select {
	case <-c.handlerReady:
	case <-c.ctx.Done():
		return
	}
	if c.closed.Load() {
		return
	}

	if event.FromPeerId == c.mainDatabase.GetHost().ID().String() {
		return
	}

	c.logger.Debugw("Received room message", "message", event.Message, "roomKey", c.key)

	messageMap, ok := event.Message.(map[string]interface{})
	if !ok {
		c.logger.Errorw("Failed to cast message", fmt.Errorf("event.Message %v", event.Message), "roomKey", c.key)
		return
	}

	typeValue, ok := messageMap["type"]
	if !ok {
		c.logger.Errorw("Failed to cast message", fmt.Errorf("event.Message %v", event.Message), "roomKey", c.key)
		return
	}

	messageType, ok := typeValue.(string)
	if !ok {
		c.logger.Errorw("Failed to cast message", fmt.Errorf("event.Message %v", event.Message), "roomKey", c.key)
		return
	}

	if messageType == "ad" {
		c.checkPeer(event.FromPeerId)
	} else {
		toValue, ok := messageMap["to"]
		if !ok {
			c.logger.Errorw("Failed to cast message", fmt.Errorf("event.Message %v", event.Message), "roomKey", c.key)
			return
		}

		to, ok := toValue.(string)
		if !ok {
			c.logger.Errorw("Failed to cast message", fmt.Errorf("event.Message %v", event.Message), "roomKey", c.key)
			return
		}

		if to != c.mainDatabase.GetHost().ID().String() {
			return
		}

		c.mu.Lock()
		for _, messageHandler := range c.messageHandlers {
			go messageHandler(messageMap["payload"], event.FromPeerId, event.ID)
		}
		c.mu.Unlock()
	}
}

func (c *RoomCommunicatorImpl) ForEachPeer(peerHandler func(peerId string)) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.peerHandlers = append(c.peerHandlers, peerHandler)
	for peer := range c.peers {
		go peerHandler(peer)
	}
}

func (c *RoomCommunicatorImpl) SendMessage(peerId string, message interface{}) (string, error) {

	if c.closed.Load() {
		return "", fmt.Errorf("room communicator closed for room %v", c.key)
	}

	ctx, cancel := context.WithTimeout(c.ctx, time.Second*10)
	defer cancel()

	data := &roomCommunicatorMessage{
		To:      peerId,
		Type:    "signal",
		Payload: message,
	}

	roomMessagesTopic := formatRoomMessageTopic(c.key)
	event, err := c.mainDatabase.Publish(ctx, roomMessagesTopic, data)

	if err != nil {
		return "", errors.Wrap(err, "publish error")
	}
	return event.ID, nil
}

func (c *RoomCommunicatorImpl) OnMessage(messageHandler func(message interface{}, fromPeerId string, eventId string)) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.messageHandlers = append(c.messageHandlers, messageHandler)
	if len(c.messageHandlers) == 1 {
		close(c.handlerReady)
	}
}

func (c *RoomCommunicatorImpl) PeerId() string {
	return c.mainDatabase.GetHost().ID().String()
}

func formatRoomMessageTopic(roomKey string) string {
	return fmt.Sprintf(roomMessagesTopicFmt, roomKey)
}

func (c *RoomCommunicatorImpl) RemovePeer(peerId string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.peers, peerId)
}
