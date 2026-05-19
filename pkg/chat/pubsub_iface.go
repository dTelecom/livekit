package chat

import (
	"context"

	p2p_common "github.com/dTelecom/p2p-database/common"
)

// pubsubAPI is the subset of *pubsub.DB the chat package uses. Lets tests
// inject an in-memory fake without spinning up a real p2p network.
//
// The concrete *pubsub.DB satisfies this interface — no adapter needed.
// Construction sites (chat_provider.go) keep passing *pubsub.DB.
type pubsubAPI interface {
	Publish(ctx context.Context, topic string, value interface{}) (p2p_common.Event, error)
	Subscribe(ctx context.Context, topic string, handler p2p_common.PubSubHandler) error
	Unsubscribe(ctx context.Context, topic string) error
}
