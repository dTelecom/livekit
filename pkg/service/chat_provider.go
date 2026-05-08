package service

import (
	"context"

	"github.com/dTelecom/p2p-database/pubsub"
	"github.com/livekit/protocol/webhook"

	"github.com/livekit/livekit-server/pkg/chat"
	"github.com/livekit/livekit-server/pkg/config"
)

// chatLookupAdapter bridges the concrete *ClientProvider to chat.ClientLookup.
// pkg/chat/ defines its own minimal interface so chat-side tests can inject a
// fake without depending on the Solana registry.
type chatLookupAdapter struct {
	cp *ClientProvider
}

func (a *chatLookupAdapter) ClientByAddress(ctx context.Context, address string) (chat.LookupResult, error) {
	c, err := a.cp.ClientByAddress(ctx, address)
	if err != nil {
		return chat.LookupResult{}, err
	}
	return chat.LookupResult{Key: c.Key}, nil
}

// createChatLookup is a Wire provider — adapts *ClientProvider to chat.ClientLookup.
func createChatLookup(cp *ClientProvider) chat.ClientLookup {
	return &chatLookupAdapter{cp: cp}
}

// getChatConfig pulls the chat slice out of the global config. Wire provider.
func getChatConfig(conf *config.Config) config.ChatConfig {
	return conf.Chat
}

// createPresenceTracker wires the chat presence layer against the shared
// pubsub DB. Wire provider.
func createPresenceTracker(db *pubsub.DB, cfg config.ChatConfig) *chat.PresenceTracker {
	return chat.NewPresenceTracker(db, cfg.UserPresenceQueryTimeout)
}

// createDispatcher wires the chat outbound send pipeline. Reuses the existing
// webhook.Notifier (signs with the node's wallet) for offline-fallback POSTs.
func createDispatcher(
	db *pubsub.DB,
	presence *chat.PresenceTracker,
	notifier webhook.Notifier,
	cfg config.ChatConfig,
) *chat.Dispatcher {
	return chat.NewDispatcher(db, presence, notifier, cfg.FallbackTimeout, cfg.UserPresenceQueryTimeout)
}

// createChatService is the top-level chat http.Handler producer. Wire provider.
func createChatService(
	cfg config.ChatConfig,
	presence *chat.PresenceTracker,
	disp *chat.Dispatcher,
	lookup chat.ClientLookup,
) *chat.Service {
	return chat.NewService(cfg, presence, disp, lookup)
}
