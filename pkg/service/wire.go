//go:build wireinject
// +build wireinject

package service

import (
	"context"

	p2p_database "github.com/dTelecom/p2p-realtime-database"
	"github.com/google/wire"
	livekit2 "github.com/livekit/livekit-server"
	"github.com/livekit/livekit-server/pkg/clientconfiguration"
	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/telemetry"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/egress"
	"github.com/livekit/protocol/livekit"
	redisLiveKit "github.com/livekit/protocol/redis"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/protocol/utils"
	"github.com/livekit/protocol/webhook"
	"github.com/livekit/psrpc"
	"github.com/oschwald/geoip2-golang"
	"github.com/pion/turn/v2"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

func InitializeServer(conf *config.Config, currentNode routing.LocalNode) (*LivekitServer, error) {
	wire.Build(
		getNodeID,
		createRedisClient,
		GetDatabaseConfiguration,
		createStore,
		wire.Bind(new(ServiceStore), new(ObjectStore)),
		createKeyProvider,
		createKeyPublicKeyProvider,
		createWebhookNotifier,
		createClientConfiguration,
		routing.CreateRouter,
		getRoomConf,
		config.DefaultAPIConfig,
		CreateMainDatabaseP2P,
		createParticipantCounter,
		//wire.Bind(new(MainP2PDatabase), new(*p2p_database.DB)),
		wire.Bind(new(routing.MessageRouter), new(routing.Router)),
		wire.Bind(new(livekit.RoomService), new(*RoomService)),
		telemetry.NewAnalyticsService,
		telemetry.NewTelemetryService,
		getMessageBus,
		getEgressClient,
		egress.NewRedisRPCClient,
		getEgressStore,
		NewEgressLauncher,
		NewEgressService,
		rpc.NewIngressClient,
		getIngressStore,
		getIngressConfig,
		NewIngressService,
		NewRoomAllocator,
		NewRoomService,
		NewRTCService,
		getSignalRelayConfig,
		NewDefaultSignalServer,
		routing.NewSignalClient,
		NewLocalRoomManager,
		newTurnAuthHandler,
		newInProcessTurnServer,
		utils.NewDefaultTimedVersionGenerator,
		createSmartContractClient,
		createClientProvider,
		createGeoIP,
		createTrafficManager,
		CreateNodeProvider,
		createRelevantNodesHandler,
		NewLivekitServer,
	)
	return &LivekitServer{}, nil
}

func createRelevantNodesHandler(conf *config.Config, nodeProvider *NodeProvider) *RelevantNodesHandler {
	return NewRelevantNodesHandler(nodeProvider, conf.LoggingP2P)
}

func createGeoIP() (*geoip2.Reader, error) {
	return geoip2.FromBytes(livekit2.MixmindDatabase)
}

func CreateNodeProvider(geo *geoip2.Reader, config *config.Config, db *p2p_database.DB) *NodeProvider {
	return NewNodeProvider(db, geo, config.LoggingP2P)
}

func createClientProvider(contract *p2p_database.EthSmartContract, db *p2p_database.DB) *ClientProvider {
	return NewClientProvider(db, contract)
}

func createSmartContractClient(conf *config.Config) (*p2p_database.EthSmartContract, error) {
	contract, err := p2p_database.NewEthSmartContract(p2p_database.Config{
		EthereumNetworkHost:     conf.Ethereum.NetworkHost,
		EthereumNetworkKey:      conf.Ethereum.NetworkKey,
		EthereumContractAddress: conf.Ethereum.ContractAddress,
	}, conf.LoggingP2P)

	if err != nil {
		return nil, errors.Wrap(err, "try create contract")
	}

	return contract, nil
}

func createParticipantCounter(mainDatabase *p2p_database.DB, conf *config.Config) (*ParticipantCounter, error) {
	return NewParticipantCounter(mainDatabase, conf.LoggingP2P)
}

func GetDatabaseConfiguration(conf *config.Config) p2p_database.Config {
	return p2p_database.Config{
		DisableGater: true,
		PeerListenPort:          conf.Ethereum.P2pNodePort,
		EthereumNetworkHost:     conf.Ethereum.NetworkHost,
		EthereumNetworkKey:      conf.Ethereum.NetworkKey,
		EthereumContractAddress: conf.Ethereum.ContractAddress,
		WalletPrivateKey:        conf.Ethereum.WalletPrivateKey,
		DatabaseName:            conf.Ethereum.P2pMainDatabaseName,
	}
}

func createTrafficManager(mainDatabase *p2p_database.DB, configuration *config.Config) *TrafficManager {
	return NewTrafficManager(mainDatabase, configuration.LoggingP2P)
}

func CreateMainDatabaseP2P(conf p2p_database.Config, c *config.Config) (*p2p_database.DB, error) {
	db, err := p2p_database.Connect(context.Background(), conf, c.LoggingP2P)
	if err != nil {
		return nil, errors.Wrap(err, "create main p2p db")
	}
	return db, nil
}

func getNodeID(currentNode routing.LocalNode) livekit.NodeID {
	return livekit.NodeID(currentNode.Id)
}

func createKeyProvider(conf *config.Config, contract *p2p_database.EthSmartContract) (auth.KeyProvider, error) {
	return createKeyPublicKeyProvider(conf, contract)
}

func createKeyPublicKeyProvider(conf *config.Config, contract *p2p_database.EthSmartContract) (auth.KeyProviderPublicKey, error) {
	return auth.NewEthKeyProvider(*contract, conf.Ethereum.WalletAddress, conf.Ethereum.WalletPrivateKey), nil
}

func createWebhookNotifier(conf *config.Config, provider auth.KeyProvider) (webhook.Notifier, error) {
	wallet := conf.Ethereum.WalletAddress
	secret := provider.GetSecret(wallet)
	if secret == "" {
		return nil, ErrWebHookMissingAPIKey
	}

	return webhook.NewNotifier(wallet, secret), nil
}

func createRedisClient(conf *config.Config) (redis.UniversalClient, error) {
	if !conf.Redis.IsConfigured() {
		return nil, nil
	}
	return redisLiveKit.GetRedisClient(&conf.Redis)
}

func createStore(
	mainDatabase *p2p_database.DB,
	p2pDbConfig p2p_database.Config,
	nodeID livekit.NodeID,
	participantCounter *ParticipantCounter,
	conf *config.Config,
	nodeProvider *NodeProvider,
) ObjectStore {
	return NewLocalStore(nodeID, participantCounter, mainDatabase, nodeProvider)
}

func getMessageBus(rc redis.UniversalClient) psrpc.MessageBus {
	if rc == nil {
		return psrpc.NewLocalMessageBus()
	}
	return psrpc.NewRedisMessageBus(rc)
}

func getEgressClient(conf *config.Config, nodeID livekit.NodeID, bus psrpc.MessageBus) (rpc.EgressClient, error) {
	if conf.Egress.UsePsRPC {
		return rpc.NewEgressClient(nodeID, bus)
	}

	return nil, nil
}

func getEgressStore(s ObjectStore) EgressStore {
	return nil
}

func getIngressStore(s ObjectStore) IngressStore {
	return nil
}

func getIngressConfig(conf *config.Config) *config.IngressConfig {
	return &conf.Ingress
}

func createClientConfiguration() clientconfiguration.ClientConfigurationManager {
	return clientconfiguration.NewStaticClientConfigurationManager(clientconfiguration.StaticConfigurations)
}

func getRoomConf(config *config.Config) config.RoomConfig {
	return config.Room
}

func getSignalRelayConfig(config *config.Config) config.SignalRelayConfig {
	return config.SignalRelay
}

func newInProcessTurnServer(conf *config.Config, authHandler turn.AuthHandler) (*turn.Server, error) {
	return NewTurnServer(conf, authHandler, false)
}
