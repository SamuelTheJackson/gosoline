package rabbitmq

import (
	"context"
	"fmt"
	"time"

	"github.com/justtrackio/gosoline/pkg/appctx"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/justtrackio/gosoline/pkg/uuid"
)

type clientAppCtxKey string

type ClientSettings struct {
	Endpoint   string        `cfg:"endpoint" default:"amqp://localhost:5672"`
	Timeout    time.Duration `cfg:"timeout" default:"400"`
	RetryCount uint          `cfg:"retry_count" default:"4"`
}

type ClientConfig struct {
	Settings    ClientSettings
	LoadOptions []func(options *LoadOptions) error
}

type ClientOption func(cfg *ClientConfig)

//go:generate mockery --name Client
type Client interface {
	AckMessage(ctx context.Context, input AckMessageInput) error
	AckMessageBatch(ctx context.Context, input AckMessageBatchInput) error
	BindQueue(ctx context.Context, input QueueBindInput) error
	CreateExchange(ctx context.Context, input CreateExchangeInput) error
	ExchangeExists(ctx context.Context, input CreateExchangeInput) (bool, error)
	CreateQueue(ctx context.Context, input CreateQueueInput) (*CreateQueueOutput, error)
	NackMessage(ctx context.Context, input NackMessageInput) error
	NackMessageBatch(ctx context.Context, input AckMessageBatchInput) error
	QueueExists(ctx context.Context, name CreateQueueInput) (bool, error)
	ReceiveMessage(ctx context.Context, input ReceiveMessageInput) (*ReceiveMessageOutput, error)
	SendMessage(ctx context.Context, input SendMessageInput) (*SendMessageOutput, error)
	SendMessageBatch(ctx context.Context, input SendMessageBatchInput) (*SendMessageBatchOutput, error)
	Ping(ctx context.Context) error
}

type client struct {
	queueName        string
	logger           log.Logger
	Config           ClientConfig
	loadOptions      LoadOptions
	connection       Connection
	publishChannel   Channel
	consumeChannel   Channel
	existenceChannel Channel
	done             chan bool
	name             string
	uuid             uuid.Uuid
}

func ProvideClient(ctx context.Context, config cfg.Config, logger log.Logger, clientName string, optFns ...ClientOption) (Client, error) {
	return appctx.Provide(ctx, clientAppCtxKey(clientName), func() (Client, error) {
		return NewClient(ctx, config, logger, clientName, optFns...)
	})
}

func NewClient(ctx context.Context, config cfg.Config, logger log.Logger, name string, optFns ...ClientOption) (Client, error) {
	clientCfg := &ClientConfig{}

	clientCfg.Settings = unmarshalClientSettings(config, name)

	for _, opt := range optFns {
		opt(clientCfg)
	}

	loadOptions := &LoadOptions{}
	for _, opt := range clientCfg.LoadOptions {
		if err := opt(loadOptions); err != nil {
			return nil, err
		}
	}

	connection, err := NewConnection(ctx, config, logger, clientCfg.Settings.Endpoint)
	if err != nil {
		return nil, err
	}
	publishChannel, err := NewChannel(ctx, config, logger, connection)
	if err != nil {
		return nil, err
	}

	consumeChannel, err := NewChannel(ctx, config, logger, connection)
	if err != nil {
		return nil, err
	}

	existenceChannel, err := NewChannel(ctx, config, logger, connection)
	if err != nil {
		return nil, err
	}

	client := &client{
		logger:           logger.WithChannel(fmt.Sprintf("rabbitmq-client-%s", name)),
		Config:           *clientCfg,
		loadOptions:      *loadOptions,
		connection:       connection,
		publishChannel:   publishChannel,
		consumeChannel:   consumeChannel,
		existenceChannel: existenceChannel,
		done:             make(chan bool),
		name:             name,
		uuid:             uuid.New(),
	}

	return client, nil
}

func (c *client) handleContextDone(ctx context.Context) {
	select {
	case <-ctx.Done():
		if err := c.Close(); err != nil {
			c.logger.WithContext(ctx).Error("could not close client: %w", err)
		}

		return
	}
}

func (c *client) Close() error {
	close(c.done)

	return nil
}

func (c *client) SendMessage(ctx context.Context, input SendMessageInput) (*SendMessageOutput, error) {
	return c.publishChannel.SendMessage(ctx, input)
}

func (c *client) SendMessageBatch(ctx context.Context, input SendMessageBatchInput) (*SendMessageBatchOutput, error) {
	return nil, nil
}

func (c *client) ReceiveMessage(ctx context.Context, input ReceiveMessageInput) (*ReceiveMessageOutput, error) {
	return c.consumeChannel.ReceiveMessage(ctx, input)
}

func (c *client) NackMessage(ctx context.Context, input NackMessageInput) error {
	return c.consumeChannel.NackMessage(ctx, input)
}

func (c *client) NackMessageBatch(ctx context.Context, input AckMessageBatchInput) error {
	return nil
}

func (c *client) AckMessage(ctx context.Context, input AckMessageInput) error {
	return c.consumeChannel.AckMessage(ctx, input)
}

func (c *client) AckMessageBatch(ctx context.Context, input AckMessageBatchInput) error {
	return nil
}

func (c *client) QueueExists(ctx context.Context, input CreateQueueInput) (bool, error) {
	return c.existenceChannel.QueueExists(ctx, input)
}

func (c *client) CreateQueue(ctx context.Context, input CreateQueueInput) (*CreateQueueOutput, error) {
	return c.existenceChannel.CreateQueue(ctx, input)
}

func (c *client) BindQueue(ctx context.Context, input QueueBindInput) error {
	return c.existenceChannel.BindQueue(ctx, input)
}

func (c *client) CreateExchange(ctx context.Context, input CreateExchangeInput) error {
	return c.existenceChannel.CreateExchange(ctx, input)
}

func (c *client) ExchangeExists(ctx context.Context, input CreateExchangeInput) (bool, error) {
	return c.existenceChannel.ExchangeExists(ctx, input)

}

func (c *client) Ping(ctx context.Context) error {
	return c.existenceChannel.Ping(ctx)
}

func generateDefaultExchangeName(config cfg.Config, name string) string {
	env := config.GetString("env")
	family := config.GetString("app_family")
	group := config.GetString("app_group")
	appName := config.GetString("app_name")

	return fmt.Sprintf("%s-%s-%s-%s-%s", env, family, group, appName, name)
}

func generateDefaultQueueName(config cfg.Config, name string) string {
	env := config.GetString("env")
	family := config.GetString("app_family")
	group := config.GetString("app_group")
	appName := config.GetString("app_name")

	return fmt.Sprintf("%s-%s-%s-%s-%s", env, family, group, appName, name)
}
