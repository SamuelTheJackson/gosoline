package rabbitmq

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/justtrackio/gosoline/pkg/appctx"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/justtrackio/gosoline/pkg/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
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
	queueName    string
	logger       log.Logger
	Config       ClientConfig
	loadOptions  LoadOptions
	cConnection  *amqp.Connection
	pConnection  *amqp.Connection
	cChannel     *amqp.Channel
	pChannel     *amqp.Channel
	done         chan bool
	pNotifyClose chan *amqp.Error
	cNotifyClose chan *amqp.Error
	name         string
	uuid         uuid.Uuid
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

	client := &client{
		logger:      logger.WithChannel(fmt.Sprintf("rabbitmq-client-%s", name)),
		Config:      *clientCfg,
		loadOptions: *loadOptions,
		name:        name,
		uuid:        uuid.New(),
	}

	if err := client.createConnections(clientCfg.Settings.Endpoint); err != nil {
		return nil, fmt.Errorf("could not createConnection to server %s: %w", clientCfg.Settings.Endpoint, err)
	}

	var err error
	client.cChannel, err = client.cConnection.Channel()
	if err != nil {
		return nil, fmt.Errorf("could not open a channel: %w", err)
	}

	if err := client.cChannel.Confirm(false); err != nil {
		return nil, fmt.Errorf("could not put client into configm mode: %w", err)
	}

	client.pChannel, err = client.pConnection.Channel()
	if err != nil {
		return nil, fmt.Errorf("could not open a channel: %w", err)
	}

	if err := client.pChannel.Confirm(false); err != nil {
		return nil, fmt.Errorf("could not put client into configm mode: %w", err)
	}

	client.pNotifyClose = make(chan *amqp.Error, 1)
	client.pChannel.NotifyClose(client.pNotifyClose)
	client.cChannel.NotifyClose(client.cNotifyClose)
	client.cConnection.NotifyClose(client.cNotifyClose)

	go client.handleReconnect(ctx)
	go client.handleContextDone(ctx)

	return client, nil
}

func (c *client) handleContextDone(ctx context.Context) {
	select {
	case <-ctx.Done():
		if err := c.Close(); err != nil {
			c.logger.WithContext(ctx).Error("could not close client: %w", err)
		}
	}
}

func (c *client) handleReconnect(ctx context.Context) {
	logger := c.logger.WithContext(ctx)
	for {
		select {
		case <-c.done:
			return
		case err := <-c.cNotifyClose:
			logger.Error("connection closed: %w", err)
			c.setup(ctx)
		case err := <-c.pNotifyClose:
			logger.Error("connection closed: %w", err)
			c.setup(ctx)
		}
	}
}

func (c *client) setup(ctx context.Context) {
	logger := c.logger.WithContext(ctx)
	for {
		select {
		case <-c.done:
			return
		case <-time.After(c.Config.Settings.Timeout * time.Millisecond):
		}

		logger.Warn("disconnected from rabbitmq trying to reconnect")
		if err := c.createConnections(c.Config.Settings.Endpoint); err != nil {
			logger.Error("could not create connection to rabbitmq server: %w", err)

			continue
		}

		if err := c.setupChannel(); err != nil {
			logger.Error("could not setup channel: %w", err)

			continue
		}

		break
	}

}

func (c *client) setupChannel() (err error) {
	if c.cConnection == nil || c.cConnection.IsClosed() || c.pConnection == nil || c.pConnection.IsClosed() {
		return fmt.Errorf("not connected to rabbitmq")
	}

	if c.cChannel == nil || c.cChannel.IsClosed() {
		if c.cChannel, err = c.cConnection.Channel(); err != nil {
			return fmt.Errorf("could not open a channel: %w", err)
		}

		if err := c.cChannel.Confirm(false); err != nil {
			return fmt.Errorf("could not put client into configm mode: %w", err)
		}
		c.cNotifyClose = make(chan *amqp.Error, 1)
		c.cChannel.NotifyClose(c.cNotifyClose)
	}

	if c.pChannel == nil || c.pChannel.IsClosed() {
		if c.pChannel, err = c.pConnection.Channel(); err != nil {
			return fmt.Errorf("could not open a channel: %w", err)
		}

		if err := c.pChannel.Confirm(false); err != nil {
			return fmt.Errorf("could not put client into configm mode: %w", err)
		}
		c.pNotifyClose = make(chan *amqp.Error, 1)
		c.pChannel.NotifyClose(c.pNotifyClose)
	}

	return nil
}

func (c *client) createConnections(addr string) (err error) {
	if c.cConnection != nil && !c.cConnection.IsClosed() && c.pConnection != nil && !c.pConnection.IsClosed() {
		return nil
	}

	if c.cConnection == nil || c.cConnection.IsClosed() {
		if c.cConnection, err = amqp.Dial(addr); err != nil {
			return fmt.Errorf("could not connect to rabbitmq server: %w", err)
		}
	}

	if c.pConnection == nil || c.pConnection.IsClosed() {
		if c.pConnection, err = amqp.Dial(addr); err != nil {
			return fmt.Errorf("could not connect to rabbitmq server: %w", err)
		}
	}

	return nil
}

func (c *client) Close() error {
	if c.cConnection.IsClosed() || c.cChannel.IsClosed() {
		return fmt.Errorf("already closed: not connected to the server")
	}

	close(c.done)
	err := c.cChannel.Close()
	if err != nil {
		return err
	}
	err = c.cConnection.Close()
	if err != nil {
		return err
	}
	err = c.pChannel.Close()
	if err != nil {
		return err
	}
	err = c.pConnection.Close()
	if err != nil {
		return err
	}

	return nil
}

func (c *client) SendMessage(ctx context.Context, input SendMessageInput) (*SendMessageOutput, error) {
	var mulError *multierror.Error
	msg := amqp.Publishing{
		Headers:         input.Headers,
		ContentType:     input.ContentType,
		ContentEncoding: input.ContentEncoding,
		DeliveryMode:    input.DeliveryMode,
		Priority:        input.Priority,
		CorrelationId:   input.CorrelationId,
		ReplyTo:         input.ReplyTo,
		Expiration:      input.Expiration,
		MessageId:       input.MessageId,
		Timestamp:       input.Timestamp,
		Type:            input.Type,
		UserId:          input.UserId,
		AppId:           input.AppId,
		Body:            input.Body,
	}

	for i := 0; i < int(c.Config.Settings.RetryCount); i++ {
		if !c.isReady() {
			mulError = multierror.Append(mulError, fmt.Errorf("client not connected to rabbitmq server"))

			continue
		}

		confirm, err := c.pChannel.PublishWithDeferredConfirmWithContext(ctx, input.ExchangeName, input.QueueName, input.Mandatory, input.Immediate, msg)
		if err != nil {
			mulError = multierror.Append(mulError, err)

			continue
		}

		cancelContext, cancelFunc := context.WithTimeout(ctx, c.Config.Settings.Timeout*time.Millisecond)
		defer cancelFunc()

		ack, err := confirm.WaitContext(cancelContext)
		if err != nil {
			mulError = multierror.Append(mulError, fmt.Errorf("could not confirm sending message to server: %w", err))

			continue
		}

		return &SendMessageOutput{Ack: ack}, nil
	}

	return nil, mulError.ErrorOrNil()
}

func (c *client) SendMessageBatch(ctx context.Context, input SendMessageBatchInput) (*SendMessageBatchOutput, error) {
	return nil, nil
}

func (c *client) ReceiveMessage(_ context.Context, input ReceiveMessageInput) (*ReceiveMessageOutput, error) {
	if !c.isReady() {
		return nil, fmt.Errorf("client not to rabbitMq connected")
	}

	deliveryChannel, err := c.cChannel.Consume(input.QueueName, c.name, input.AutoAck, input.Exclusive, input.NoLocal, input.NoWait, input.Attributes)
	if err != nil {
		return nil, fmt.Errorf("could not start consuming messages: %w", err)
	}

	return &ReceiveMessageOutput{
		DeliverChannel: deliveryChannel,
	}, nil
}

func (c *client) NackMessage(ctx context.Context, input NackMessageInput) error {
	if !c.isReady() {
		return fmt.Errorf("client not to rabbitMq connected")
	}

	return c.cChannel.Nack(input.Tag, input.Multiple, input.Requeue)
}

func (c *client) NackMessageBatch(ctx context.Context, input AckMessageBatchInput) error {
	return nil
}

func (c *client) AckMessage(ctx context.Context, input AckMessageInput) error {
	if !c.isReady() {
		return fmt.Errorf("client not to rabbitMq connected")
	}
	if err := c.cChannel.Ack(input.Tag, input.Multiple); err != nil {
		return fmt.Errorf("could not ack message: %w", err)
	}

	return nil
}

func (c *client) AckMessageBatch(ctx context.Context, input AckMessageBatchInput) error {
	return nil
}

func (c *client) QueueExists(_ context.Context, input CreateQueueInput) (bool, error) {
	if !c.isReady() {
		return false, fmt.Errorf("client not to rabbitMq connected")
	}

	_, err := c.cChannel.QueueDeclarePassive(input.QueueName, input.Durable, input.AutoDelete, input.Exclusive, input.NoWait, input.Attributes)
	if err == nil {
		return true, nil
	}

	amqpErr, ok := err.(*amqp.Error)
	if !ok || amqpErr.Code != amqp.PreconditionFailed {
		return false, err
	}

	return false, nil
}

func (c *client) CreateQueue(ctx context.Context, input CreateQueueInput) (*CreateQueueOutput, error) {
	if !c.isReady() {
		return nil, fmt.Errorf("client not to rabbitMq connected")
	}

	queue, err := c.cChannel.QueueDeclare(input.QueueName, input.Durable, input.AutoDelete, input.Exclusive, input.NoWait, input.Attributes)
	if err != nil {
		return nil, fmt.Errorf("could not declare queue: %w", err)
	}

	return &CreateQueueOutput{
		QueueName: queue.Name,
		Consumers: queue.Consumers,
		Messages:  queue.Messages,
	}, nil
}

func (c *client) BindQueue(_ context.Context, input QueueBindInput) error {
	if !c.isReady() {
		return fmt.Errorf("client not to rabbitMq connected")
	}

	if err := c.cChannel.QueueBind(input.QueueName, input.RoutingKey, input.ExchangeName, input.NoWait, input.Attributes); err != nil {
		return fmt.Errorf("could not bind queue to exchange: %w", err)
	}

	return nil
}

func (c *client) CreateExchange(_ context.Context, input CreateExchangeInput) error {
	if !c.isReady() {
		return fmt.Errorf("client not to rabbitMq connected")
	}

	if err := c.cChannel.ExchangeDeclare(input.Name, input.ExchangeType, input.Durable, input.AutoDelete, input.Internal, input.NoWait, input.Attributes); err != nil {
		return fmt.Errorf("could not declare exchange: %w", err)
	}

	return nil
}

func (c *client) ExchangeExists(ctx context.Context, input CreateExchangeInput) (bool, error) {
	if !c.isReady() {
		return false, fmt.Errorf("client not to rabbitMq connected")
	}

	err := c.cChannel.ExchangeDeclarePassive(input.Name, input.ExchangeType, false, false, false, false, nil)
	if err == nil {
		return true, nil
	}

	amqpErr, ok := err.(*amqp.Error)
	if !ok || amqpErr.Code != amqp.PreconditionFailed {
		return false, err
	}

	return false, nil

}

func (c *client) Ping(_ context.Context) error {
	if !c.isReady() {
		return fmt.Errorf("not connected to rabbitmq server")
	}

	return nil
}

func (c *client) isReady() bool {
	return !c.cConnection.IsClosed() && !c.cChannel.IsClosed() && !c.pConnection.IsClosed() && !c.pChannel.IsClosed()
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
