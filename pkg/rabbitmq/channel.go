package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

type channel struct {
	logger    log.Logger
	ch        *amqp.Channel
	mutex     *sync.Mutex
	errorChan chan *amqp.Error
	rdy       bool
	con       Connection
}

type Channel interface {
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

func NewChannel(ctx context.Context, config cfg.Config, logger log.Logger, conn Connection) (Channel, error) {
	ch, err := conn.CreateChannel()
	if err != nil {
		return nil, err
	}

	errorChan := make(chan *amqp.Error)
	ch.NotifyClose(errorChan)

	mu := new(sync.Mutex)
	chane := &channel{
		logger:    logger,
		ch:        ch,
		mutex:     mu,
		errorChan: errorChan,
		rdy:       true,
		con:       conn,
	}

	go chane.reconnect(ctx)

	return chane, err
}

func (c *channel) reconnect(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("closing channel due ctx done")
			c.rdy = false
			if err := c.ch.Close(); err != nil {
				c.logger.Error(err.Error())
			}

			return
		case err := <-c.errorChan:
			c.mutex.Lock()
			c.rdy = false
			c.logger.Error("disconnected from channel: %w", err)

			for !c.rdy {
				c.logger.Info("trying to reconnect the channel")

				ch, err := c.con.CreateChannel()
				if err != nil {
					c.logger.Error("could not reconnect to rabbitmq server: %w", err)
					time.Sleep(2 * time.Second)

					continue
				}

				c.errorChan = make(chan *amqp.Error)
				ch.NotifyClose(c.errorChan)

				c.ch = ch
				c.rdy = true
			}

			c.logger.Info("reconnected to channel")

			c.mutex.Unlock()
			break
		}
	}
}

func (c *channel) isReady(ctx context.Context) bool {
	if c.rdy {
		return true
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.rdy
}

func (c *channel) AckMessage(ctx context.Context, input AckMessageInput) error {
	if !c.isReady(ctx) {
		return fmt.Errorf("client not to rabbitMq connected")
	}
	if err := c.ch.Ack(input.Tag, input.Multiple); err != nil {
		return fmt.Errorf("could not ack message: %w", err)
	}

	return nil
}

func (c *channel) AckMessageBatch(ctx context.Context, input AckMessageBatchInput) error {
	return nil
}

func (c *channel) BindQueue(ctx context.Context, input QueueBindInput) error {
	if !c.isReady(ctx) {
		return fmt.Errorf("client not to rabbitMq connected")
	}

	if err := c.ch.QueueBind(input.QueueName, input.RoutingKey, input.ExchangeName, input.NoWait, input.Attributes); err != nil {
		return fmt.Errorf("could not bind queue to exchange: %w", err)
	}

	return nil
}

func (c *channel) CreateExchange(ctx context.Context, input CreateExchangeInput) error {
	if !c.isReady(ctx) {
		return fmt.Errorf("client not to rabbitMq connected")
	}

	if err := c.ch.ExchangeDeclare(input.Name, input.ExchangeType, input.Durable, input.AutoDelete, input.Internal, input.NoWait, input.Attributes); err != nil {
		return fmt.Errorf("could not declare exchange: %w", err)
	}

	return nil
}

func (c *channel) ExchangeExists(ctx context.Context, input CreateExchangeInput) (bool, error) {
	if !c.isReady(ctx) {
		return false, fmt.Errorf("client not to rabbitMq connected")
	}

	err := c.ch.ExchangeDeclarePassive(input.Name, input.ExchangeType, input.Durable, input.AutoDelete, input.Internal, input.NoWait, input.Attributes)
	if err == nil {
		return true, nil
	}

	amqpErr, ok := err.(*amqp.Error)
	if !ok || amqpErr.Code != amqp.NotFound {
		return false, err
	}

	return false, nil
}

func (c *channel) CreateQueue(ctx context.Context, input CreateQueueInput) (*CreateQueueOutput, error) {
	if !c.isReady(ctx) {
		return nil, fmt.Errorf("client not to rabbitMq connected")
	}

	queue, err := c.ch.QueueDeclare(input.QueueName, input.Durable, input.AutoDelete, input.Exclusive, input.NoWait, input.Attributes)
	if err != nil {
		return nil, fmt.Errorf("could not declare queue: %w", err)
	}

	return &CreateQueueOutput{
		QueueName: queue.Name,
		Consumers: queue.Consumers,
		Messages:  queue.Messages,
	}, nil
}

func (c *channel) NackMessage(ctx context.Context, input NackMessageInput) error {
	if !c.isReady(ctx) {
		return fmt.Errorf("client not to rabbitMq connected")
	}

	return c.ch.Nack(input.Tag, input.Multiple, input.Requeue)
}

func (c *channel) NackMessageBatch(ctx context.Context, input AckMessageBatchInput) error {
	return nil
}

func (c *channel) QueueExists(ctx context.Context, input CreateQueueInput) (bool, error) {
	if !c.isReady(ctx) {
		return false, fmt.Errorf("client not to rabbitMq connected")
	}

	_, err := c.ch.QueueDeclarePassive(input.QueueName, input.Durable, input.AutoDelete, input.Exclusive, input.NoWait, input.Attributes)
	if err == nil {
		return true, nil
	}

	amqpErr, ok := err.(*amqp.Error)
	if !ok || amqpErr.Code != amqp.NotFound {
		return false, err
	}

	return false, nil
}

func (c *channel) ReceiveMessage(ctx context.Context, input ReceiveMessageInput) (*ReceiveMessageOutput, error) {
	if !c.isReady(ctx) {
		return nil, fmt.Errorf("client not to rabbitMq connected")
	}

	deliveryChannel, err := c.ch.Consume(input.QueueName, input.ConsumerName, input.AutoAck, input.Exclusive, input.NoLocal, input.NoWait, input.Attributes)
	if err != nil {
		return nil, fmt.Errorf("could not start consuming messages: %w", err)
	}

	return &ReceiveMessageOutput{
		DeliverChannel: deliveryChannel,
	}, nil
}

func (c *channel) SendMessage(ctx context.Context, input SendMessageInput) (*SendMessageOutput, error) {
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

	for i := 0; i < int(5); i++ {
		if !c.isReady(ctx) {
			mulError = multierror.Append(mulError, fmt.Errorf("client not connected to rabbitmq server"))

			continue
		}

		confirm, err := c.ch.PublishWithDeferredConfirmWithContext(ctx, input.ExchangeName, input.QueueName, input.Mandatory, input.Immediate, msg)
		if err != nil {
			mulError = multierror.Append(mulError, err)

			continue
		}

		cancelContext, cancelFunc := context.WithTimeout(ctx, 20*time.Millisecond)
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

func (c *channel) SendMessageBatch(ctx context.Context, input SendMessageBatchInput) (*SendMessageBatchOutput, error) {
	return nil, nil
}

func (c *channel) Ping(ctx context.Context) error {
	if !c.isReady(ctx) {
		return fmt.Errorf("not connected to rabbitmq server")
	}

	return nil

}
