package stream

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/funk"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/justtrackio/gosoline/pkg/mdl"
	"github.com/justtrackio/gosoline/pkg/rabbitmq"
	"github.com/spf13/cast"
)

const RabbitmqOutputBatchSize = 10

type RabbitmqOutputSettings struct {
	cfg.AppId
	ClientName           string
	ExchangeId           string
	QueueId              string
	QueueNamePattern     string
	RoutingKeys          []string
	VisibilityTimeout    int
	MessageDeduplication bool
}

func (s RabbitmqOutputSettings) GetAppId() cfg.AppId {
	return s.AppId
}

func (s RabbitmqOutputSettings) GetClientName() string {
	return s.ClientName
}

func (s RabbitmqOutputSettings) GetQueueId() string {
	return s.QueueId
}

func (s RabbitmqOutputSettings) GetExchangeId() string {
	return s.ExchangeId
}

type rabbitmqOutput struct {
	logger   log.Logger
	queue    rabbitmq.Queue
	settings *RabbitmqOutputSettings
}

func NewRabbitmqOutput(ctx context.Context, config cfg.Config, logger log.Logger, settings *RabbitmqOutputSettings) (Output, error) {
	settings.PadFromConfig(config)

	queueSettings, err := rabbitmq.GetQueueSettings(config, settings)
	if err != nil {
		return nil, fmt.Errorf("can not get rabbitmq queue name: %w", err)
	}
	exchangeSettings, err := rabbitmq.GetExchangeSettings(config, settings)
	if err != nil {
		return nil, fmt.Errorf("can not get rabbitmq queue name: %w", err)
	}

	rabbitSettings := &rabbitmq.Settings{
		ClientName: settings.ClientName,
		Exchange: rabbitmq.ExchangeSettings{
			Name: exchangeSettings.Name,
			Type: exchangeSettings.Type,
		},
		Queue: rabbitmq.QueueSettings{
			Name: queueSettings.Name,
		},
		RoutingKeys:          settings.RoutingKeys,
		VisibilityTimeout:    settings.VisibilityTimeout,
		MessageDeduplication: settings.MessageDeduplication,
	}

	queue, err := rabbitmq.ProvideQueue(ctx, config, logger, rabbitSettings)
	if err != nil {
		return nil, fmt.Errorf("can not create queue: %w", err)
	}

	return NewRabbitmqOutputWithInterfaces(logger, queue, settings), nil
}

func NewRabbitmqOutputWithInterfaces(logger log.Logger, queue rabbitmq.Queue, settings *RabbitmqOutputSettings) Output {
	return &rabbitmqOutput{
		logger:   logger,
		queue:    queue,
		settings: settings,
	}
}

func (o *rabbitmqOutput) WriteOne(ctx context.Context, record WritableMessage) error {
	rabbitmqMessage, err := o.buildRabbitmqMessage(ctx, record)
	if err != nil {
		return fmt.Errorf("could not build rabbitmq message: %w", err)
	}

	err = o.queue.Send(ctx, rabbitmqMessage)
	if err != nil {
		return fmt.Errorf("could not send rabbitmq message: %w", err)
	}

	return nil
}

func (o *rabbitmqOutput) Write(ctx context.Context, batch []WritableMessage) error {
	chunks := funk.Chunk(batch, RabbitmqOutputBatchSize)

	var result error

	for _, chunk := range chunks {
		messages, err := o.buildRabbitmqMessages(ctx, chunk)
		if err != nil {
			result = multierror.Append(result, err)
		}

		if len(messages) == 0 {
			continue
		}

		err = o.queue.SendBatch(ctx, messages)

		if err != nil {
			result = multierror.Append(result, err)
		}
	}

	if result != nil {
		return fmt.Errorf("there were errors on writing to the rabbitmq stream: %w", result)
	}

	return nil
}

func (o *rabbitmqOutput) GetMaxMessageSize() *int {
	return mdl.Box(64 * 1024)
}

func (o *rabbitmqOutput) GetMaxBatchSize() *int {
	return mdl.Box(10)
}

func (o *rabbitmqOutput) buildRabbitmqMessages(ctx context.Context, messages []WritableMessage) ([]*rabbitmq.Message, error) {
	var result error
	rabbitmqMessages := make([]*rabbitmq.Message, 0)

	for _, msg := range messages {
		rabbitmqMessage, err := o.buildRabbitmqMessage(ctx, msg)
		if err != nil {
			result = multierror.Append(result, err)
			continue
		}

		rabbitmqMessages = append(rabbitmqMessages, rabbitmqMessage)
	}

	return rabbitmqMessages, result
}

func (o *rabbitmqOutput) buildRabbitmqMessage(ctx context.Context, msg WritableMessage) (*rabbitmq.Message, error) {
	var err error
	var messageDeliveryTag uint64
	var messageDeduplicationId string
	var messageId string

	attributes := getAttributes(msg)

	if d, ok := attributes[rabbitmq.AttributeDeduplication]; ok {
		if messageDeduplicationId, err = cast.ToStringE(d); err != nil {
			return nil, fmt.Errorf("the type of the %s attribute with value %v should be castable to string: %w", rabbitmq.AttributeDeduplication, attributes[rabbitmq.AttributeDeduplication], err)
		}
	}

	if d, ok := attributes[rabbitmq.AttributeRabbitmqDeliveryTag]; ok {
		if messageDeliveryTag, err = cast.ToUint64E(d); err != nil {
			return nil, fmt.Errorf("the type of the %s attribute with value %v should be castable to string: %w", rabbitmq.AttributeRabbitmqDeliveryTag, attributes[rabbitmq.AttributeRabbitmqDeliveryTag], err)
		}
	}

	if d, ok := attributes[rabbitmq.AttributeMessageId]; ok {
		if messageId, err = cast.ToStringE(d); err != nil {
			return nil, fmt.Errorf("the type of the %s attribute with value %v should be castable to string: %w", rabbitmq.AttributeMessageId, attributes[rabbitmq.AttributeMessageId], err)
		}
	}

	body, err := msg.MarshalToString()
	if err != nil {
		return nil, err
	}

	rabbitmqMessage := &rabbitmq.Message{
		DeduplicationId: mdl.NilIfEmpty(messageDeduplicationId),
		MessageId:       messageId,
		Body:            mdl.Box(body),
		DeliveryTag:     messageDeliveryTag,
	}

	return rabbitmqMessage, nil
}
