package stream

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/hashicorp/go-multierror"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/coffin"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/justtrackio/gosoline/pkg/rabbitmq"
)

var _ AcknowledgeableInput = &rabbitmqInput{}

type RabbitmqInputSettings struct {
	cfg.AppId
	ExchangeId   string                    `cfg:"exchange_id"`
	QueueId      string                    `cfg:"queue_id"`
	Exchange     rabbitmq.ExchangeSettings `cfg:"exchange"`
	RunnerCount  int                       `cfg:"runner_count"`
	ClientName   string                    `cfg:"client_name"`
	Unmarshaller string                    `cfg:"unmarshaller" default:"msg"`
}

func (s RabbitmqInputSettings) GetAppId() cfg.AppId {
	return s.AppId
}

func (s RabbitmqInputSettings) GetClientName() string {
	return s.ClientName
}

func (s RabbitmqInputSettings) GetQueueId() string {
	return s.QueueId
}

func (s RabbitmqInputSettings) GetExchangeId() string {
	return s.ExchangeId
}

func (s RabbitmqInputSettings) IsFifoEnabled() bool {
	return false
}

type rabbitmqInput struct {
	logger      log.Logger
	settings    *RabbitmqInputSettings
	unmarshaler UnmarshallerFunc
	queue       rabbitmq.Queue
	cfn         coffin.Coffin
	channel     chan *Message
	stopped     int32
}

func NewRabbitmqInput(ctx context.Context, config cfg.Config, logger log.Logger, settings *RabbitmqInputSettings) (*rabbitmqInput, error) {
	settings.AppId.PadFromConfig(config)

	var ok bool
	var err error
	var unmarshaller UnmarshallerFunc

	queueSettings, err := rabbitmq.GetQueueSettings(config, settings)
	if err != nil {
		return nil, fmt.Errorf("can not get rabbitmq queue name: %w", err)
	}

	exchangeSettings, err := rabbitmq.GetExchangeName(config, settings)
	if err != nil {
		return nil, fmt.Errorf("can not get rabbitmq exchange name: %w", err)
	}

	queue, err := rabbitmq.ProvideQueue(ctx, config, logger, &rabbitmq.Settings{
		Queue:      *queueSettings,
		ClientName: settings.ClientName,
		Exchange:   *exchangeSettings,
	})
	if err != nil {
		return nil, fmt.Errorf("can not create queue: %w", err)
	}

	if unmarshaller, ok = unmarshallers[settings.Unmarshaller]; !ok {
		return nil, fmt.Errorf("unknown unmarshaller %s", settings.Unmarshaller)
	}

	return NewRabbitmqInputWithInterfaces(logger, queue, unmarshaller, settings), nil
}

func NewRabbitmqInputWithInterfaces(logger log.Logger, queue rabbitmq.Queue, unmarshaller UnmarshallerFunc, settings *RabbitmqInputSettings) *rabbitmqInput {
	if settings.RunnerCount <= 0 {
		settings.RunnerCount = 1
	}

	return &rabbitmqInput{
		logger:      logger,
		queue:       queue,
		settings:    settings,
		unmarshaler: unmarshaller,
		cfn:         coffin.New(),
		channel:     make(chan *Message, 10),
	}
}

func (i *rabbitmqInput) Data() <-chan *Message {
	return i.channel
}

func (i *rabbitmqInput) Run(ctx context.Context) error {
	defer close(i.channel)
	defer i.logger.Info("leaving rabbitmq input")

	i.logger.Info("starting rabbitmq input with %d runners", i.settings.RunnerCount)

	for j := 0; j < i.settings.RunnerCount; j++ {
		i.cfn.Gof(func() error {
			return i.runLoop(ctx)
		}, "panic in rabbitmq input runner")
	}

	<-i.cfn.Dying()

	i.Stop()

	return i.cfn.Wait()
}

func (i *rabbitmqInput) runLoop(ctx context.Context) error {
	defer i.logger.Info("leaving rabbitmq input runner")

	rabbitmqMessages, err := i.queue.Receive(ctx)
	if err != nil {
		i.logger.Error("could not get messages from rabbitmq: %w", err)

		return err
	}

	for rabbitmqMessage := range rabbitmqMessages {
		if atomic.LoadInt32(&i.stopped) != 0 {
			return nil
		}

		msg, err := i.unmarshaler(rabbitmqMessage.Body)
		if err != nil {
			i.logger.Error("could not unmarshal message: %w", err)

			continue
		}

		if msg.Attributes == nil {
			msg.Attributes = make(map[string]interface{})
		}

		msg.Attributes[AttributeRabbitmqDeliveryTag] = rabbitmqMessage.DeliveryTag
		msg.Attributes[AttributeRabbitmqMessageId] = rabbitmqMessage.MessageDeduplicationId

		i.channel <- msg
	}

	return nil
}

func (i *rabbitmqInput) Stop() {
	atomic.StoreInt32(&i.stopped, 1)
}

func (i *rabbitmqInput) Ack(ctx context.Context, msg *Message, ack bool) error {
	messageTagI, ok := msg.Attributes[AttributeRabbitmqDeliveryTag]
	if !ok {
		return fmt.Errorf("the message has no attribute %s", AttributeRabbitmqDeliveryTag)
	}

	messageTag, ok := messageTagI.(uint64)
	if !ok {
		return fmt.Errorf("expected messagTag type uint64 but got %T", messageTagI)
	}

	if !ack {
		return i.queue.NackMessage(ctx, messageTag)
	}

	return i.queue.AckMessage(ctx, messageTag)
}

func (i *rabbitmqInput) AckBatch(ctx context.Context, msgs []*Message, acks []bool) error {
	deliveryTagsAck := make([]uint64, 0)
	deliveryTagsNack := make([]uint64, 0)
	multiError := new(multierror.Error)

	for i := range msgs {
		var (
			msg = msgs[i]
			ack = acks[i]
		)

		messageTagI, ok := msg.Attributes[AttributeRabbitmqDeliveryTag]
		if !ok {
			multiError = multierror.Append(multiError, fmt.Errorf("the message has no attribute %s", AttributeRabbitmqDeliveryTag))

			continue
		}

		deliveryTag, ok := messageTagI.(uint64)
		if !ok {
			multiError = multierror.Append(multiError, fmt.Errorf("expected messagTag type uint64 but got %T", messageTagI))

			continue
		}

		if ack {
			deliveryTagsAck = append(deliveryTagsAck, deliveryTag)
		} else {
			deliveryTagsNack = append(deliveryTagsNack, deliveryTag)
		}
	}

	if err := i.queue.NackMessageBatch(ctx, deliveryTagsNack); err != nil {
		multiError = multierror.Append(multiError, err)
	}

	if err := i.queue.AckMessageBatch(ctx, deliveryTagsAck); err != nil {
		multiError = multierror.Append(multiError, err)
	}

	return multiError.ErrorOrNil()
}

func (i *rabbitmqInput) HasRetry() bool {
	return true
}

func (i *rabbitmqInput) SetUnmarshaler(unmarshaler UnmarshallerFunc) {
	i.unmarshaler = unmarshaler
}
