package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/hashicorp/go-multierror"
	"github.com/justtrackio/gosoline/pkg/appctx"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/funk"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/justtrackio/gosoline/pkg/mdl"
	"github.com/justtrackio/gosoline/pkg/uuid"
)

const (
	MetadataKeyQueues = "cloud.aws.sqs.queues"
	sqsBatchSize      = 10
)

//go:generate mockery --name Queue
type Queue interface {
	GetName() string
	AckMessage(ctx context.Context, messageTag uint64) error
	AckMessageBatch(ctx context.Context, messageTags []uint64) error
	NackMessage(ctx context.Context, messageTag uint64) error
	NackMessageBatch(ctx context.Context, messageTags []uint64) error
	Receive(ctx context.Context) (<-chan Message, error)
	Send(ctx context.Context, msg *Message) error
	SendBatch(ctx context.Context, messages []*Message) error
}

type Message struct {
	MessageDeduplicationId *string
	Body                   *string
	DeliveryTag            uint64
}

type FifoSettings struct {
	Enabled                   bool `cfg:"enabled" default:"false"`
	ContentBasedDeduplication bool `cfg:"content_based_deduplication" default:"false"`
}

type RedrivePolicy struct {
	Enabled         bool `cfg:"enabled" default:"true"`
	MaxReceiveCount int  `cfg:"max_receive_count" default:"3"`
}

type Properties struct {
	Attributes   map[string]any
	AutoAck      bool
	ClientName   string
	Exclusive    bool
	NoLocal      bool
	NoWait       bool
	QueueName    string
	ExchangeName string
}

type ExchangeSettings struct {
	Name string
	Type string `cfg:"type" default:"direct"`
}

type QueueSettings struct {
	Name string
}

type Settings struct {
	ClientName        string
	Exchange          ExchangeSettings
	ExchangeId        string
	Queue             QueueSettings
	QueueId           string
	RoutingKeys       []string
	VisibilityTimeout int
}

type queue struct {
	logger     log.Logger
	client     Client
	uuidGen    uuid.Uuid
	properties *Properties
}

type metadataQueueKey string

func ProvideQueue(ctx context.Context, config cfg.Config, logger log.Logger, settings *Settings, optFns ...ClientOption) (Queue, error) {
	key := fmt.Sprintf("%s-%s", settings.ClientName, settings.QueueId)

	return appctx.Provide(ctx, metadataQueueKey(key), func() (Queue, error) {
		return NewQueue(ctx, config, logger, settings, optFns...)
	})
}

func NewQueue(ctx context.Context, config cfg.Config, logger log.Logger, settings *Settings, optFns ...ClientOption) (Queue, error) {
	var err error
	var client Client
	var props *Properties

	if client, err = ProvideClient(ctx, config, logger, settings.ClientName, optFns...); err != nil {
		return nil, fmt.Errorf("can not create sqs client %s: %w", settings.ClientName, err)
	}

	srv, err := NewService(ctx, config, logger, settings.ClientName, optFns...)
	if err != nil {
		return nil, fmt.Errorf("can not create service: %w", err)
	}

	_, err = srv.CreateExchange(ctx, *settings)
	if err != nil {
		return nil, fmt.Errorf("could not create or get properties of exchange %s: %w", settings.ExchangeId, err)
	}

	if props, err = srv.CreateQueue(ctx, *settings); err != nil {
		return nil, fmt.Errorf("could not create or get properties of queue %s: %w", settings.QueueId, err)
	}

	if _, err := srv.CreateBinding(ctx, *settings); err != nil {
		return nil, fmt.Errorf("could not create queue bindings: %w", err)
	}

	if err = appctx.MetadataAppend(ctx, MetadataKeyQueues, settings.QueueId); err != nil {
		return nil, fmt.Errorf("can not access the appctx metadata: %w", err)
	}

	return NewQueueWithInterfaces(logger, client, props), nil
}

func NewQueueWithInterfaces(logger log.Logger, client Client, props *Properties) Queue {
	return &queue{
		logger:     logger,
		client:     client,
		uuidGen:    uuid.New(),
		properties: props,
	}
}

func (q *queue) Send(ctx context.Context, msg *Message) error {
	input := &SendMessageInput{
		AppId:           "",
		Body:            []byte(*msg.Body),
		ContentEncoding: "",
		ContentType:     "",
		CorrelationId:   "",
		DeliveryMode:    0,
		ExchangeName:    q.properties.ExchangeName,
		Expiration:      "",
		Headers: map[string]any{
			"x-deduplication-id": msg.MessageDeduplicationId,
		},
		Immediate: false,
		Mandatory: false,
		MessageId: *msg.MessageDeduplicationId,
		Priority:  0,
		QueueName: q.properties.QueueName,
		ReplyTo:   "",
		Timestamp: time.Time{},
		Type:      "",
		UserId:    "",
	}

	_, err := q.client.SendMessage(ctx, *input)

	return err
}

func (q *queue) SendBatch(ctx context.Context, messages []*Message) error {
	logger := q.logger.WithContext(ctx)
	if len(messages) == 0 {
		return nil
	}

	entries := make([]types.SendMessageBatchRequestEntry, len(messages))

	for i := 0; i < len(messages); i++ {
		id := q.uuidGen.NewV4()

		entries[i] = types.SendMessageBatchRequestEntry{
			Id:                     aws.String(id),
			MessageDeduplicationId: messages[i].MessageDeduplicationId,
			MessageBody:            messages[i].Body,
		}
	}

	input := SendMessageBatchInput{}

	_, err := q.client.SendMessageBatch(ctx, input)

	var errRequestTooLong *types.BatchRequestTooLong
	if errors.As(err, &errRequestTooLong) && len(messages) > 1 {
		logger.Info("messages were bigger than the allowed max, splitting them up")

		half := float64(len(messages)) / 2
		chunkSize := int(math.Ceil(half))
		messageChunks := funk.Chunk(messages, chunkSize)

		for _, msgChunk := range messageChunks {
			if err = q.SendBatch(ctx, msgChunk); err != nil {
				return err
			}
		}
		return nil
	}

	return err
}

func (q *queue) Receive(ctx context.Context) (<-chan Message, error) {
	input := &ReceiveMessageInput{
		Attributes: q.properties.Attributes,
		AutoAck:    q.properties.AutoAck,
		Exclusive:  q.properties.Exclusive,
		NoLocal:    q.properties.NoLocal,
		NoWait:     q.properties.NoWait,
		QueueName:  q.GetName(),
	}

	out, err := q.client.ReceiveMessage(ctx, *input)
	if err != nil {
		return nil, err
	}

	msgInput := make(chan Message, 10)
	go func() {
		for rawInput := range out.DeliverChannel {
			msg := Message{
				MessageDeduplicationId: mdl.Box(rawInput.MessageId),
				Body:                   mdl.Box(string(rawInput.Body)),
				DeliveryTag:            rawInput.DeliveryTag,
			}
			msgInput <- msg
		}
	}()

	return msgInput, nil
}

func (q *queue) NackMessage(ctx context.Context, deliveryTag uint64) error {
	return q.client.AckMessage(ctx, AckMessageInput{Tag: deliveryTag, Multiple: false})
}

func (q *queue) NackMessageBatch(ctx context.Context, deliverTags []uint64) error {
	multiError := new(multierror.Error)
	for _, tag := range deliverTags {
		if err := q.NackMessage(ctx, tag); err != nil {
			multiError = multierror.Append(multiError, err)
		}
	}

	return multiError.ErrorOrNil()
}

func (q *queue) AckMessage(ctx context.Context, deliveryTag uint64) error {
	return q.client.AckMessage(ctx, AckMessageInput{Tag: deliveryTag})
}

func (q *queue) AckMessageBatch(ctx context.Context, deliverTags []uint64) error {
	multiError := new(multierror.Error)
	for _, tag := range deliverTags {
		if err := q.AckMessage(ctx, tag); err != nil {
			multiError = multierror.Append(multiError, err)
		}
	}

	return multiError.ErrorOrNil()
}

func (q *queue) GetName() string {
	return q.properties.QueueName
}
