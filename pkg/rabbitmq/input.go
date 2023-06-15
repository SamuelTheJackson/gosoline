package rabbitmq

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SendMessageInput struct {
	AppId           string
	Body            []byte
	ContentEncoding string
	ContentType     string
	CorrelationId   string
	DeliveryMode    uint8
	ExchangeName    string
	Expiration      string
	Headers         map[string]any
	Immediate       bool
	Mandatory       bool
	MessageId       string
	Priority        uint8
	QueueName       string
	ReplyTo         string
	Timestamp       time.Time
	Type            string
	UserId          string
}
type SendMessageOutput struct {
	Ack bool
}

type SendMessageBatchInput struct{}
type SendMessageBatchOutput struct{}

type ReceiveMessageInput struct {
	Attributes   map[string]any
	AutoAck      bool
	Exclusive    bool
	NoLocal      bool
	NoWait       bool
	ConsumerName string
	QueueName    string
}
type ReceiveMessageOutput struct {
	DeliverChannel <-chan amqp.Delivery
}

type AckMessageInput struct {
	Tag      uint64
	Multiple bool
}

type AckMessageBatchInput struct{}

type NackMessageInput struct {
	Tag      uint64
	Multiple bool
	Requeue  bool
}

type CreateQueueInput struct {
	Attributes map[string]any
	AutoDelete bool
	Durable    bool
	Exclusive  bool
	NoWait     bool
	QueueName  string
}

type CreateQueueOutput struct {
	QueueName string
	Consumers int
	Messages  int
}

type CreateExchangeInput struct {
	Attributes   map[string]any
	AutoDelete   bool
	Durable      bool
	ExchangeType string
	Internal     bool
	Name         string
	NoWait       bool
}
type CreateExchangeOutput struct{}

type QueueBindInput struct {
	Attributes   map[string]any
	ExchangeName string
	NoWait       bool
	QueueName    string
	RoutingKey   string
}

type QueueBindOutput struct {
}
