package stream

import (
	"context"
	"github.com/applike/gosoline/pkg/encoding/json"
	"github.com/applike/gosoline/pkg/tracing"
	"github.com/hashicorp/go-multierror"
)

const (
	AttributeSqsDelaySeconds   = "sqsDelaySeconds"
	AttributeSqsReceiptHandle  = "sqsReceiptHandle"
	AttributeSqsMessageGroupId = "sqsMessageGroupId"
)

type Message struct {
	Trace      *tracing.Trace         `json:"trace"`
	Attributes map[string]interface{} `json:"attributes"`
	Body       string                 `json:"body"`
}

func (m *Message) GetTrace() *tracing.Trace {
	return m.Trace
}

func (m *Message) MarshalToBytes() ([]byte, error) {
	return json.Marshal(*m)
}

func (m *Message) GetReceiptHandler() interface{} {
	var receiptHandleInterface interface{}
	var ok bool

	if receiptHandleInterface, ok = m.Attributes[AttributeSqsReceiptHandle]; !ok {
		return nil
	}

	return receiptHandleInterface
}

func (m *Message) MarshalToString() (string, error) {
	bytes, err := json.Marshal(*m)

	if err != nil {
		return "", err
	}

	return string(bytes), nil
}

func (m *Message) UnmarshalFromBytes(data []byte) error {
	return json.Unmarshal(data, m)
}

func (m *Message) UnmarshalFromString(data string) error {
	return m.UnmarshalFromBytes([]byte(data))
}

func CreateMessage(ctx context.Context, body interface{}) (*Message, error) {
	msg := CreateMessageFromContext(ctx)

	serializedOutput, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	msg.Attributes[AttributeEncoding] = EncodingJson
	msg.Body = string(serializedOutput)

	return msg, nil
}

func CreateMessageFromContext(ctx context.Context) *Message {
	msg := &Message{
		Trace:      &tracing.Trace{},
		Attributes: make(map[string]interface{}),
	}

	if span := tracing.GetSpanFromContext(ctx); span != nil {
		msg.Trace = span.GetTrace()
	}

	return msg
}

type MessageBuilder struct {
	error error

	trace      *tracing.Trace
	attributes map[string]interface{}
	body       string
}

func NewMessageBuilder() *MessageBuilder {
	return &MessageBuilder{
		trace:      &tracing.Trace{},
		attributes: make(map[string]interface{}),
	}
}
func (b *MessageBuilder) FromMessage(msg *Message) *MessageBuilder {
	b.trace = msg.Trace
	b.attributes = msg.Attributes
	b.body = msg.Body

	return b
}

func (b *MessageBuilder) WithContext(ctx context.Context) *MessageBuilder {
	if span := tracing.GetSpanFromContext(ctx); span != nil {
		b.trace = span.GetTrace()
	}

	return b
}

func (b *MessageBuilder) WithBody(body interface{}) *MessageBuilder {
	serialized, err := json.Marshal(body)

	if err != nil {
		b.error = multierror.Append(b.error, err)
		return b
	}

	b.body = string(serialized)

	return b
}

func (b *MessageBuilder) WithSqsDelaySeconds(seconds int64) *MessageBuilder {
	b.attributes[AttributeSqsDelaySeconds] = seconds

	return b
}

func (b *MessageBuilder) WithSqsMessageGroupId(groupId string) *MessageBuilder {
	b.attributes[AttributeSqsMessageGroupId] = groupId

	return b
}

func (b *MessageBuilder) GetMessage() (*Message, error) {
	if b.error != nil {
		return nil, b.error
	}

	msg := &Message{
		Trace:      b.trace,
		Attributes: b.attributes,
		Body:       b.body,
	}

	return msg, nil
}
