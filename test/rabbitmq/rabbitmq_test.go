//go:build integration
// +build integration

package ddb_test

import (
	"context"
	"testing"
	"time"

	"github.com/justtrackio/gosoline/pkg/clock"
	"github.com/justtrackio/gosoline/pkg/rabbitmq"
	"github.com/justtrackio/gosoline/pkg/test/suite"
)

type RabbitmqTestSuite struct {
	suite.Suite
	clock clock.FakeClock
	queue rabbitmq.Queue
}

func (s *RabbitmqTestSuite) SetupSuite() []suite.Option {
	s.clock = clock.NewFakeClockAt(time.Now().UTC())

	return []suite.Option{
		suite.WithLogLevel("debug"),
		suite.WithClockProvider(s.clock),
		suite.WithConfigFile("./config.dist.yml"),
	}
}

func (s *RabbitmqTestSuite) SetupTest() error {
	return nil
}

func (s *RabbitmqTestSuite) TestPublishAndConsumeMessage() {
	ctx := context.Background()

	client, err := rabbitmq.NewClient(s.Env().Context(), s.Env().Config(), s.Env().Logger(), "test")
	s.NoError(err, "could not create client")

	err = client.Ping(ctx)
	s.NoError(err)

	_, err = client.CreateQueue(ctx, rabbitmq.CreateQueueInput{
		QueueName: "test",
		Attributes: map[string]any{
			"x-max-length":            10000,
			"x-message-deduplication": true,
		},
	})
	s.NoError(err)

	err = client.CreateExchange(ctx, rabbitmq.CreateExchangeInput{
		Attributes: map[string]any{
			"x-max-length":            10000,
			"x-message-deduplication": true,
		},
		ExchangeType: "direct",
		Name:         "test",
	})
	s.NoError(err)

	err = client.BindQueue(ctx, rabbitmq.QueueBindInput{
		ExchangeName: "test",
		QueueName:    "test",
	})
	s.NoError(err)

	for i := 0; i < 100; i++ {
		_, err = client.SendMessage(ctx, rabbitmq.SendMessageInput{
			ExchangeName: "test",
			Body:         []byte("hallo"),
		})
		s.NoError(err)
	}

	out, err := client.ReceiveMessage(ctx, rabbitmq.ReceiveMessageInput{
		QueueName: "test",
	})
	s.NoError(err)

	counter := 0
	for msg := range out.DeliverChannel {
		s.Equal("hallo", string(msg.Body))
		s.NoError(msg.Ack(false))
		counter++
		if counter == 100 {
			break
		}
	}

	select {
	case msg := <-out.DeliverChannel:
		s.Fail("didn't expect a new message but got a message: %w", msg)
	default:

	}
}

func TestRabbitmq(t *testing.T) {
	suite.Run(t, new(RabbitmqTestSuite))
}
