package rabbitmq

import (
	"context"
	"fmt"
	"sync"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/dx"
	"github.com/justtrackio/gosoline/pkg/log"
)

const (
	DefaultVisibilityTimeout = 30000
)

type ServiceSettings struct {
	AutoCreate bool
}

type service struct {
	lck      sync.Mutex
	logger   log.Logger
	client   Client
	settings *ServiceSettings
}

type Service interface {
	CreateQueue(ctx context.Context, settings Settings) (*Properties, error)
	CreateExchange(ctx context.Context, settings Settings) (*Properties, error)
	CreateBinding(ctx context.Context, settings Settings) (*Properties, error)
}

func NewService(ctx context.Context, config cfg.Config, logger log.Logger, clientName string, optFns ...ClientOption) (Service, error) {
	client, err := ProvideClient(ctx, config, logger, clientName, optFns...)
	if err != nil {
		return nil, err
	}

	settings := &ServiceSettings{
		AutoCreate: dx.ShouldAutoCreate(config),
	}

	return NewServiceWithInterfaces(logger, client, settings), nil
}

func NewServiceWithInterfaces(logger log.Logger, client Client, settings *ServiceSettings) *service {
	return &service{
		logger:   logger,
		client:   client,
		settings: settings,
	}
}

func (s service) CreateQueue(ctx context.Context, settings Settings) (*Properties, error) {
	s.lck.Lock()
	s.lck.Unlock()

	attributes := make(map[string]any)

	visibilityTimeout := DefaultVisibilityTimeout
	if settings.VisibilityTimeout > 0 {
		visibilityTimeout = settings.VisibilityTimeout
	}

	attributes["x-message-ttl"] = visibilityTimeout
	attributes["x-message-deduplication"] = settings.MessageDeduplication

	rabbitMqInput := CreateQueueInput{
		Attributes: attributes,
	}
	exists, err := s.QueueExists(ctx, rabbitMqInput)
	if err != nil {
		return nil, fmt.Errorf("could not check if quueue exists: %w", err)
	}

	if exists {
		return s.GetPropertiesByName(ctx, settings.Queue.Name)
	}

	if !exists && !s.settings.AutoCreate {
		return nil, fmt.Errorf("sqs queue with name %s does not exist", settings.Queue.Name)
	}
	//TODO dead letter

	props, err := s.doCreateQueue(ctx, rabbitMqInput)
	if err != nil {
		return nil, err
	}

	return props, nil
}

func (s service) QueueExists(ctx context.Context, input CreateQueueInput) (bool, error) {
	s.lck.Lock()
	s.lck.Unlock()

	s.logger.WithFields(log.Fields{
		"name": input.QueueName,
	}).Info("checking the existence of sqs queue")

	return s.client.QueueExists(ctx, CreateQueueInput{
		Attributes: input.Attributes,
		AutoDelete: input.AutoDelete,
		Durable:    input.Durable,
		Exclusive:  input.Exclusive,
		NoWait:     input.NoWait,
		QueueName:  input.QueueName,
	})

}

func (s service) ExchangeExists(ctx context.Context, input CreateExchangeInput) (bool, error) {
	s.lck.Lock()
	s.lck.Unlock()

	s.logger.WithFields(log.Fields{
		"name": input.Name,
	}).Info("checking the existence of rabbitmq exchange")

	return s.client.ExchangeExists(ctx, input)

}

func (s service) doCreateQueue(ctx context.Context, input CreateQueueInput) (*Properties, error) {
	name := input.QueueName
	s.logger.Info("trying to create rabbitmq queue: %v", name)

	if _, err := s.client.CreateQueue(ctx, input); err != nil {
		s.logger.Error("could not create rabbitmq queue %v: %w", name, err)

		return nil, err
	}

	s.logger.Info("created sqs queue %v", name)

	return s.GetPropertiesByName(ctx, name)
}

func (s service) doCreateExchange(ctx context.Context, input CreateExchangeInput) (*Properties, error) {
	name := input.Name
	s.logger.Info("trying to create rabbitmq queue: %v", name)

	if err := s.client.CreateExchange(ctx, input); err != nil {
		s.logger.Error("could not create rabbitmq queue %v: %w", name, err)

		return nil, err
	}

	s.logger.Info("created sqs queue %v", name)

	return s.GetPropertiesByName(ctx, name)
}

func (s service) CreateExchange(ctx context.Context, settings Settings) (*Properties, error) {
	s.lck.Lock()
	s.lck.Unlock()

	attributes := make(map[string]any)
	attributes["x-message-deduplication"] = settings.MessageDeduplication

	rabbitMqInput := CreateExchangeInput{
		Attributes:   attributes,
		ExchangeType: settings.Exchange.Type,
		Name:         settings.Exchange.Name,
	}
	exists, err := s.ExchangeExists(ctx, rabbitMqInput)
	if err != nil {
		return nil, fmt.Errorf("could not check if quueue exists: %w", err)
	}

	if exists {
		return s.GetPropertiesByName(ctx, settings.Exchange.Name)
	}

	if !exists && !s.settings.AutoCreate {
		return nil, fmt.Errorf("sqs queue with name %s does not exist", settings.Exchange.Name)
	}

	props, err := s.doCreateExchange(ctx, rabbitMqInput)
	if err != nil {
		return nil, err
	}

	return props, nil
}

func (s service) CreateBinding(ctx context.Context, settings Settings) (*Properties, error) {
	attributes := make(map[string]any)
	attributes["x-message-deduplication"] = settings.MessageDeduplication

	if len(settings.RoutingKeys) == 0 {
		if err := s.client.BindQueue(ctx, QueueBindInput{
			Attributes:   attributes,
			QueueName:    settings.Queue.Name,
			ExchangeName: settings.Exchange.Name,
		}); err != nil {
			return nil, fmt.Errorf("could not bind queue: %w", err)
		}

		return &Properties{}, nil
	}

	for _, key := range settings.RoutingKeys {
		if err := s.client.BindQueue(ctx, QueueBindInput{
			Attributes:   attributes,
			QueueName:    settings.Queue.Name,
			RoutingKey:   key,
			ExchangeName: settings.Exchange.Name,
		}); err != nil {
			return nil, fmt.Errorf("could not bind queue: %w", err)
		}
	}

	return &Properties{}, nil
}

func (s service) GetPropertiesByName(ctx context.Context, name string) (*Properties, error) {
	properties := &Properties{}

	return properties, nil
}
