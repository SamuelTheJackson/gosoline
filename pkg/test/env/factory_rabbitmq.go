package env

import (
	"fmt"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

func init() {
	componentFactories[componentRabbitmq] = new(rabbitmqFactory)
}

const componentRabbitmq = "rabbitmq"

type rabbitmqSettings struct {
	ComponentBaseSettings
	ComponentContainerSettings
	Port int `cfg:"port" default:"5672"`
}

type rabbitmqFactory struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

func (f *rabbitmqFactory) Detect(config cfg.Config, manager *ComponentsConfigManager) error {
	if !config.IsSet("rabbitmq") {
		return nil
	}

	if !manager.ShouldAutoDetect(componentRabbitmq) {
		return nil
	}

	if manager.HasType(componentRabbitmq) {
		return nil
	}

	settings := &rabbitmqSettings{}
	config.UnmarshalDefaults(settings)

	settings.Type = componentRabbitmq

	if err := manager.Add(settings); err != nil {
		return fmt.Errorf("can not add default rabbitmq component: %w", err)
	}

	return nil
}

func (f *rabbitmqFactory) GetSettingsSchema() ComponentBaseSettingsAware {
	return &rabbitmqSettings{}
}

func (f *rabbitmqFactory) DescribeContainers(settings interface{}) componentContainerDescriptions {
	return componentContainerDescriptions{
		"main": {
			containerConfig: f.configureContainer(settings),
			healthCheck:     f.healthCheck(),
		},
	}
}

func (f *rabbitmqFactory) configureContainer(settings interface{}) *containerConfig {
	s := settings.(*rabbitmqSettings)

	return &containerConfig{
		Repository: "rabbitmq",
		Tag:        "3.12-rc-alpine",
		PortBindings: portBindings{
			"5672/tcp": s.Port,
		},
		ExpireAfter:          s.ExpireAfter,
		UseExternalContainer: true,
	}
}

func (f *rabbitmqFactory) healthCheck() ComponentHealthCheck {
	return func(container *container) error {
		address := f.address(container)

		var err error
		f.connection, err = amqp.Dial(address)
		if err != nil {
			return err
		}

		f.channel, err = f.connection.Channel()
		if err != nil {
			return err
		}

		return nil
	}
}

func (f *rabbitmqFactory) Component(_ cfg.Config, _ log.Logger, containers map[string]*container, _ interface{}) (Component, error) {
	component := &RabbitmqComponent{
		address: f.address(containers["main"]),
	}

	return component, nil
}

func (f *rabbitmqFactory) address(container *container) string {
	binding := container.bindings["5672/tcp"]
	address := fmt.Sprintf("amqp://%s:%s", binding.host, binding.port)

	return address
}
