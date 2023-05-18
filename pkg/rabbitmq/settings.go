package rabbitmq

import (
	"fmt"

	"github.com/justtrackio/gosoline/pkg/cfg"
)

func unmarshalClientSettings(config cfg.Config, name string) ClientSettings {
	settings := ClientSettings{}
	if name == "" {
		name = "default"
	}

	clientsKey := getClientConfigKey(name)
	defaultClientKey := getClientConfigKey("default")

	config.UnmarshalKey(clientsKey, &settings, []cfg.UnmarshalDefaults{
		cfg.UnmarshalWithDefaultsFromKey("rabbitmq.defaults.endpoint", "endpoint"),
		cfg.UnmarshalWithDefaultsFromKey("rabbitmq.defaults.timeout", "timeout"),
		cfg.UnmarshalWithDefaultsFromKey("rabbitmq.defaults.retryCount", "retryCount"),
		cfg.UnmarshalWithDefaultsFromKey(defaultClientKey, "."),
	}...)

	return settings
}

func getClientConfigKey(name string) string {
	return fmt.Sprintf("rabbitmq.%s", name)
}
