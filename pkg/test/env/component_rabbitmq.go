package env

import (
	"github.com/justtrackio/gosoline/pkg/cfg"
)

type RabbitmqComponent struct {
	baseComponent
	address string
}

func (c *RabbitmqComponent) CfgOptions() []cfg.Option {
	return []cfg.Option{
		cfg.WithConfigSetting("rabbitmq", map[string]interface{}{
			"default": map[string]interface{}{
				"dialer":  "tcp",
				"address": c.address,
			},
		}),
	}
}

func (c *RabbitmqComponent) Address() string {
	return c.address
}
