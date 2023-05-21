package env

import (
	"fmt"

	"github.com/justtrackio/gosoline/pkg/cfg"
)

type rabbitmqComponent struct {
	baseComponent
	binding containerBinding
}

func (c *rabbitmqComponent) CfgOptions() []cfg.Option {
	return []cfg.Option{
		cfg.WithConfigMap(map[string]interface{}{
			"rabbitmq": map[string]interface{}{
				c.name: map[string]interface{}{
					"endpoint": fmt.Sprintf("amqp://%s:%s", c.binding.host, c.binding.port),
				},
			},
		}),
	}
}
