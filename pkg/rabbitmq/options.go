package rabbitmq

type LoadOptions struct {
	ExchangeName string
	QueueName    string
	RoutingKey   string
}

func WithExchange(name string) ClientOption {
	return func(cfg *ClientConfig) {
		cfg.LoadOptions = append(cfg.LoadOptions, func(options *LoadOptions) error {
			options.ExchangeName = name

			return nil
		})
	}
}

func WithQueue(name string) ClientOption {
	return func(cfg *ClientConfig) {
		cfg.LoadOptions = append(cfg.LoadOptions, func(options *LoadOptions) error {
			options.QueueName = name

			return nil
		})
	}
}

func WithRoutingKey(name string) ClientOption {
	return func(cfg *ClientConfig) {
		cfg.LoadOptions = append(cfg.LoadOptions, func(options *LoadOptions) error {
			options.RoutingKey = name

			return nil
		})
	}
}

func WithEndpoint(endpoint string) ClientOption {
	return func(cfg *ClientConfig) {
		cfg.Settings.Endpoint = endpoint
	}
}
