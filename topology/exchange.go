package topology

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// Exchange represents a RabbitMQ exchange configuration.
type Exchange struct {
	Name string
	Type string
	Args amqp.Table
}

// NewDirectExchange creates a new direct exchange.
func NewDirectExchange(name string) *Exchange {
	return &Exchange{
		Name: name,
		Type: amqp.ExchangeDirect,
		Args: map[string]any{},
	}
}

// NewFanoutExchange creates a new fanout exchange.
func NewFanoutExchange(name string) *Exchange {
	return &Exchange{
		Name: name,
		Type: amqp.ExchangeFanout,
		Args: map[string]any{},
	}
}

// NewTopicExchange creates a new topic exchange.
func NewTopicExchange(name string) *Exchange {
	return &Exchange{
		Name: name,
		Type: amqp.ExchangeTopic,
		Args: map[string]any{},
	}
}
