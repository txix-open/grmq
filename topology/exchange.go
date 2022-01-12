package topology

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type Exchange struct {
	Name string
	Type string
	Args amqp.Table
}

func NewDirectExchange(name string) *Exchange {
	return &Exchange{
		Name: name,
		Type: amqp.ExchangeDirect,
		Args: map[string]interface{}{},
	}
}

func NewFanoutExchange(name string) *Exchange {
	return &Exchange{
		Name: name,
		Type: amqp.ExchangeFanout,
		Args: map[string]interface{}{},
	}
}

func NewTopicExchange(name string) *Exchange {
	return &Exchange{
		Name: name,
		Type: amqp.ExchangeTopic,
		Args: map[string]interface{}{},
	}
}
