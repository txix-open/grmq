package topology

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type Binding struct {
	ExchangeName string
	QueueName    string
	RoutingKey   string
	Args         amqp.Table
}

func NewBinding(exchangeName string, queueName string, routingKey string) *Binding {
	return &Binding{
		ExchangeName: exchangeName,
		QueueName:    queueName,
		RoutingKey:   routingKey,
		Args:         map[string]any{},
	}
}
