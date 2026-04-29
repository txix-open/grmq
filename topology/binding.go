package topology

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// Binding represents a connection between an exchange and a queue.
type Binding struct {
	ExchangeName string
	QueueName    string
	RoutingKey   string
	Args         amqp.Table
}

// NewBinding creates a new binding between an exchange and a queue.
func NewBinding(exchangeName string, queueName string, routingKey string) *Binding {
	return &Binding{
		ExchangeName: exchangeName,
		QueueName:    queueName,
		RoutingKey:   routingKey,
		Args:         map[string]any{},
	}
}
