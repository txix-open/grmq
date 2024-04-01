package grmq

import (
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/txix-open/grmq/topology"
)

type Declarator struct {
	cfg topology.Declarations
	ch  *amqp.Channel
}

func NewDeclarator(cfg topology.Declarations, ch *amqp.Channel) *Declarator {
	return &Declarator{
		cfg: topology.Compile(cfg),
		ch:  ch,
	}
}

func (c *Declarator) Run() error {
	for _, exchange := range c.cfg.Exchanges {
		err := c.ch.ExchangeDeclare(exchange.Name, exchange.Type, true, false, false, false, exchange.Args)
		if err != nil {
			return errors.WithMessagef(err, "declare exchange '%s'", exchange.Name)
		}
	}

	for _, queue := range c.cfg.Queues {
		_, err := c.ch.QueueDeclare(queue.Name, queue.Durable, queue.AutoDelete, false, false, queue.Args)
		if err != nil {
			return errors.WithMessagef(err, "declare queue '%s'", queue.Name)
		}
	}

	for _, binding := range c.cfg.Bindings {
		err := c.ch.QueueBind(binding.QueueName, binding.RoutingKey, binding.ExchangeName, false, binding.Args)
		if err != nil {
			return errors.WithMessagef(err, "declare binding for queue '%s' to exchange '%s'", binding.QueueName, binding.ExchangeName)
		}
	}

	return nil
}

func (c *Declarator) Close() error {
	err := c.ch.Close()
	return errors.WithMessage(err, "channel close")
}
