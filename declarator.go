package grmq

import (
	"fmt"

	"github.com/integration-system/grmq/topology"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	DLXName   = "default-dead-letter"
	DLQSuffix = "DLQ"
)

type Declarator struct {
	cfg topology.Declarations
	ch  *amqp.Channel
}

func NewDeclarator(cfg topology.Declarations, ch *amqp.Channel) *Declarator {
	extraQueues := make([]*topology.Queue, 0)
	extraExchanges := make([]*topology.Exchange, 0)
	extraBindings := make([]*topology.Binding, 0)
	for _, queue := range cfg.Queues {
		if !queue.DLQ {
			continue
		}

		dlx := topology.NewDirectExchange(DLXName)
		extraExchanges = append(extraExchanges, dlx)

		queue.Args["x-dead-letter-exchange"] = dlx.Name

		dlqName := fmt.Sprintf("%s.%s", queue.Name, DLQSuffix)
		dlq := topology.NewQueue(dlqName)
		extraQueues = append(extraQueues, dlq)

		binding := topology.NewBinding(dlx.Name, dlqName, queue.Name)
		extraBindings = append(extraBindings, binding)
	}
	cfg.Queues = append(cfg.Queues, extraQueues...)
	cfg.Exchanges = append(cfg.Exchanges, extraExchanges...)
	cfg.Bindings = append(cfg.Bindings, extraBindings...)
	return &Declarator{
		cfg: cfg,
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
		_, err := c.ch.QueueDeclare(queue.Name, true, false, false, false, queue.Args)
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
