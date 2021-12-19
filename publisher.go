package rmq

import (
	"context"

	publisher2 "github.com/integration-system/grmq/publisher"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	ch        *amqp.Channel
	publisher *publisher2.Publisher
}

func NewPublisher(publisher *publisher2.Publisher, ch *amqp.Channel) *Publisher {
	return &Publisher{
		ch:        ch,
		publisher: publisher,
	}
}

func (p *Publisher) Publish(_ context.Context, exchange string, routingKey string, msg *amqp.Publishing) error {
	err := p.ch.Publish(exchange, routingKey, true, false, *msg)
	if err != nil {
		return errors.WithMessagef(err, "publish")
	}
	return nil
}

func (p *Publisher) Run() error {
	p.publisher.SetRoundTripper(p)
	return nil
}

func (p *Publisher) Close() error {
	err := p.ch.Close()
	return errors.WithMessagef(err, "channel close")
}
