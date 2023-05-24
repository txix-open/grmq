package grmq

import (
	"context"
	"sync"

	publisher2 "github.com/integration-system/grmq/publisher"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	ch                  *amqp.Channel
	publisher           *publisher2.Publisher
	unexpectedErr       chan *amqp.Error
	flow                chan bool
	observer            Observer
	setConfirmationMode *sync.Once
}

func NewPublisher(publisher *publisher2.Publisher, ch *amqp.Channel, observer Observer) *Publisher {
	return &Publisher{
		ch:                  ch,
		publisher:           publisher,
		unexpectedErr:       make(chan *amqp.Error, 1),
		flow:                make(chan bool, 1),
		observer:            observer,
		setConfirmationMode: &sync.Once{},
	}
}

func (p *Publisher) Publish(ctx context.Context, exchange string, routingKey string, msg *amqp.Publishing) error {
	err := p.ch.PublishWithContext(ctx, exchange, routingKey, true, false, *msg)
	if err != nil {
		return errors.WithMessage(err, "publish")
	}
	return nil
}

func (p *Publisher) PublishWithConfirmation(ctx context.Context, exchange string, routingKey string, msg *amqp.Publishing) error {
	var err error
	p.setConfirmationMode.Do(func() {
		err = p.ch.Confirm(false)
	})
	if err != nil {
		return errors.WithMessage(err, "run channel in confirmation mode")
	}

	confirmation, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, true, false, *msg)
	if err != nil {
		return errors.WithMessage(err, "publishWithDeferredConfirmWithContext")
	}
	acked := confirmation.Wait()
	if !acked {
		return errors.Errorf("message with tag %d was not acked by broker", confirmation.DeliveryTag)
	}

	return nil
}

func (p *Publisher) Run() error {
	p.unexpectedErr = p.ch.NotifyClose(p.unexpectedErr)
	p.flow = p.ch.NotifyFlow(p.flow)
	go p.runWatcher()

	p.publisher.SetRoundTripper(p)
	return nil
}

func (p *Publisher) runWatcher() {
	for {
		select {
		case flow, isOpen := <-p.flow:
			if !isOpen {
				return
			}
			p.observer.PublishingFlow(p.publisher, flow)
		case err, isOpen := <-p.unexpectedErr:
			if !isOpen {
				return
			}
			if err != nil {
				p.observer.PublisherError(p.publisher, err)
				return
			}
		}
	}
}

func (p *Publisher) Close() error {
	err := p.ch.Close()
	return errors.WithMessage(err, "channel close")
}
