package publisher

import (
	"context"
	"sync/atomic"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrPublisherIsNotInitialized = errors.New("publisher is not initialized")
)

type Middleware func(next RoundTripper) RoundTripper

type RoundTripper interface {
	Publish(ctx context.Context, exchange string, routingKey string, msg *amqp.Publishing) error
}

type RoundTripperFunc func(ctx context.Context, exchange string, routingKey string, msg *amqp.Publishing) error

func (f RoundTripperFunc) Publish(ctx context.Context, exchange string, routingKey string, msg *amqp.Publishing) error {
	return f(ctx, exchange, routingKey, msg)
}

type Publisher struct {
	Exchange    string
	RoutingKey  string
	Middlewares []Middleware

	roundTripper     *atomic.Value
	rootRoundTripper *atomic.Value
}

func New(exchange string, routingKey string, opts ...Option) *Publisher {
	p := &Publisher{
		Exchange:         exchange,
		RoutingKey:       routingKey,
		roundTripper:     &atomic.Value{},
		rootRoundTripper: &atomic.Value{},
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *Publisher) Publish(ctx context.Context, msg *amqp.Publishing) error {
	roundTripper, err := p.getRoundTripper()
	if err != nil {
		return err
	}
	return roundTripper.Publish(ctx, p.Exchange, p.RoutingKey, msg)
}

func (p *Publisher) PublishTo(ctx context.Context, exchange string, routingKey string, msg *amqp.Publishing) error {
	roundTripper, err := p.getRoundTripper()
	if err != nil {
		return err
	}
	return roundTripper.Publish(ctx, exchange, routingKey, msg)
}

func (p *Publisher) SetRoundTripper(rootRoundTripper RoundTripper) {
	p.rootRoundTripper.Store(rootRoundTripper)
	var roundTripper RoundTripper = RoundTripperFunc(p.publish)
	for i := len(p.Middlewares) - 1; i >= 0; i-- {
		roundTripper = p.Middlewares[i](roundTripper)
	}

	p.roundTripper.Store(roundTripper)
}

func (p *Publisher) getRoundTripper() (RoundTripper, error) {
	roundTripper, ok := p.roundTripper.Load().(RoundTripper)
	if !ok {
		return nil, ErrPublisherIsNotInitialized
	}
	return roundTripper, nil
}

func (p *Publisher) publish(ctx context.Context, exchange string, routingKey string, msg *amqp.Publishing) error {
	roundTripper, ok := p.rootRoundTripper.Load().(RoundTripper)
	if !ok {
		return ErrPublisherIsNotInitialized
	}
	return roundTripper.Publish(ctx, exchange, routingKey, msg)
}
