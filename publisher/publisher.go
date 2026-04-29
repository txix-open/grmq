// Package publisher provides a high-level publisher for sending messages to RabbitMQ
// with support for middleware and confirmation modes.
package publisher

import (
	"context"
	"sync/atomic"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	// ErrPublisherIsNotInitialized is returned when the publisher has not been initialized.
	ErrPublisherIsNotInitialized = errors.New("publisher is not initialized")
)

// Middleware is a function that wraps a RoundTripper, enabling request processing chains.
type Middleware func(next RoundTripper) RoundTripper

// RoundTripper defines the interface for publishing messages to RabbitMQ.
type RoundTripper interface {
	Publish(ctx context.Context, exchange string, routingKey string, msg *amqp.Publishing) error
}

// RoundTripperFunc is an adapter that allows regular functions to be used as RoundTripper.
type RoundTripperFunc func(ctx context.Context, exchange string, routingKey string, msg *amqp.Publishing) error

// Publish implements the RoundTripper interface.
func (f RoundTripperFunc) Publish(ctx context.Context, exchange string, routingKey string, msg *amqp.Publishing) error {
	return f(ctx, exchange, routingKey, msg)
}

// Publisher represents a message publisher with configurable exchange, routing key, and middleware support.
type Publisher struct {
	Exchange    string
	RoutingKey  string
	Middlewares []Middleware

	roundTripper     *atomic.Value
	rootRoundTripper *atomic.Value
}

// New creates a new Publisher with the specified exchange and routing key.
// Optional configuration can be provided using Option functions.
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

// Publish sends a message to the configured exchange and routing key.
// Returns an error if the publisher is not initialized or the publish operation fails.
func (p *Publisher) Publish(ctx context.Context, msg *amqp.Publishing) error {
	roundTripper, err := p.getRoundTripper()
	if err != nil {
		return err
	}
	return roundTripper.Publish(ctx, p.Exchange, p.RoutingKey, msg)
}

// PublishTo sends a message to the specified exchange and routing key,
// overriding the publisher's default configuration.
func (p *Publisher) PublishTo(ctx context.Context, exchange string, routingKey string, msg *amqp.Publishing) error {
	roundTripper, err := p.getRoundTripper()
	if err != nil {
		return err
	}
	return roundTripper.Publish(ctx, exchange, routingKey, msg)
}

// SetRoundTripper configures the round tripper with middleware chain.
// This method must be called before the publisher can be used.
func (p *Publisher) SetRoundTripper(rootRoundTripper RoundTripper) {
	p.rootRoundTripper.Store(rootRoundTripper)
	var roundTripper RoundTripper = RoundTripperFunc(p.publish)
	for i := len(p.Middlewares) - 1; i >= 0; i-- {
		roundTripper = p.Middlewares[i](roundTripper)
	}

	p.roundTripper.Store(roundTripper)
}

// getRoundTripper returns the configured round tripper or an error if not initialized.
func (p *Publisher) getRoundTripper() (RoundTripper, error) {
	roundTripper, ok := p.roundTripper.Load().(RoundTripper)
	if !ok {
		return nil, ErrPublisherIsNotInitialized
	}
	return roundTripper, nil
}

// publish is the underlying publish function that uses the root round tripper.
func (p *Publisher) publish(ctx context.Context, exchange string, routingKey string, msg *amqp.Publishing) error {
	roundTripper, ok := p.rootRoundTripper.Load().(RoundTripper)
	if !ok {
		return ErrPublisherIsNotInitialized
	}
	return roundTripper.Publish(ctx, exchange, routingKey, msg)
}
