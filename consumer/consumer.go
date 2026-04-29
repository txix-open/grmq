// Package consumer provides a high-level consumer for receiving messages from RabbitMQ
// with support for concurrency, middleware, and retry policies.
package consumer

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"

	"github.com/txix-open/grmq/retry"
)

// Closer defines an interface for closing resources.
type Closer interface {
	Close()
}

// Handler defines an interface for processing message deliveries.
type Handler interface {
	Handle(ctx context.Context, delivery *Delivery)
}

// HandlerFunc is an adapter that allows regular functions to be used as Handler.
type HandlerFunc func(ctx context.Context, delivery *Delivery)

// Handle implements the Handler interface.
func (f HandlerFunc) Handle(ctx context.Context, delivery *Delivery) {
	f(ctx, delivery)
}

// Middleware is a function that wraps a Handler, enabling request processing chains.
type Middleware func(next Handler) Handler

// Consumer represents a message consumer with configurable queue, concurrency, and middleware support.
type Consumer struct {
	Queue         string
	Name          string
	Concurrency   int
	PrefetchCount int
	Middlewares   []Middleware
	RetryPolicy   *retry.Policy
	Closer        Closer

	handler Handler
}

// New creates a new Consumer with the specified handler and queue.
// A unique name base on os.Args[0] is automatically generated if not provided.
// Optional configuration can be provided using Option functions.
func New(handler Handler, queue string, opts ...Option) Consumer {
	suffix := make([]byte, 8)
	_, err := rand.Read(suffix)
	if err != nil {
		panic(err)
	}
	name := fmt.Sprintf("%s_%x", os.Args[0], suffix)
	c := &Consumer{
		Queue:         queue,
		Name:          name,
		Concurrency:   1,
		PrefetchCount: 1,
	}
	for _, opt := range opts {
		opt(c)
	}

	for i := len(c.Middlewares) - 1; i >= 0; i-- {
		handler = c.Middlewares[i](handler)
	}
	c.handler = handler

	return *c
}

// Handler returns the configured message handler.
func (c *Consumer) Handler() Handler {
	return c.handler
}
