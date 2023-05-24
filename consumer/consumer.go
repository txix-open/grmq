package consumer

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"

	"github.com/integration-system/grmq/retry"
)

type Handler interface {
	Handle(ctx context.Context, delivery *Delivery)
}

type HandlerFunc func(ctx context.Context, delivery *Delivery)

func (f HandlerFunc) Handle(ctx context.Context, delivery *Delivery) {
	f(ctx, delivery)
}

type Middleware func(next Handler) Handler

type Consumer struct {
	Queue         string
	Name          string
	Concurrency   int
	PrefetchCount int
	Middlewares   []Middleware
	RetryPolicy   *retry.Policy

	handler Handler
}

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

func (c *Consumer) Handler() Handler {
	return c.handler
}
