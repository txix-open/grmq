package consumer

import (
	"github.com/integration-system/grmq/retry"
)

type Option func(c *Consumer)

func WithName(name string) Option {
	return func(c *Consumer) {
		c.Name = name
	}
}

func WithPrefetchCount(prefetchCount int) Option {
	return func(c *Consumer) {
		c.PrefetchCount = prefetchCount
	}
}

func WithConcurrency(concurrency int) Option {
	return func(c *Consumer) {
		c.Concurrency = concurrency
	}
}

func WithMiddlewares(middlewares ...Middleware) Option {
	return func(c *Consumer) {
		c.Middlewares = middlewares
	}
}

func WithRetryPolicy(policy retry.Policy) Option {
	return func(c *Consumer) {
		c.RetryPolicy = &policy
	}
}

func WithCloser(closer Closer) Option {
	return func(c *Consumer) {
		c.Closer = closer
	}
}
