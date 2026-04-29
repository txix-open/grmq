package consumer

import (
	"github.com/txix-open/grmq/retry"
)

// Option is a function that configures a Consumer instance.
type Option func(c *Consumer)

// WithName sets the consumer name.
func WithName(name string) Option {
	return func(c *Consumer) {
		c.Name = name
	}
}

// WithPrefetchCount sets the maximum number of unacknowledged messages.
func WithPrefetchCount(prefetchCount int) Option {
	return func(c *Consumer) {
		c.PrefetchCount = prefetchCount
	}
}

// WithConcurrency sets the number of concurrent workers processing messages.
func WithConcurrency(concurrency int) Option {
	return func(c *Consumer) {
		c.Concurrency = concurrency
	}
}

// WithMiddlewares sets the middleware chain for the consumer.
// Middlewares are executed in the order they are provided.
func WithMiddlewares(middlewares ...Middleware) Option {
	return func(c *Consumer) {
		c.Middlewares = middlewares
	}
}

// WithRetryPolicy configures the retry policy for failed message processing.
func WithRetryPolicy(policy retry.Policy) Option {
	return func(c *Consumer) {
		c.RetryPolicy = &policy
	}
}

// WithCloser sets the resource closer for the consumer.
func WithCloser(closer Closer) Option {
	return func(c *Consumer) {
		c.Closer = closer
	}
}
