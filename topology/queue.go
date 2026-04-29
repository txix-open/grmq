package topology

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/txix-open/grmq/retry"
)

// QueueOption is a function that configures a Queue instance.
type QueueOption func(q *Queue)

// Queue represents a RabbitMQ queue configuration.
type Queue struct {
	Name        string
	DLQ         bool
	Durable     bool
	AutoDelete  bool
	RetryPolicy *retry.Policy
	Args        amqp.Table
}

// NewQueue creates a new queue with the specified name and optional configuration.
// By default, queues are created as durable.
func NewQueue(name string, opts ...QueueOption) *Queue {
	q := &Queue{
		Name:    name,
		Durable: true, // default value
		Args:    map[string]any{},
	}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// WithDLQ configures whether the queue is a dead-letter queue.
func WithDLQ(value bool) QueueOption {
	return func(q *Queue) {
		q.DLQ = value
	}
}

// WithDurable configures whether the queue survives broker restarts.
func WithDurable(value bool) QueueOption {
	return func(q *Queue) {
		q.Durable = value
	}
}

// WithAutoDelete configures whether the queue is deleted when no longer in use.
func WithAutoDelete(value bool) QueueOption {
	return func(q *Queue) {
		q.AutoDelete = value
	}
}

// WithQueueArg sets a custom argument for the queue.
func WithQueueArg(key string, value any) QueueOption {
	return func(q *Queue) {
		q.Args[key] = value
	}
}

// WithRetryPolicy configures the retry policy for the queue.
func WithRetryPolicy(policy retry.Policy) QueueOption {
	return func(q *Queue) {
		q.RetryPolicy = &policy
	}
}
