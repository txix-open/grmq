package topology

import (
	"github.com/integration-system/grmq/retry"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueOption func(q *Queue)

type Queue struct {
	Name        string
	DLQ         bool
	Durable     bool
	AutoDelete  bool
	RetryPolicy *retry.Policy
	Args        amqp.Table
}

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

func WithDLQ(value bool) QueueOption {
	return func(q *Queue) {
		q.DLQ = value
	}
}

func WithDurable(value bool) QueueOption {
	return func(q *Queue) {
		q.Durable = value
	}
}

func WithAutoDelete(value bool) QueueOption {
	return func(q *Queue) {
		q.AutoDelete = value
	}
}

func WithQueueArg(key string, value any) QueueOption {
	return func(q *Queue) {
		q.Args[key] = value
	}
}

func WithRetryPolicy(policy retry.Policy) QueueOption {
	return func(q *Queue) {
		q.RetryPolicy = &policy
	}
}
