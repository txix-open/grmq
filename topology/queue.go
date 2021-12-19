package topology

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueOption func(q *Queue)

type Queue struct {
	Name string
	DLQ  bool
	Args amqp.Table
}

func NewQueue(name string, opts ...QueueOption) *Queue {
	q := &Queue{
		Name: name,
		Args: map[string]interface{}{},
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

func WithQueueArg(key string, value interface{}) QueueOption {
	return func(q *Queue) {
		q.Args[key] = value
	}
}
