package topology

import (
	"fmt"
)

const (
	DLXName   = "default-dead-letter"
	DLQSuffix = "DLQ"

	rabbitMqDlxArg           = "x-dead-letter-exchange"
	rabbitMqDlqRoutingKeyArg = "x-dead-letter-routing-key"
	rabbitMqMessageTtlHeader = "x-message-ttl"
)

func Compile(cfg Declarations) Declarations {
	extraQueues := make([]*Queue, 0)
	extraExchanges := make([]*Exchange, 0)
	extraBindings := make([]*Binding, 0)
	for _, queue := range cfg.Queues {
		if queue.DLQ || queue.RetryPolicy != nil {
			dlx := NewDirectExchange(DLXName)
			extraExchanges = append(extraExchanges, dlx)

			queue.Args[rabbitMqDlxArg] = dlx.Name
			queue.Args[rabbitMqDlqRoutingKeyArg] = queue.Name

			dlqName := fmt.Sprintf("%s.%s", queue.Name, DLQSuffix)
			dlq := NewQueue(dlqName)
			extraQueues = append(extraQueues, dlq)

			binding := NewBinding(dlx.Name, dlqName, queue.Name)
			extraBindings = append(extraBindings, binding)
		}

		if queue.RetryPolicy != nil {
			for _, retry := range queue.RetryPolicy.Retries {
				retryQueueName := retry.QueueName(queue.Name)
				retryQueue := NewQueue(
					retryQueueName,
					WithQueueArg(rabbitMqMessageTtlHeader, retry.Delay.Milliseconds()),
					WithQueueArg(rabbitMqDlxArg, DLXName),
					WithQueueArg(rabbitMqDlqRoutingKeyArg, retryQueueName),
				)
				extraQueues = append(extraQueues, retryQueue)

				binding := NewBinding(DLXName, queue.Name, retryQueueName)
				extraBindings = append(extraBindings, binding)
			}
		}
	}
	cfg.Queues = append(cfg.Queues, extraQueues...)
	cfg.Exchanges = append(cfg.Exchanges, extraExchanges...)
	cfg.Bindings = append(cfg.Bindings, extraBindings...)

	return cfg
}
