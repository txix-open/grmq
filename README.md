# GRMQ
## Go Rabbit MQ
![Build and test](https://github.com/integration-system/grmq/actions/workflows/main.yml/badge.svg)
[![codecov](https://codecov.io/gh/integration-system/grmq/branch/main/graph/badge.svg?token=JMTTJ5O6WB)](https://codecov.io/gh/integration-system/grmq)
[![Go Report Card](https://goreportcard.com/badge/github.com/integration-system/grmq)](https://goreportcard.com/report/github.com/integration-system/grmq)

What are the typical use-cases for RabbitMQ broker ?
* We create a durable topology (exchanges, queues, bindings).
* Begin queue consuming (commonly in several goroutines with prefetch count) and use [DLQ](https://www.rabbitmq.com/dlx.html) to avoid poison messages.
* Also, we expect that if something happens with connection, we can reestablish it and continue our work transparently.
* Graceful shutdown to reduce probability of message duplication.

All of those commonly used cases are implemented in the package.

High abstraction wrapper for [amqp091-go](https://github.com/rabbitmq/amqp091-go). Inspired by http package and [cony](https://github.com/assembla/cony)

## Features
* re-connection support
* graceful shutdown support
* flexible `context.Context` based api
* middlewares for publishers and consumers
* DLQ declaration out of the box

## Complete Example
```go
type LogObserver struct {
	grmq.NoopObserver
}

func (o LogObserver) ClientError(err error) {
	log.Printf("rmq client error: %v", err)
}

func (o LogObserver) ConsumerError(consumer consumer.Consumer, err error) {
	log.Printf("unexpected consumer error (queue=%s): %v", consumer.Queue, err)
}

func main() {
	url := amqpUrl()

	pub := publisher.New(
		"exchange",
		"test",
		publisher.WithMiddlewares(publisher.PersistentMode()),
	)

	handler := consumer.HandlerFunc(func(ctx context.Context, delivery *consumer.Delivery) {
		log.Printf("message body: %s", delivery.Source().Body)
		err := delivery.Ack()
		if err != nil {
			panic(err)
		}
	})
	consumer := consumer.New(
		handler,
		"queue",
		consumer.WithConcurrency(32),   //default 1
		consumer.WithPrefetchCount(32), //default 1
	)

	cli := grmq.New(
		url,
		grmq.WithPublishers(pub),
		grmq.WithConsumers(consumer),
		grmq.WithTopologyBuilding(
			topology.WithQueue("queue", topology.WithDLQ(true)),
			topology.WithDirectExchange("exchange"),
			topology.WithBinding("exchange", "queue", "test"),
		),
		grmq.WithReconnectTimeout(3*time.Second), //default 1s
		grmq.WithObserver(LogObserver{}),
	)
	//it tries to connect
	//declare topology
	//init publishers and consumers
	//returns first occurred error or nil
	err := cli.Run(context.Background())
	if err != nil {
		panic(err)
	}

	err = pub.Publish(context.Background(), &amqp091.Publishing{Body: []byte("hello world")})
	if err != nil {
		panic(err)
	}

	time.Sleep(1 * time.Second)

	cli.Shutdown()
}
```

## State and road map
* the package is used in production (reconnection works perfect)
* more tests need to be implemented
* add supporting for publishing confirmation to achieve more reliable publishing
