# GRMQ
## Go Rabbit MQ
![Build and test](https://github.com/txix-open/grmq/actions/workflows/main.yml/badge.svg)
[![codecov](https://codecov.io/gh/txix-open/grmq/branch/main/graph/badge.svg?token=JMTTJ5O6WB)](https://codecov.io/gh/txix-open/grmq)
[![Go Report Card](https://goreportcard.com/badge/github.com/txix-open/grmq)](https://goreportcard.com/report/github.com/txix-open/grmq)

What are the typical use-cases for RabbitMQ broker ?
* We create a durable topology (exchanges, queues, bindings).
* Begin queue consuming (commonly in several goroutines with prefetch count) and use [DLQ](https://www.rabbitmq.com/dlx.html) to avoid poison messages.
* If we can't handle message at this time, we can retry a bit later (some external service is not available for instance)
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
* [flexible retries](#retries)

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

	simpleHandler := consumer.HandlerFunc(func(ctx context.Context, delivery *consumer.Delivery) {
		log.Printf("message body: %s, queue: %s", delivery.Source().Body, delivery.Source().RoutingKey)
		err := delivery.Ack()
		if err != nil {
			panic(err)
		}
	})
	simpleConsumer := consumer.New(
		simpleHandler,
		"queue",
		consumer.WithConcurrency(32),   //default 1
		consumer.WithPrefetchCount(32), //default 1
	)

	retryPolicy := retry.NewPolicy(
		true, //move to dlq after last failed try
		retry.WithDelay(500*time.Millisecond, 1),
		retry.WithDelay(1*time.Second, 1),
		retry.WithDelay(2*time.Second, 1),
	)
	retryHandler := consumer.HandlerFunc(func(ctx context.Context, delivery *consumer.Delivery) {
		log.Printf("message body: %s, queue: %s", delivery.Source().Body, delivery.Source().RoutingKey)
		err := delivery.Retry()
		if err != nil {
			panic(err)
		}
	})
	retryConsumer := consumer.New(
		retryHandler,
		"retryQueue",
		consumer.WithRetryPolicy(retryPolicy),
	)

	cli := grmq.New(
		url,
		grmq.WithPublishers(pub),
		grmq.WithConsumers(simpleConsumer, retryConsumer),
		grmq.WithTopologyBuilding(
			topology.WithQueue("queue", topology.WithDLQ(true)),
			//you MUST declare queue with the same retry policy
			topology.WithQueue("retryQueue", topology.WithRetryPolicy(retryPolicy)),
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
	//or you can use  cli.Serve(context.Background()), which is completely non-blocking
	err := cli.Run(context.Background())
	if err != nil {
		panic(err)
	}

	err = pub.Publish(context.Background(), &amqp091.Publishing{Body: []byte("hello world")})
	if err != nil {
		panic(err)
	}

	//you may use any publisher to send message to any exchange
	err = pub.PublishTo(context.Background(), "", "retryQueue", &amqp091.Publishing{Body: []byte("retry me")})
	if err != nil {
		panic(err)
	}

	time.Sleep(10 * time.Second)

	cli.Shutdown()
}
```

## Retries
This is quite fresh feature implemented in `1.4.0`.
Before using it you must know how it works under the hood.
It combines two mechanisms: [DLQ](https://www.rabbitmq.com/dlx.html) + [TTL](https://www.rabbitmq.com/ttl.html)

Lets say we use policy below for queue `test`
```go
retryPolicy := retry.NewPolicy(
	true,
	retry.WithDelay(500*time.Millisecond, 1),
	retry.WithDelay(1*time.Second, 1),
	retry.WithDelay(2*time.Second, 1), 
)
```
This configuration will create
* exchange with name `default-dead-letter`
* 4 extra queues
  * `test.DLQ`
  * `test.retry.500`
  * `test.retry.1000`
  * `test.retry.2000`
* each retry queue will have `x-message-ttl` property equal to its delay
* each retry queue will have DLX routing to the original queue `test`
* `consumer.Delivery.Retry()` will find a suitable queue by `grmq-retry-count` header(0 by default), increment `grmq-retry-count` header, directly publish with confirmation to the queue, manually acknowledge the delivery
* if there is no suitable retry option and `moveToDql` is `true`, it moves the message to `test.DLQ`
* otherwise, it performs ack

**Recommendation**: If you want to change retry policy for a queue, before doing it, ensure there is no messages in retry queues.

Don't forget to delete old retry queues. 

## State and road map
* the package is used in production (reconnection works perfect)
* more tests need to be implemented
* add `go doc`
* add supporting for publishing confirmation to achieve more reliable publishing
