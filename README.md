# GRMQ
## Go Rabbit MQ
What are the typical use-cases of RabbitMQ broker ?
* We create a durable topology (exchanges, queues, binding).
* Begin queue consuming (commonly in several goroutines with prefetch count) and use [DLQ](https://www.rabbitmq.com/dlx.html) to avoid poison messages.
* Also, we expect that if something happens with connection, we can reestablish it and continue our work transparently.
* We want graceful shutdown to reduce probability of message duplication.

All of those commonly used cases are implemented in the package.

High abstraction wrapper for [amqp091-go](https://github.com/rabbitmq/amqp091-go). Inspired by http package and [cony](https://github.com/assembla/cony)

## Features
* re-connection support
* graceful shutdown support
* flexible context.Context based api
* middlewares for publishers and consumers
* DLQ declaration out of the box

## Complete Example
```go

```

## State and road map
* the package is not tested well and hasn't been used in production yet
* API should be stabilized
* more tests need to be implemented
* add supporting for publishing confirmation to achieve more reliable publishing
* there is a local case when we need a batch of deliveries
