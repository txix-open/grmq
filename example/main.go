package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/txix-open/grmq"
	"github.com/txix-open/grmq/consumer"
	"github.com/txix-open/grmq/publisher"
	"github.com/txix-open/grmq/retry"
	"github.com/txix-open/grmq/topology"
)

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

func amqpUrl() string {
	host := envOrDefault("RMQ_HOST", "127.0.0.1")
	user := envOrDefault("RMQ_USER", "guest")
	pass := envOrDefault("RMQ_PASS", "guest")

	amqpUrl := fmt.Sprintf("amqp://%s:%s@%s:5672/", user, pass, host)
	return amqpUrl
}

func envOrDefault(name string, defValue string) string {
	value := os.Getenv(name)
	if value != "" {
		return value
	}
	return defValue
}
