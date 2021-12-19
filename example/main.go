package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/integration-system/grmq"
	"github.com/integration-system/grmq/consumer"
	"github.com/integration-system/grmq/publisher"
	"github.com/integration-system/grmq/topology"
	"github.com/rabbitmq/amqp091-go"
)

type LogObserver struct {
	grmq.NoopObserver
}

func (o LogObserver) ClientError(err error) {
	log.Printf("rmq client error: %v", err)
}

func (o LogObserver) ConsumerError(consumer consumer.Consumer, err error) {
	log.Printf("unexpected consumer error (queueu=%s): %v", consumer.Queue, err)
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
