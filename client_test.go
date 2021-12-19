package grmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/integration-system/grmq"
	"github.com/integration-system/grmq/consumer"
	"github.com/integration-system/grmq/publisher"
	"github.com/integration-system/grmq/topology"
	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestClient(t *testing.T) {
	require := require.New(t)

	url := amqpUrl(t)
	pub := publisher.New("exchange", "test")
	await := make(chan struct{})
	handler := consumer.HandlerFunc(func(ctx context.Context, delivery *consumer.Delivery) {
		err := delivery.Ack()
		require.NoError(err)
		await <- struct{}{}
	})
	consumer := consumer.New(handler, "queue")
	observer := NewObserverCounter()
	cli := grmq.New(url,
		grmq.WithPublishers(pub),
		grmq.WithConsumers(consumer),
		grmq.WithObserver(observer),
		grmq.WithTopologyBuilding(
			topology.WithQueue("queue"),
			topology.WithDirectExchange("exchange"),
			topology.WithBinding("exchange", "queue", "test"),
		),
	)
	err := cli.Run(context.Background())
	require.NoError(err)

	err = pub.Publish(context.Background(), &amqp091.Publishing{})
	require.NoError(err)

	select {
	case <-await:
	case <-time.After(1 * time.Second):
		require.Fail("handler wasn't called")
	}

	cli.Shutdown()

	require.EqualValues(1, observer.clientReady.Load())
	require.EqualValues(0, observer.clientError.Load())
	require.EqualValues(0, observer.consumerError.Load())
	require.EqualValues(1, observer.shutdownStarted.Load())
	require.EqualValues(1, observer.shutdownDone.Load())
}

func TestClient_RunError(t *testing.T) {
	require := require.New(t)

	url := amqpUrl(t)
	declarations := topology.Declarations{
		Exchanges: []*topology.Exchange{{
			Name: "test",
			Type: "invalid_type",
			Args: nil,
		}},
	}
	observer := NewObserverCounter()
	cli := grmq.New(url, grmq.WithDeclarations(declarations), grmq.WithObserver(observer))
	err := cli.Run(context.Background())
	require.Error(err)

	cli.Shutdown()

	require.EqualValues(0, observer.clientReady.Load())
	require.GreaterOrEqual(int32(1), observer.clientError.Load())
	require.EqualValues(0, observer.consumerError.Load())
	require.EqualValues(1, observer.shutdownStarted.Load())
	require.EqualValues(1, observer.shutdownDone.Load())
}

func TestDLQ(t *testing.T) {
	require := require.New(t)

	url := amqpUrl(t)
	pub := publisher.New("", "queue")
	await := make(chan struct{})
	value := atomic.NewInt32(0)
	handler := consumer.HandlerFunc(func(ctx context.Context, delivery *consumer.Delivery) {
		if value.Add(1) == 1 {
			err := delivery.Nack(true)
			require.NoError(err)
			return
		}
		err := delivery.Nack(false)
		require.NoError(err)
		await <- struct{}{}
	})
	consumer := consumer.New(handler, "queue")
	cli := grmq.New(url,
		grmq.WithPublishers(pub),
		grmq.WithConsumers(consumer),
		grmq.WithTopologyBuilding(
			topology.WithQueue("queue", topology.WithDLQ(true)),
		),
	)
	err := cli.Run(context.Background())
	require.NoError(err)

	err = pub.Publish(context.Background(), &amqp091.Publishing{})
	require.NoError(err)

	select {
	case <-await:
	case <-time.After(1 * time.Second):
		require.Fail("handler wasn't called")
	}

	cli.Shutdown()

	require.EqualValues(2, value.Load())
	require.EqualValues(1, queueSize(t, url, "queue.DLQ"))
}

func TestPersistentMode(t *testing.T) {
	require := require.New(t)

	url := amqpUrl(t)
	pub := publisher.New("", "queue", publisher.WithMiddlewares(publisher.PersistentMode()))
	await := make(chan struct{})
	handler := consumer.HandlerFunc(func(ctx context.Context, delivery *consumer.Delivery) {
		require.EqualValues(amqp091.Persistent, delivery.Source().DeliveryMode)
		err := delivery.Ack()
		require.NoError(err)
		await <- struct{}{}
	})
	consumer := consumer.New(handler, "queue")
	cli := grmq.New(url,
		grmq.WithPublishers(pub),
		grmq.WithConsumers(consumer),
		grmq.WithTopologyBuilding(
			topology.WithQueue("queue"),
		),
	)
	err := cli.Run(context.Background())
	require.NoError(err)

	err = pub.Publish(context.Background(), &amqp091.Publishing{})
	require.NoError(err)

	select {
	case <-await:
	case <-time.After(1 * time.Second):
		require.Fail("handler wasn't called")
	}

	cli.Shutdown()
}
