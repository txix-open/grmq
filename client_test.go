package grmq_test

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"github.com/txix-open/grmq"
	"github.com/txix-open/grmq/consumer"
	"github.com/txix-open/grmq/publisher"
	"github.com/txix-open/grmq/retry"
	"github.com/txix-open/grmq/topology"
)

func TestClient(t *testing.T) {
	t.Parallel()
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
	observer := NewObserverCounter(t)
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
	t.Parallel()
	require := require.New(t)

	url := amqpUrl(t)
	declarations := topology.Declarations{
		Exchanges: []*topology.Exchange{{
			Name: "test",
			Type: "invalid_type",
			Args: nil,
		}},
	}
	observer := NewObserverCounter(t)
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

func TestInvalidCred(t *testing.T) {
	require := require.New(t)

	amqpUrl := amqpUrl(t)
	u, err := url.Parse(amqpUrl)
	require.NoError(err)
	u.User = url.UserPassword(u.User.Username(), "invalid_pass")

	cli := grmq.New(u.String())
	err = cli.Run(context.Background())
	require.Error(err)
}

func TestDLQ(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	url := amqpUrl(t)
	pub := publisher.New("exchange", "key")
	await := make(chan struct{})
	value := atomic.Int32{}
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
			topology.WithDirectExchange("exchange"),
			topology.WithBinding("exchange", "queue", "key"),
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
	t.Parallel()
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

func TestRetries(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	url := amqpUrl(t)
	pub := publisher.New("", "queue")
	await := make(chan struct{})
	value := atomic.Int32{}
	retryPolicy := retry.NewPolicy(
		true,
		retry.WithDelay(500*time.Millisecond, 2),
		retry.WithDelay(1*time.Second, 1),
	)
	handler := consumer.HandlerFunc(func(ctx context.Context, delivery *consumer.Delivery) {
		value := value.Add(1)
		err := delivery.Retry()
		require.NoError(err)
		if value == 4 {
			await <- struct{}{}
		}
	})
	consumer := consumer.New(handler, "queue", consumer.WithRetryPolicy(retryPolicy))
	cli := grmq.New(url,
		grmq.WithPublishers(pub),
		grmq.WithConsumers(consumer),
		grmq.WithTopologyBuilding(
			topology.WithQueue("queue", topology.WithDLQ(true), topology.WithRetryPolicy(retryPolicy)),
		),
	)
	err := cli.Run(context.Background())
	require.NoError(err)

	err = pub.Publish(context.Background(), &amqp091.Publishing{})
	require.NoError(err)

	select {
	case <-await:
	case <-time.After(5 * time.Second):
		require.Fail("handler wasn't called")
	}

	cli.Shutdown()

	require.EqualValues(4, value.Load())
	require.EqualValues(1, queueSize(t, url, "queue.DLQ"))
}

func TestClient_Serve(t *testing.T) {
	t.Parallel()
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
	observer := NewObserverCounter(t)
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
	cli.Serve(context.Background())
	time.Sleep(500 * time.Millisecond)
	err := pub.Publish(context.Background(), &amqp091.Publishing{})
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
