package grmq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/integration-system/grmq"
	"github.com/integration-system/grmq/consumer"
	"github.com/integration-system/grmq/publisher"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestConsumer_Run(t *testing.T) {
	require := require.New(t)
	url := amqpUrl(t)
	ch := amqpChannel(t, url)
	declareQueue(t, ch, "test")

	publishMessages(t, ch, "test", 1)

	await := make(chan struct{})
	handler := consumer.HandlerFunc(func(ctx context.Context, delivery *consumer.Delivery) {
		err := delivery.Ack()
		require.NoError(err)
		await <- struct{}{}
	})
	consumerCfg := consumer.New(handler, "test")

	consumer := grmq.NewConsumer(consumerCfg, ch, grmq.NoopObserver{})
	err := consumer.Run()
	require.NoError(err)

	select {
	case <-await:
	case <-time.After(1 * time.Second):
		require.Fail("handler wasn't called")
	}

	err = consumer.Close()
	require.NoError(err)

	require.EqualValues(0, queueSize(t, url, "test"))
}

func TestConsumer_RunWithMiddlewares(t *testing.T) {
	require := require.New(t)
	url := amqpUrl(t)
	ch := amqpChannel(t, url)
	declareQueue(t, ch, "test")

	publishMessages(t, ch, "test", 1)

	await := make(chan struct{})
	value := atomic.NewInt32(0)
	handler := consumer.HandlerFunc(func(ctx context.Context, delivery *consumer.Delivery) {
		err := delivery.Ack()
		require.NoError(err)
		await <- struct{}{}
	})
	consumerCfg := consumer.New(handler, "test", consumer.WithMiddlewares(func(next consumer.Handler) consumer.Handler {
		return consumer.HandlerFunc(func(ctx context.Context, delivery *consumer.Delivery) {
			value.Add(1)
			next.Handle(ctx, delivery)
		})
	}))

	consumer := grmq.NewConsumer(consumerCfg, ch, grmq.NoopObserver{})
	err := consumer.Run()
	require.NoError(err)

	select {
	case <-await:
	case <-time.After(1 * time.Second):
		require.Fail("handler wasn't called")
	}

	require.EqualValues(1, value.Load())

	err = consumer.Close()
	require.NoError(err)
}

func TestConsumer_RunWithConcurrency(t *testing.T) {
	require := require.New(t)
	url := amqpUrl(t)
	ch := amqpChannel(t, url)
	declareQueue(t, ch, "test")

	publishMessages(t, ch, "test", 2)

	value := atomic.NewInt32(0)
	handler := consumer.HandlerFunc(func(ctx context.Context, delivery *consumer.Delivery) {
		time.Sleep(3 * time.Second)
		value.Add(1)
		err := delivery.Ack()
		require.NoError(err)
	})
	consumerCfg := consumer.New(handler, "test", consumer.WithConcurrency(2), consumer.WithPrefetchCount(2))

	consumer := grmq.NewConsumer(consumerCfg, ch, grmq.NoopObserver{})
	err := consumer.Run()
	require.NoError(err)

	time.Sleep(4 * time.Second) //4 < 6
	require.EqualValues(2, value.Load())

	err = consumer.Close()
	require.NoError(err)
}

func TestConsumer_ConsumerError(t *testing.T) {
	require := require.New(t)
	url := amqpUrl(t)
	ch := amqpChannel(t, url)
	declareQueue(t, ch, "test")

	publishMessages(t, ch, "test", 1)

	await := make(chan struct{})
	handler := consumer.HandlerFunc(func(ctx context.Context, delivery *consumer.Delivery) {
		err := delivery.Source().Ack(false)
		require.NoError(err)
		err = delivery.Ack() //ack twice, provoke error
		require.NoError(err)
		await <- struct{}{}
	})
	consumerCfg := consumer.New(handler, "test")

	observer := NewObserverCounter()
	consumer := grmq.NewConsumer(consumerCfg, ch, observer)
	err := consumer.Run()
	require.NoError(err)

	select {
	case <-await:
	case <-time.After(1 * time.Second):
		require.Fail("handler wasn't called")
	}

	time.Sleep(100 * time.Millisecond)

	err = consumer.Close()
	require.Error(err)

	require.EqualValues(1, observer.consumerError.Load())
}

func TestConsumer_AsyncHandler(t *testing.T) {
	require := require.New(t)
	url := amqpUrl(t)
	ch := amqpChannel(t, url)
	declareQueue(t, ch, "test")

	publishMessages(t, ch, "test", 1)

	value := atomic.NewInt32(0)
	handler := consumer.HandlerFunc(func(ctx context.Context, delivery *consumer.Delivery) {
		go func() {
			time.Sleep(500 * time.Millisecond)
			err := delivery.Ack()
			require.NoError(err)
			value.Add(1)
		}()
	})
	consumerCfg := consumer.New(handler, "test")

	consumer := grmq.NewConsumer(consumerCfg, ch, grmq.NoopObserver{})
	err := consumer.Run()
	require.NoError(err)

	err = consumer.Close()
	require.NoError(err)

	require.EqualValues(1, value.Load())
}

func TestConsumer_GracefulClose(t *testing.T) {
	require := require.New(t)
	url := amqpUrl(t)
	ch := amqpChannel(t, url)
	declareQueue(t, ch, "test")

	messagesCount := 10
	publishMessages(t, ch, "test", messagesCount)

	value := atomic.NewInt32(0)
	handler := consumer.HandlerFunc(func(ctx context.Context, delivery *consumer.Delivery) {
		time.Sleep(50 * time.Millisecond)
		err := delivery.Ack()
		require.NoError(err)
		value.Add(1)
	})
	consumerCfg := consumer.New(handler, "test", consumer.WithPrefetchCount(5), consumer.WithConcurrency(5))

	observer := NewObserverCounter()
	consumer := grmq.NewConsumer(consumerCfg, ch, observer)
	err := consumer.Run()
	require.NoError(err)

	time.Sleep(50 * time.Millisecond)

	err = consumer.Close()
	require.NoError(err)

	qz := queueSize(t, url, "test")
	t.Logf("queueSize = %d", qz)
	t.Logf("value = %d", value.Load())

	require.EqualValues(messagesCount, int32(qz)+value.Load())
	require.EqualValues(0, observer.consumerError.Load())
}

type ObserverCounter struct {
	clientReady     *atomic.Int32
	clientError     *atomic.Int32
	consumerError   *atomic.Int32
	shutdownStarted *atomic.Int32
	shutdownDone    *atomic.Int32
	publisherError  *atomic.Int32
	publisherFlow   *atomic.Int32
}

func NewObserverCounter() *ObserverCounter {
	return &ObserverCounter{
		clientReady:     atomic.NewInt32(0),
		clientError:     atomic.NewInt32(0),
		consumerError:   atomic.NewInt32(0),
		shutdownStarted: atomic.NewInt32(0),
		shutdownDone:    atomic.NewInt32(0),
		publisherError:  atomic.NewInt32(0),
		publisherFlow:   atomic.NewInt32(0),
	}
}

func (o *ObserverCounter) ClientReady() {
	o.clientReady.Add(1)
}

func (o *ObserverCounter) ClientError(err error) {
	o.clientError.Add(1)
}

func (o *ObserverCounter) ConsumerError(consumer consumer.Consumer, err error) {
	o.consumerError.Add(1)
}

func (o *ObserverCounter) ShutdownStarted() {
	o.shutdownStarted.Add(1)
}

func (o *ObserverCounter) ShutdownDone() {
	o.shutdownDone.Add(1)
}

func (o *ObserverCounter) PublisherError(publisher *publisher.Publisher, err error) {
	fmt.Println(err)
	o.publisherError.Add(1)
}

func (o *ObserverCounter) PublishingFlow(publisher *publisher.Publisher, flow bool) {
	o.publisherFlow.Add(1)
}
