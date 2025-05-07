package grmq_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/txix-open/grmq"
	"github.com/txix-open/grmq/consumer"
	"github.com/txix-open/grmq/publisher"
)

func TestConsumer_Run(t *testing.T) {
	t.Parallel()
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

	consumer := grmq.NewConsumer(consumerCfg, ch, nil, grmq.NoopObserver{})
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
	t.Parallel()
	require := require.New(t)

	url := amqpUrl(t)
	ch := amqpChannel(t, url)
	declareQueue(t, ch, "test")

	publishMessages(t, ch, "test", 1)

	await := make(chan struct{})
	value := atomic.Int32{}
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

	consumer := grmq.NewConsumer(consumerCfg, ch, nil, grmq.NoopObserver{})
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
	t.Parallel()
	require := require.New(t)

	url := amqpUrl(t)
	ch := amqpChannel(t, url)
	declareQueue(t, ch, "test")

	publishMessages(t, ch, "test", 2)

	value := atomic.Int32{}
	handler := consumer.HandlerFunc(func(ctx context.Context, delivery *consumer.Delivery) {
		time.Sleep(3 * time.Second)
		value.Add(1)
		err := delivery.Ack()
		require.NoError(err)
	})
	consumerCfg := consumer.New(handler, "test", consumer.WithConcurrency(2), consumer.WithPrefetchCount(2))

	consumer := grmq.NewConsumer(consumerCfg, ch, nil, grmq.NoopObserver{})
	err := consumer.Run()
	require.NoError(err)

	time.Sleep(4 * time.Second) //4 < 6
	require.EqualValues(2, value.Load())

	err = consumer.Close()
	require.NoError(err)
}

func TestConsumer_ConsumerError(t *testing.T) {
	t.Parallel()
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

	observer := NewObserverCounter(t)
	consumer := grmq.NewConsumer(consumerCfg, ch, nil, observer)
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
	t.Parallel()
	require := require.New(t)

	url := amqpUrl(t)
	ch := amqpChannel(t, url)
	declareQueue(t, ch, "test")

	publishMessages(t, ch, "test", 1)

	value := atomic.Int32{}
	handler := consumer.HandlerFunc(func(ctx context.Context, delivery *consumer.Delivery) {
		go func() {
			time.Sleep(500 * time.Millisecond)
			err := delivery.Ack()
			require.NoError(err)
			value.Add(1)
		}()
	})
	consumerCfg := consumer.New(handler, "test")

	consumer := grmq.NewConsumer(consumerCfg, ch, nil, grmq.NoopObserver{})
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

	value := atomic.Int32{}
	handler := consumer.HandlerFunc(func(ctx context.Context, delivery *consumer.Delivery) {
		time.Sleep(50 * time.Millisecond)
		err := delivery.Ack()
		require.NoError(err)
		value.Add(1)
	})
	consumerCfg := consumer.New(handler, "test", consumer.WithPrefetchCount(5), consumer.WithConcurrency(5))

	observer := NewObserverCounter(t)
	consumer := grmq.NewConsumer(consumerCfg, ch, nil, observer)
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
	test                *testing.T
	clientReady         *atomic.Int32
	clientError         *atomic.Int32
	consumerError       *atomic.Int32
	shutdownStarted     *atomic.Int32
	shutdownDone        *atomic.Int32
	publisherError      *atomic.Int32
	publisherFlow       *atomic.Int32
	connectionBlocked   *atomic.Int32
	connectionUnblocked *atomic.Int32
}

func NewObserverCounter(test *testing.T) *ObserverCounter {
	return &ObserverCounter{
		test:                test,
		clientReady:         &atomic.Int32{},
		clientError:         &atomic.Int32{},
		consumerError:       &atomic.Int32{},
		shutdownStarted:     &atomic.Int32{},
		shutdownDone:        &atomic.Int32{},
		publisherError:      &atomic.Int32{},
		publisherFlow:       &atomic.Int32{},
		connectionBlocked:   &atomic.Int32{},
		connectionUnblocked: &atomic.Int32{},
	}
}

func (o *ObserverCounter) ClientReady() {
	o.test.Log("client ready")
	o.clientReady.Add(1)
}

func (o *ObserverCounter) ClientError(err error) {
	o.test.Log("client error", err)
	o.clientError.Add(1)
}

func (o *ObserverCounter) ConsumerError(consumer consumer.Consumer, err error) {
	o.test.Log("consumer error", err)
	o.consumerError.Add(1)
}

func (o *ObserverCounter) ShutdownStarted() {
	o.test.Log("shutdown started")
	o.shutdownStarted.Add(1)
}

func (o *ObserverCounter) ShutdownDone() {
	o.test.Log("shutdown done")
	o.shutdownDone.Add(1)
}

func (o *ObserverCounter) PublisherError(publisher *publisher.Publisher, err error) {
	o.test.Log("publisher error", err)
	o.publisherError.Add(1)
}

func (o *ObserverCounter) PublishingFlow(publisher *publisher.Publisher, flow bool) {
	o.test.Log("publisher flow", flow)
	o.publisherFlow.Add(1)
}

func (o *ObserverCounter) ConnectionBlocked(reason string) {
	o.test.Log("connection blocked", reason)
	o.connectionBlocked.Add(1)
}

func (o *ObserverCounter) ConnectionUnblocked() {
	o.test.Log("connection unblocked")
	o.connectionUnblocked.Add(1)
}
