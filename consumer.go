package grmq

import (
	"context"
	"sync"

	"github.com/integration-system/grmq/consumer"
	"github.com/integration-system/grmq/retry"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	cfg           consumer.Consumer
	ch            *amqp.Channel
	retryer       *retry.Retryer
	deliveryWg    *sync.WaitGroup
	workersWg     *sync.WaitGroup
	workersStop   chan struct{}
	closeConsumer chan struct{}
	observer      Observer

	unexpectedErr chan *amqp.Error
	deliveries    <-chan amqp.Delivery
}

func NewConsumer(cfg consumer.Consumer, ch *amqp.Channel, retryPub *Publisher, observer Observer) *Consumer {
	var retryer *retry.Retryer
	if cfg.RetryPolicy != nil {
		retryer = retry.NewRetryer(cfg.Queue, *cfg.RetryPolicy, retryPub)
	}
	return &Consumer{
		cfg:           cfg,
		ch:            ch,
		retryer:       retryer,
		workersWg:     &sync.WaitGroup{},
		deliveryWg:    &sync.WaitGroup{},
		unexpectedErr: make(chan *amqp.Error, 1),
		workersStop:   make(chan struct{}),
		closeConsumer: make(chan struct{}),
		observer:      observer,
	}
}

func (c *Consumer) Run() error {
	c.unexpectedErr = c.ch.NotifyClose(c.unexpectedErr)
	go c.runUnexpectedErrorListener()

	if c.cfg.PrefetchCount > 0 {
		err := c.ch.Qos(c.cfg.PrefetchCount, 0, false)
		if err != nil {
			return errors.WithMessage(err, "set qos")
		}
	}

	deliveries, err := c.ch.Consume(c.cfg.Queue, c.cfg.Name, false, false, false, false, nil)
	if err != nil {
		return errors.WithMessage(err, "begin consume")
	}
	c.deliveries = deliveries

	for i := 0; i < c.cfg.Concurrency; i++ {
		c.workersWg.Add(1)
		go c.runWorker()
	}

	return nil
}

func (c *Consumer) runWorker() {
	defer c.workersWg.Done()

	for {
		select {
		case delivery, isOpen := <-c.deliveries:
			if !isOpen { //normal close
				return
			}
			c.deliveryWg.Add(1)
			d := consumer.NewDelivery(c.deliveryWg, &delivery, c.retryer)
			c.cfg.Handler().Handle(context.Background(), d)
		case <-c.workersStop:
			return //unexpected error occurred
		}
	}
}

func (c *Consumer) runUnexpectedErrorListener() {
	for {
		select {
		case <-c.closeConsumer: //close consumer
			return
		case err, isOpen := <-c.unexpectedErr:
			if !isOpen { //normal close
				return
			}
			if err != nil {
				c.observer.ConsumerError(c.cfg, err)
				close(c.workersStop) //stop all workers because of unexpected error
				return
			}
		}
	}
}

func (c *Consumer) Close() error {
	err := c.ch.Cancel(c.cfg.Name, false)
	if err != nil {
		return errors.WithMessage(err, "channel cancel")
	}

	close(c.closeConsumer)
	if c.cfg.Closer != nil {
		c.cfg.Closer.Close()
	}
	c.workersWg.Wait()
	c.deliveryWg.Wait()

	err = c.ch.Close()
	return errors.WithMessage(err, "channel close")
}
