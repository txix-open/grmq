package grmq

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	publisher2 "github.com/txix-open/grmq/publisher"
)

// Publisher wraps a publisher instance with connection management, confirmation support,
// and automatic reconnection on channel failures.
//
// Reconnection Behavior:
// The publisher automatically reconnects when a precondition failed error occurs
// (e.g., exchange or queue declaration mismatch). Reconnection creates a new channel,
// resets confirmation mode, and re-registers the round tripper.
//
// Note: Reconnection is attempted only once per error. If reconnection fails,
// the publisher stops processing and reports the error via the Observer.
type Publisher struct {
	conn                *amqp.Connection
	ch                  *amqp.Channel
	publisher           *publisher2.Publisher
	unexpectedErr       chan *amqp.Error
	flow                chan bool
	observer            Observer
	setConfirmationMode *sync.Once
}

// NewPublisher creates a new Publisher instance with the specified configuration.
func NewPublisher(publisher *publisher2.Publisher, ch *amqp.Channel, observer Observer, conn *amqp.Connection) *Publisher {
	return &Publisher{
		conn:                conn,
		ch:                  ch,
		publisher:           publisher,
		unexpectedErr:       make(chan *amqp.Error, 1),
		flow:                make(chan bool, 1),
		observer:            observer,
		setConfirmationMode: &sync.Once{},
	}
}

// Publish sends a message to the specified exchange and routing key without waiting for confirmation.
// Returns an error if the publish operation fails.
func (p *Publisher) Publish(ctx context.Context, exchange string, routingKey string, msg *amqp.Publishing) error {
	err := p.ch.PublishWithContext(ctx, exchange, routingKey, true, false, *msg)
	if err != nil {
		return errors.WithMessage(err, "publish")
	}
	return nil
}

// PublishWithConfirmation sends a message and waits for broker confirmation.
// Enables publisher confirms mode on first call. Returns an error if the message
// is not acknowledged by the broker or if the publish operation fails.
func (p *Publisher) PublishWithConfirmation(ctx context.Context, exchange string, routingKey string, msg *amqp.Publishing) error {
	var err error
	p.setConfirmationMode.Do(func() {
		err = p.ch.Confirm(false)
	})
	if err != nil {
		return errors.WithMessage(err, "run channel in confirmation mode")
	}

	confirmation, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, true, false, *msg)
	if err != nil {
		return errors.WithMessage(err, "publishWithDeferredConfirmWithContext")
	}
	acked := confirmation.Wait()
	if !acked {
		return errors.Errorf("message with tag %d was not acked by broker", confirmation.DeliveryTag)
	}

	return nil
}

// Run initializes the publisher's event watchers and registers it with the underlying publisher.
// Must be called before using the publisher.
//
// This method starts a background watcher that monitors for channel errors and flow control events.
// If a precondition failed error occurs (e.g., due to topology changes), the watcher triggers
// automatic reconnection. The watcher runs until the channel is closed or an unrecoverable error occurs.
func (p *Publisher) Run() error {
	p.unexpectedErr = p.ch.NotifyClose(p.unexpectedErr)
	p.flow = p.ch.NotifyFlow(p.flow)
	go p.runWatcher()

	p.publisher.SetRoundTripper(p)
	return nil
}

// runWatcher monitors the publisher's channel for errors and flow control events.
// It runs in a separate goroutine and handles automatic reconnection on precondition failed errors.
//
// Error Handling:
// - On precondition failed errors (code 406): attempts automatic reconnection
// - On other errors or channel closure: reports error and terminates
//
// Flow Control:
// - Notifies the Observer when publishing flow is paused or resumed
func (p *Publisher) runWatcher() {
	for {
		select {
		case flow, isOpen := <-p.flow:
			if !isOpen {
				return
			}
			p.observer.PublishingFlow(p.publisher, flow)
		case err, isOpen := <-p.unexpectedErr:
			if !isOpen {
				return
			}
			if err != nil {
				p.observer.PublisherError(p.publisher, err)
				if isPreconditionFailed(err) {
					err1 := p.selfReconnect()
					if err1 != nil {
						p.observer.PublisherError(p.publisher, err1)
					}
				}
				return
			}
		}
	}
}

// selfReconnect attempts to restore the publisher after a channel failure.
//
// Reconnection Process:
// 1. Creates a new AMQP channel using the existing connection
// 2. Resets confirmation mode (sync.Once) to re-enable publisher confirms
// 3. Re-registers error and flow event channels
// 4. Re-registers the round tripper with middlewares
// 5. Starts a new watcher goroutine
// 6. Notifies the Observer of successful reconnection
//
// Returns an error if channel creation fails. Note that reconnection
// is only attempted for precondition failed errors (code 406).
// Other errors cause the publisher to stop permanently.
func (p *Publisher) selfReconnect() error {
	newCh, err := p.conn.Channel()
	if err != nil {
		return errors.WithMessage(err, "create channel while reconnect publisher")
	}

	p.ch = newCh
	p.setConfirmationMode = &sync.Once{}
	p.unexpectedErr = make(chan *amqp.Error, 1)
	p.unexpectedErr = newCh.NotifyClose(p.unexpectedErr)
	p.flow = make(chan bool, 1)
	p.flow = newCh.NotifyFlow(p.flow)
	p.publisher.SetRoundTripper(p)

	p.observer.PublisherReconnected(p.publisher)

	go p.runWatcher()

	return nil
}

// Close terminates the publisher's channel connection.
func (p *Publisher) Close() error {
	err := p.ch.Close()
	return errors.WithMessage(err, "channel close")
}

// isPreconditionFailed checks if the error is a precondition failed error (AMQP code 406).
// This typically occurs when trying to publish to a non-existent exchange or with invalid routing.
// Only precondition failed errors trigger automatic reconnection.
func isPreconditionFailed(err *amqp.Error) bool {
	return err.Code == amqp.PreconditionFailed
}
