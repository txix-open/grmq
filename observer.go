package grmq

import (
	"github.com/txix-open/grmq/consumer"
	"github.com/txix-open/grmq/publisher"
)

// Observer defines an interface for monitoring client lifecycle events and errors.
// Implement this interface to receive notifications about connection state,
// publisher/consumer errors, and shutdown events.
type Observer interface {
	ClientReady()
	ClientError(err error)
	ConsumerError(consumer consumer.Consumer, err error)
	PublisherError(publisher *publisher.Publisher, err error)
	PublishingFlow(publisher *publisher.Publisher, flow bool)
	PublisherReconnected(publisher *publisher.Publisher)
	ConnectionBlocked(reason string)
	ConnectionUnblocked()
	ShutdownStarted()
	ShutdownDone()
}

// NoopObserver provides a default no-op implementation of the Observer interface.
// Use this when you don't need to observe client events.
// You can also embed NoopObserver in your observer struct
type NoopObserver struct {
}

func (n NoopObserver) ClientReady() {

}

func (n NoopObserver) ClientError(err error) {

}

func (n NoopObserver) ConsumerError(consumer consumer.Consumer, err error) {

}

func (n NoopObserver) ShutdownStarted() {
}

func (n NoopObserver) ShutdownDone() {

}

func (n NoopObserver) PublisherError(publisher *publisher.Publisher, err error) {

}

func (n NoopObserver) PublishingFlow(publisher *publisher.Publisher, flow bool) {

}

func (n NoopObserver) ConnectionBlocked(reason string) {

}

func (n NoopObserver) ConnectionUnblocked() {

}

func (n NoopObserver) PublisherReconnected(publisher *publisher.Publisher) {}
