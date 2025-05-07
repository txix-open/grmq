package grmq

import (
	"github.com/txix-open/grmq/consumer"
	"github.com/txix-open/grmq/publisher"
)

type Observer interface {
	ClientReady()
	ClientError(err error)
	ConsumerError(consumer consumer.Consumer, err error)
	PublisherError(publisher *publisher.Publisher, err error)
	PublishingFlow(publisher *publisher.Publisher, flow bool)
	ConnectionBlocked(reason string)
	ConnectionUnblocked()
	ShutdownStarted()
	ShutdownDone()
}

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
