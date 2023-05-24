package consumer

import (
	"github.com/integration-system/grmq/retry"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrDeliveryAlreadyHandled = errors.New("delivery already handled")
)

type Donner interface {
	Done()
}

type Delivery struct {
	donner  Donner
	source  *amqp.Delivery
	retryer *retry.Retryer
	handled bool
}

func NewDelivery(donner Donner, source *amqp.Delivery, retrier *retry.Retryer) *Delivery {
	return &Delivery{
		donner:  donner,
		source:  source,
		retryer: retrier,
	}
}

func (d *Delivery) Source() *amqp.Delivery {
	return d.source
}

func (d *Delivery) Ack() error {
	if d.handled {
		return ErrDeliveryAlreadyHandled
	}

	defer d.donner.Done()
	d.handled = true

	err := d.source.Ack(false)
	if err != nil {
		return errors.WithMessage(err, "ack delivery")
	}
	return nil
}

func (d *Delivery) Nack(requeue bool) error {
	if d.handled {
		return ErrDeliveryAlreadyHandled
	}

	defer d.donner.Done()
	d.handled = true

	err := d.source.Nack(false, requeue)
	if err != nil {
		return errors.WithMessage(err, "nack delivery")
	}
	return nil
}

func (d *Delivery) Retry() error {
	if d.handled {
		return ErrDeliveryAlreadyHandled
	}

	if d.retryer == nil {
		return errors.New("retryer is not initialized. possibly retry policy for consumer was not specified")
	}

	defer d.donner.Done()
	d.handled = true

	err := d.retryer.Do(d.source)
	if err != nil {
		return errors.WithMessage(err, "perform retry")
	}

	return nil
}
