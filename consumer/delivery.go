package consumer

import (
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
	handled bool
}

func NewDelivery(donner Donner, source *amqp.Delivery) *Delivery {
	return &Delivery{
		donner: donner,
		source: source,
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
