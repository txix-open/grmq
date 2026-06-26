package consumer

import (
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/txix-open/grmq/retry"
)

var (
	// ErrDeliveryAlreadyHandled is returned when attempting to handle an already processed delivery.
	ErrDeliveryAlreadyHandled = errors.New("delivery already handled")
)

// Donner defines an interface for signaling completion of delivery processing.
type Donner interface {
	Done()
}

// Delivery wraps an AMQP delivery with acknowledgment and retry capabilities.
type Delivery struct {
	// Body contains the payload visible to handlers.
	// It may differ from source.Body (e.g. after transparent decompression).
	// The original AMQP payload is always preserved in source.Body.
	Body []byte

	donner  Donner
	source  *amqp.Delivery
	retryer *retry.Retryer
	handled bool
}

// NewDelivery creates a new Delivery instance with the specified configuration.
func NewDelivery(donner Donner, source *amqp.Delivery, retrier *retry.Retryer) *Delivery {
	return &Delivery{
		donner:  donner,
		source:  source,
		retryer: retrier,
		Body:    source.Body,
	}
}

// Source returns the underlying AMQP delivery.
func (d *Delivery) Source() *amqp.Delivery {
	return d.source
}

// Ack Acknowledges the delivery, marking it as successfully processed and removing it from the queue.
// The delivery must be acknowledged exactly once. Subsequent calls will return ErrDeliveryAlreadyHandled.
// Returns an error if the broker fails to acknowledge the delivery (e.g., connection issues).
//
// After Ack returns (either successfully or with an error), the delivery is considered handled
// and no further acknowledgment operations should be performed on it.
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

// Nack negatively acknowledges the delivery, indicating processing failed.
// The requeue parameter controls whether the message is returned to the queue immediately (true)
// or sent to the dead-letter queue (false).
// Returns an error if the delivery has already been handled or if the broker rejects the nack.
//
// After Nack returns (either successfully or with an error), the delivery is considered handled
// and no further acknowledgment operations should be performed on it.
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

// Retry attempts to redeliver the message according to the configured retry policy.
// If a retry policy is configured, the message is published to a retry queue with a delay.
// If no retry policy is configured, the message is requeued via Nack.
//
// Returns ErrDeliveryAlreadyHandled if the delivery has already been processed.
// Returns an error if the retry operation fails (e.g., publishing to retry queue fails).
//
// Note: When Retry returns an error, the message has NOT been acknowledged and will need
// to be handled again by the caller.
func (d *Delivery) Retry() error {
	if d.handled {
		return ErrDeliveryAlreadyHandled
	}

	defer d.donner.Done()
	d.handled = true

	if d.retryer == nil {
		_ = d.source.Nack(false, true)
		return errors.New("retryer is not initialized. possibly retry policy for consumer was not specified. call nack with requeue")
	}

	err := d.retryer.Do(d.source)
	if err != nil {
		return errors.WithMessage(err, "perform retry")
	}

	return nil
}
