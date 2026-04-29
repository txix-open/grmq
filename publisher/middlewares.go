package publisher

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// PersistentMode is a middleware that sets message delivery mode to persistent.
// This ensures messages are durably stored by the broker.
func PersistentMode() Middleware {
	return func(next RoundTripper) RoundTripper {
		return RoundTripperFunc(func(ctx context.Context, exchange string, routingKey string, msg *amqp.Publishing) error {
			msg.DeliveryMode = amqp.Persistent
			return next.Publish(ctx, exchange, routingKey, msg)
		})
	}
}
