package publisher

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PersistentMode() Middleware {
	return func(next RoundTripper) RoundTripper {
		return RoundTripperFunc(func(ctx context.Context, exchange string, routingKey string, msg *amqp.Publishing) error {
			msg.DeliveryMode = amqp.Persistent
			return next.Publish(ctx, exchange, routingKey, msg)
		})
	}
}
