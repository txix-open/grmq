package retry

import (
	"context"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher interface {
	PublishWithConfirmation(ctx context.Context, exchange string, routingKey string, msg *amqp.Publishing) error
}

type Retrier struct {
	originalQueue string
	policy        Policy
	pub           Publisher
}

func NewRetryer(originalQueue string, policy Policy, pub Publisher) *Retrier {
	return &Retrier{
		originalQueue: originalQueue,
		policy:        policy,
		pub:           pub,
	}
}

func (r *Retrier) Do(delivery *amqp.Delivery) error {
	retry := r.nextRetry(delivery)
	switch {
	case retry != nil:
		queue := retry.QueueName(r.originalQueue)
		msg := &amqp.Publishing{
			Headers:         delivery.Headers,
			ContentType:     delivery.ContentType,
			ContentEncoding: delivery.ContentEncoding,
			DeliveryMode:    delivery.DeliveryMode,
			Priority:        delivery.Priority,
			CorrelationId:   delivery.CorrelationId,
			ReplyTo:         delivery.ReplyTo,
			Expiration:      delivery.Expiration,
			MessageId:       delivery.MessageId,
			Timestamp:       delivery.Timestamp,
			Type:            delivery.Type,
			UserId:          delivery.UserId,
			AppId:           delivery.AppId,
			Body:            delivery.Body,
		}
		err := r.pub.PublishWithConfirmation(context.Background(), "", queue, msg)
		if err != nil {
			return errors.WithMessagef(err, "publish to %s", queue)
		}

		err = delivery.Ack(false)
		if err != nil {
			return errors.WithMessage(err, "ack")
		}
	case r.policy.FinallyMoveToDlq:
		err := delivery.Nack(false, false)
		if err != nil {
			return errors.WithMessage(err, "nack with no requeue")
		}
	default:
		err := delivery.Ack(false)
		if err != nil {
			return errors.WithMessage(err, "ack")
		}
	}

	return nil
}

func (r *Retrier) nextRetry(delivery *amqp.Delivery) *Retry {
	retriesCount := totalTries(delivery.Headers)
	retriesByPolicies := int64(0)
	for i := range r.policy.Retries {
		retry := r.policy.Retries[i]
		if retry.MaxAttempts == Forever {
			return &retry
		}
		retriesByPolicies += int64(retry.MaxAttempts)
		if retriesCount < retriesByPolicies {
			return &retry
		}
	}

	return nil
}

func totalTries(headers amqp.Table) int64 {
	totalRetries := int64(0)
	xDeath, ok := headers["x-death"].([]any)
	if !ok {
		return totalRetries
	}
	for _, elem := range xDeath {
		table, ok := elem.(amqp.Table)
		if !ok {
			continue
		}
		totalRetries += table["count"].(int64)
	}
	return totalRetries
}
