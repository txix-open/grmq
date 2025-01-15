package retry

import (
	"context"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"maps"
)

const (
	grmqRetryCountHeader = "grmq-retry-count"
)

type Publisher interface {
	PublishWithConfirmation(ctx context.Context, exchange string, routingKey string, msg *amqp.Publishing) error
}

type Retryer struct {
	originalQueue string
	policy        Policy
	pub           Publisher
}

func NewRetryer(originalQueue string, policy Policy, pub Publisher) *Retryer {
	return &Retryer{
		originalQueue: originalQueue,
		policy:        policy,
		pub:           pub,
	}
}

func (r *Retryer) Do(delivery *amqp.Delivery) error {
	retriesCount := totalTries(delivery.Headers)
	retry := r.nextRetry(retriesCount)
	switch {
	case retry != nil:
		queue := retry.QueueName(r.originalQueue)
		headers := maps.Clone(delivery.Headers)
		if headers == nil {
			headers = amqp.Table{}
		}
		headers[grmqRetryCountHeader] = retriesCount + 1
		msg := &amqp.Publishing{
			Headers:         headers,
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

func (r *Retryer) nextRetry(retriesCount int64) *Retry {
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
	if headers == nil {
		return totalRetries
	}
	headerValue, ok := headers[grmqRetryCountHeader]
	if !ok {
		return totalRetries
	}
	switch xDeath := headerValue.(type) {
	case int64:
		totalRetries = xDeath
	case int32:
		totalRetries = int64(xDeath)
	case float64:
		totalRetries = int64(xDeath)
	case float32:
		totalRetries = int64(xDeath)
	}
	return totalRetries
}
