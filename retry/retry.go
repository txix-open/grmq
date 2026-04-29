// Package retry provides retry policies for failed message processing in consumers.
package retry

import (
	"fmt"
	"time"
)

const (
	// Forever represents unlimited retry attempts.
	Forever = -1
)

// Retry defines a single retry step with a delay and maximum attempt count.
type Retry struct {
	Delay       time.Duration
	MaxAttempts int
}

// QueueName generates the queue name for this retry step.
func (r Retry) QueueName(originalQueue string) string {
	return fmt.Sprintf("%s.retry.%d", originalQueue, r.Delay.Milliseconds())
}

// Policy defines a complete retry configuration with multiple retry steps.
type Policy struct {
	// Retries is a list of retry steps to attempt.
	Retries []Retry
	// FinallyMoveToDlq specifies whether to move the message to DLQ after all retries are exhausted.
	FinallyMoveToDlq bool
}

// NewPolicy creates a new retry policy with the specified configuration.
func NewPolicy(finallyMoveToDlq bool, retries ...Retry) Policy {
	return Policy{
		Retries:          retries,
		FinallyMoveToDlq: finallyMoveToDlq,
	}
}

// WithDelay creates a retry step with a specific delay and maximum attempts.
func WithDelay(delay time.Duration, maxTries int) Retry {
	return Retry{
		Delay:       delay,
		MaxAttempts: maxTries,
	}
}

// ForeverWithDelay creates a retry step that retries indefinitely with the specified delay.
func ForeverWithDelay(delay time.Duration) Retry {
	return Retry{
		Delay:       delay,
		MaxAttempts: Forever,
	}
}
