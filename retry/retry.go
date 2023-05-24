package retry

import (
	"fmt"
	"time"
)

const (
	Forever = -1
)

type Retry struct {
	Delay       time.Duration
	MaxAttempts int
}

func (r Retry) QueueName(originalQueue string) string {
	return fmt.Sprintf("%s.retry.%d", originalQueue, r.Delay.Milliseconds())
}

type Policy struct {
	Retries          []Retry
	FinallyMoveToDlq bool
}

func NewPolicy(finallyMoveToDlq bool, retries ...Retry) Policy {
	return Policy{
		Retries:          retries,
		FinallyMoveToDlq: finallyMoveToDlq,
	}
}

func WithDelay(delay time.Duration, maxTries int) Retry {
	return Retry{
		Delay:       delay,
		MaxAttempts: maxTries,
	}
}

func ForeverWithDelay(delay time.Duration) Retry {
	return Retry{
		Delay:       delay,
		MaxAttempts: Forever,
	}
}
