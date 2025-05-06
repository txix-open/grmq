package grmq

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/txix-open/grmq/consumer"
	"github.com/txix-open/grmq/publisher"
	"github.com/txix-open/grmq/topology"
)

type closer interface {
	Close() error
}

type DialConfig struct {
	amqp.Config
	DialTimeout time.Duration
}

type Client struct {
	url              string
	dialConfig       DialConfig
	consumers        []consumer.Consumer
	publishers       []*publisher.Publisher
	declarations     topology.Declarations
	reconnectTimeout time.Duration
	observer         Observer

	mustReconnect *atomic.Bool

	reportErrOnce      *sync.Once
	firstOccurredError chan error

	close        chan struct{}
	shutdownDone chan struct{}
}

func New(url string, options ...ClientOption) *Client {
	mustReconnect := &atomic.Bool{}
	mustReconnect.Store(true)

	c := &Client{
		url: url,
		dialConfig: DialConfig{
			Config: amqp.Config{
				Heartbeat: 10 * time.Second,
				Locale:    "en_US",
			},
			DialTimeout: 30 * time.Second,
		},
		reconnectTimeout:   1 * time.Second,
		mustReconnect:      mustReconnect,
		reportErrOnce:      &sync.Once{},
		firstOccurredError: make(chan error, 1),
		close:              make(chan struct{}),
		shutdownDone:       make(chan struct{}),
		observer:           NoopObserver{},
	}
	for _, opt := range options {
		opt(c)
	}

	if c.dialConfig.Dial == nil {
		timeout := c.dialConfig.DialTimeout
		if timeout == 0 {
			timeout = 30 * time.Second
		}
		c.dialConfig.Dial = amqp.DefaultDial(timeout)
	}

	return c
}

// Run
// Block and wait first successfully established session
// It means all declarations were applied successfully
// All publishers were initialized
// All consumers were run
// Returns first occurred error during first session opening or nil
func (s *Client) Run(ctx context.Context) error {
	go s.run(ctx)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-s.firstOccurredError:
		if err != nil {
			s.mustReconnect.Store(false)
		}
		return err
	}
}

// Serve
// Similar to Run but doesn't wait first successful session
// Just pass to Observer occurred errors and retry
func (s *Client) Serve(ctx context.Context) {
	go s.run(ctx)
}

func (s *Client) run(ctx context.Context) {
	defer func() {
		close(s.shutdownDone)
	}()

	for {
		err := s.runSession()
		if err == nil { //normal close
			return
		}

		s.observer.ClientError(err)

		if !s.mustReconnect.Load() {
			return //prevent goroutine leak for Run if error occurred
		}

		select {
		case <-ctx.Done():
			s.observer.ClientError(ctx.Err())
			return
		case <-s.close:
			return //shutdown called
		case <-time.After(s.reconnectTimeout):

		}
	}
}

func (s *Client) runSession() (err error) {
	defer func() {
		if err != nil {
			s.reportFirstOccurredErrorOnes(err)
		}
	}()

	conn, err := amqp.DialConfig(s.url, s.dialConfig.Config)
	if err != nil {
		return errors.WithMessage(err, "dial")
	}
	defer conn.Close()
	connCloseChan := make(chan *amqp.Error, 1)
	connCloseChan = conn.NotifyClose(connCloseChan)
	connBlockedChan := make(chan amqp.Blocking, 1)
	connBlockedChan = conn.NotifyBlocked(connBlockedChan)

	ch, err := conn.Channel()
	if err != nil {
		return errors.WithMessage(err, "create channel for declarator")
	}
	declarator := NewDeclarator(s.declarations, ch)
	err = declarator.Run()
	if err != nil {
		return errors.WithMessage(err, "run declarator")
	}
	err = declarator.Close()
	if err != nil {
		return errors.WithMessage(err, "close declarator")
	}

	closers := make([]closer, 0)
	defer func() {
		for i := len(closers); i > 0; i-- {
			_ = closers[i-1].Close()
		}
	}()

	for _, publisher := range s.publishers {
		publisherUnit, err := s.runPublisher(publisher, conn)
		if err != nil {
			return errors.WithMessagef(err, "run publisher exchange: '%s', routingKey: '%s'", publisher.Exchange, publisher.RoutingKey)
		}
		closers = append(closers, publisherUnit)
	}

	for _, consumer := range s.consumers {
		ch, err = conn.Channel()
		if err != nil {
			return errors.WithMessagef(err, "create channel for consumer '%s'", consumer.Queue)
		}
		var retryPub *Publisher
		if consumer.RetryPolicy != nil {
			retryPub, err = s.runPublisher(publisher.New("", ""), conn)
			if err != nil {
				return errors.WithMessagef(err, "run retry publisher for consumer '%s'", consumer.Queue)
			}
			closers = append(closers, retryPub)
		}
		consumerUnit := NewConsumer(consumer, ch, retryPub, s.observer)
		err = consumerUnit.Run()
		if err != nil {
			return errors.WithMessagef(err, "run consumer '%s'", consumer.Queue)
		}
		closers = append(closers, consumerUnit)
	}

	s.reportFirstOccurredErrorOnes(nil) //to unblock Run

	s.observer.ClientReady()

	for {
		select {
		case blocking, isOpen := <-connBlockedChan:
			if !isOpen {
				return errors.New("block channel unexpectedly closed")
			}
			if blocking.Active {
				s.observer.ConnectionBlocked(blocking)
			}
		case err, isOpen := <-connCloseChan:
			if !isOpen {
				return errors.New("error channel unexpectedly closed")
			}
			return err
		case <-s.close:
			return nil
		}
	}
}

// Shutdown
// Perform graceful shutdown
func (s *Client) Shutdown() {
	close(s.close)
	s.observer.ShutdownStarted()
	<-s.shutdownDone
	s.observer.ShutdownDone()
}

func (s *Client) runPublisher(publisher *publisher.Publisher, conn *amqp.Connection) (*Publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, errors.WithMessage(err, "create channel")
	}
	publisherUnit := NewPublisher(publisher, ch, s.observer)
	err = publisherUnit.Run()
	if err != nil {
		return nil, errors.WithMessage(err, "run publisher")
	}
	return publisherUnit, nil
}

func (s *Client) reportFirstOccurredErrorOnes(err error) {
	s.reportErrOnce.Do(func() {
		s.firstOccurredError <- err
	})
}
