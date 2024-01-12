package grmq

import (
	"context"
	"time"

	"github.com/integration-system/grmq/consumer"
	"github.com/integration-system/grmq/publisher"
	"github.com/integration-system/grmq/topology"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
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

	close        chan struct{}
	shutdownDone chan struct{}
}

func New(url string, options ...ClientOption) *Client {
	c := &Client{
		url: url,
		dialConfig: DialConfig{
			Config: amqp.Config{
				Heartbeat: 10 * time.Second,
				Locale:    "en_US",
			},
			DialTimeout: 30 * time.Second,
		},
		reconnectTimeout: 1 * time.Second,
		close:            make(chan struct{}),
		shutdownDone:     make(chan struct{}),
		observer:         NoopObserver{},
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
	firstSessionErr := make(chan error, 1)
	go s.run(ctx, firstSessionErr, true)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-firstSessionErr:
		return err
	}
}

// Serve
// Similar to Run but doesn't wait first successful session
// Just pass to Observer occurred errors and retry
func (s *Client) Serve(ctx context.Context) {
	firstSessionErr := make(chan error, 1)
	go s.run(ctx, firstSessionErr, false)
}

func (s *Client) run(ctx context.Context, firstSessionErr chan error, expectFirstSuccessSession bool) {
	defer func() {
		close(s.shutdownDone)
	}()

	sessNum := 0
	for {
		sessNum++
		err := s.runSession(sessNum, firstSessionErr)
		if err == nil { //normal close
			return
		}

		s.observer.ClientError(err)

		if err != nil && sessNum == 1 && expectFirstSuccessSession {
			return
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

func (s *Client) runSession(sessNum int, firstSessionErr chan error) (err error) {
	firstSessionErrWritten := false
	defer func() {
		if !firstSessionErrWritten && sessNum == 1 {
			firstSessionErr <- err
		}
	}()

	conn, err := amqp.DialConfig(s.url, s.dialConfig.Config)
	if err != nil {
		return errors.WithMessage(err, "dial")
	}
	defer conn.Close()

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

	if sessNum == 1 {
		firstSessionErrWritten = true
		firstSessionErr <- nil
	}

	s.observer.ClientReady()

	connCloseChan := make(chan *amqp.Error, 1)
	connCloseChan = conn.NotifyClose(connCloseChan)
	select {
	case err, isOpen := <-connCloseChan:
		if !isOpen {
			return nil
		}
		return err
	case <-s.close:
		return nil
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
