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

type Client struct {
	url              string
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
		url:              url,
		reconnectTimeout: 1 * time.Second,
		close:            make(chan struct{}),
		shutdownDone:     make(chan struct{}),
		observer:         NoopObserver{},
	}
	for _, opt := range options {
		opt(c)
	}
	return c
}

// Run
// Block and wait first successfully established session
// It means all declarations were applied successfully
// All publishers were initialized
// All consumers were run
// Returns first occurred error during session opening or nil
func (s *Client) Run(ctx context.Context) error {
	firstSessionErr := make(chan error, 1)
	go s.run(ctx, firstSessionErr)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-firstSessionErr:
		return err
	}
}

func (s *Client) run(ctx context.Context, firstSessionErr chan error) {
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

		select {
		case <-ctx.Done():
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

	conn, err := amqp.Dial(s.url)
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

	for i, publisher := range s.publishers {
		ch, err = conn.Channel()
		if err != nil {
			return errors.WithMessagef(err, "create channel for publisher %d", i)
		}
		publisherUnit := NewPublisher(publisher, ch)
		err = publisherUnit.Run()
		if err != nil {
			return errors.WithMessagef(err, "run publisher %d", i)
		}
		closers = append(closers, publisherUnit)
	}

	for _, consumer := range s.consumers {
		ch, err = conn.Channel()
		if err != nil {
			return errors.WithMessagef(err, "create channel for consumer for '%s'", consumer.Queue)
		}
		consumerUnit := NewConsumer(consumer, ch, s.observer)
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

	connCloseChan := make(chan *amqp.Error)
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
