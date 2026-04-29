package grmq

import (
	"time"

	"github.com/txix-open/grmq/consumer"
	"github.com/txix-open/grmq/publisher"
	"github.com/txix-open/grmq/topology"
)

// ClientOption is a function that configures a Client instance.
type ClientOption func(c *Client)

// WithPublishers sets the publishers for the client.
func WithPublishers(publishers ...*publisher.Publisher) ClientOption {
	return func(c *Client) {
		c.publishers = publishers
	}
}

// WithConsumers sets the consumers for the client.
func WithConsumers(consumers ...consumer.Consumer) ClientOption {
	return func(c *Client) {
		c.consumers = consumers
	}
}

// WithDeclarations sets the topology declarations for the client.
func WithDeclarations(declarations topology.Declarations) ClientOption {
	return func(c *Client) {
		c.declarations = declarations
	}
}

// WithTopologyBuilding creates and sets topology declarations using the provided options.
func WithTopologyBuilding(options ...topology.DeclarationsOption) ClientOption {
	declarations := topology.New(options...)
	return func(c *Client) {
		c.declarations = declarations
	}
}

// WithObserver sets the observer for monitoring client events.
func WithObserver(observer Observer) ClientOption {
	return func(c *Client) {
		c.observer = observer
	}
}

// WithReconnectTimeout sets the timeout between reconnection attempts.
func WithReconnectTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.reconnectTimeout = timeout
	}
}

// WithDialConfig sets the dial configuration for connecting to RabbitMQ.
func WithDialConfig(config DialConfig) ClientOption {
	return func(c *Client) {
		c.dialConfig = config
	}
}
