package grmq

import (
	"time"

	"github.com/txix-open/grmq/consumer"
	"github.com/txix-open/grmq/publisher"
	"github.com/txix-open/grmq/topology"
)

type ClientOption func(c *Client)

func WithPublishers(publishers ...*publisher.Publisher) ClientOption {
	return func(c *Client) {
		c.publishers = publishers
	}
}

func WithConsumers(consumers ...consumer.Consumer) ClientOption {
	return func(c *Client) {
		c.consumers = consumers
	}
}

func WithDeclarations(declarations topology.Declarations) ClientOption {
	return func(c *Client) {
		c.declarations = declarations
	}
}

func WithTopologyBuilding(options ...topology.DeclarationsOption) ClientOption {
	declarations := topology.New(options...)
	return func(c *Client) {
		c.declarations = declarations
	}
}

func WithObserver(observer Observer) ClientOption {
	return func(c *Client) {
		c.observer = observer
	}
}

func WithReconnectTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.reconnectTimeout = timeout
	}
}

func WithDialConfig(config DialConfig) ClientOption {
	return func(c *Client) {
		c.dialConfig = config
	}
}
