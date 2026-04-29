// Package topology provides types and functions for defining RabbitMQ topology
// including exchanges, queues, and bindings.
package topology

// Declarations represents a complete topology configuration.
type Declarations struct {
	Exchanges []*Exchange
	Queues    []*Queue
	Bindings  []*Binding
}

// New creates a new Declarations instance with the specified options.
func New(opts ...DeclarationsOption) Declarations {
	d := &Declarations{}
	for _, opt := range opts {
		opt(d)
	}
	return *d
}

// DeclarationsOption is a function that configures a Declarations instance.
type DeclarationsOption func(d *Declarations)

// WithQueue adds a queue to the declarations.
func WithQueue(name string, opts ...QueueOption) DeclarationsOption {
	queue := NewQueue(name, opts...)
	return func(d *Declarations) {
		d.Queues = append(d.Queues, queue)
	}
}

// WithDirectExchange adds a direct exchange to the declarations.
func WithDirectExchange(name string) DeclarationsOption {
	exchange := NewDirectExchange(name)
	return func(d *Declarations) {
		d.Exchanges = append(d.Exchanges, exchange)
	}
}

// WithFanoutExchange adds a fanout exchange to the declarations.
func WithFanoutExchange(name string) DeclarationsOption {
	exchange := NewFanoutExchange(name)
	return func(d *Declarations) {
		d.Exchanges = append(d.Exchanges, exchange)
	}
}

// WithTopicExchange adds a topic exchange to the declarations.
func WithTopicExchange(name string) DeclarationsOption {
	exchange := NewTopicExchange(name)
	return func(d *Declarations) {
		d.Exchanges = append(d.Exchanges, exchange)
	}
}

// WithBinding adds a binding between an exchange and a queue.
func WithBinding(exchangeName string, queueName string, routingKey string) DeclarationsOption {
	binding := NewBinding(exchangeName, queueName, routingKey)
	return func(d *Declarations) {
		d.Bindings = append(d.Bindings, binding)
	}
}
