package topology

type Declarations struct {
	Exchanges []*Exchange
	Queues    []*Queue
	Bindings  []*Binding
}

func New(opts ...DeclarationsOption) Declarations {
	d := &Declarations{}
	for _, opt := range opts {
		opt(d)
	}
	return *d
}

type DeclarationsOption func(d *Declarations)

func WithQueue(name string, opts ...QueueOption) DeclarationsOption {
	queue := NewQueue(name, opts...)
	return func(d *Declarations) {
		d.Queues = append(d.Queues, queue)
	}
}

func WithDirectExchange(name string) DeclarationsOption {
	exchange := NewDirectExchange(name)
	return func(d *Declarations) {
		d.Exchanges = append(d.Exchanges, exchange)
	}
}

func WithFanoutExchange(name string) DeclarationsOption {
	exchange := NewFanoutExchange(name)
	return func(d *Declarations) {
		d.Exchanges = append(d.Exchanges, exchange)
	}
}

func WithBinding(exchangeName string, queueName string, routingKey string) DeclarationsOption {
	binding := NewBinding(exchangeName, queueName, routingKey)
	return func(d *Declarations) {
		d.Bindings = append(d.Bindings, binding)
	}
}
