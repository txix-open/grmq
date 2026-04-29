package publisher

// Option is a function that configures a Publisher instance.
type Option func(publisher *Publisher)

// WithMiddlewares sets the middleware chain for the publisher.
// Middlewares are executed in the order they are provided.
func WithMiddlewares(middlewares ...Middleware) Option {
	return func(publisher *Publisher) {
		publisher.Middlewares = middlewares
	}
}
