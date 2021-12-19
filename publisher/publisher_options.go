package publisher

type Option func(publisher *Publisher)

func WithMiddlewares(middlewares ...Middleware) Option {
	return func(publisher *Publisher) {
		publisher.Middlewares = middlewares
	}
}
