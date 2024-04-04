package timerx

type Options struct {
	logger Logger
}

func defaultOptions() Options {
	return Options{
		logger: NewLogger(),
	}
}

type Option func(*Options)

func newOptions(opts ...Option) Options {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

func SetLogger(log Logger) Option {
	return func(o *Options) {
		o.logger = log
	}
}
