package timerx

import "time"

type Options struct {
	logger   Logger
	location *time.Location
}

func defaultOptions() Options {
	return Options{
		logger:   NewLogger(),
		location: time.Local,
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

// 设置日志
func SetLogger(log Logger) Option {
	return func(o *Options) {
		o.logger = log
	}
}

// 设定时区
func SetTimeZone(zone *time.Location) Option {
	return func(o *Options) {
		o.location = zone
	}
}
