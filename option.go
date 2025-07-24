package timerx

import (
	"time"

	"github.com/yuninks/timerx/logger"
)

type Options struct {
	logger   logger.Logger
	location *time.Location
	timeout  time.Duration
	priority  int
}

func defaultOptions() Options {
	return Options{
		logger:   logger.NewLogger(),
		location: time.Local,
		timeout:  time.Hour,
		priority: 0,
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
func SetLogger(log logger.Logger) Option {
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

// 设置任务最长执行时间
func SetTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.timeout = d
	}
}

// 设置优先级
func SetPriority(priority int) Option {
	return func(o *Options) {
		o.priority = priority
	}
}
