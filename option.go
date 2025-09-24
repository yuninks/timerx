package timerx

import (
	"time"

	"github.com/yuninks/timerx/logger"
)

type Options struct {
	logger        logger.Logger
	location      *time.Location
	timeout       time.Duration
	usePriority   bool
	priorityVal   int64
	batchSize     int
	maxRetryCount int
}

func defaultOptions() Options {
	return Options{
		logger:        logger.NewLogger(),
		location:      time.Local,
		timeout:       time.Hour,
		usePriority:   false,
		priorityVal:   0,
		batchSize:     100,
		maxRetryCount: 100,
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
func WithLogger(log logger.Logger) Option {
	return func(o *Options) {
		o.logger = log
	}
}

// 设定时区
func WithLocation(zone *time.Location) Option {
	return func(o *Options) {
		o.location = zone
	}
}

// 设置任务最长执行时间
func WithTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.timeout = d
	}
}

// 设置优先级
func WithPriority(priority int64) Option {
	return func(o *Options) {
		o.usePriority = true
		o.priorityVal = priority
	}
}

func WithBatchSize(size int) Option {
	return func(o *Options) {
		if size <= 1 {
			size = 1
		}
		o.batchSize = size
	}
}

func WithMaxRetryCount(count int) Option {
	return func(o *Options) {
		if count <= 0 {
			count = 1
		}
		o.maxRetryCount = count
	}
}
