package priority

import (
	"time"

	"github.com/yuninks/timerx/logger"
)

type Options struct {
	priority       int           // 优先级,数字越大越优先
	updateInterval time.Duration // 更新间隔
	expireTime     time.Duration
	logger         logger.Logger
}

func defaultOptions() Options {
	return Options{
		priority:       0, // 默认优先级
		updateInterval: time.Second * 10,
		expireTime:     time.Second * 32,
		logger:         logger.NewLogger(),
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

func SetPriority(priority int) Option {
	return func(o *Options) {
		o.priority = priority
	}
}

func SetLogger(log logger.Logger) Option {
	return func(o *Options) {
		o.logger = log
	}
}

// 有效时间是3个周期
func SetUpdateInterval(d time.Duration) Option {
	if d.Abs() < time.Second {
		d = time.Second * 10
	}
	return func(o *Options) {
		o.updateInterval = d
		o.expireTime = d*3 + time.Second
	}
}
