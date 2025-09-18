package priority

import (
	"time"

	"github.com/yuninks/timerx/logger"
)

type Options struct {
	getInterval    time.Duration // 查询周期
	updateInterval time.Duration // 更新间隔
	expireTime     time.Duration // 有效时间
	logger         logger.Logger
}

func defaultOptions() Options {
	return Options{
		getInterval:    time.Second * 2,
		updateInterval: time.Second * 4,
		expireTime:     time.Second * 8,
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

func SetLogger(log logger.Logger) Option {
	return func(o *Options) {
		o.logger = log
	}
}

// 更新周期
func SetUpdateInterval(d time.Duration) Option {
	if d.Abs() < time.Second {
		d = time.Second * 5
	}
	return func(o *Options) {
		o.updateInterval = d
		o.expireTime = d*2 + time.Second
		o.getInterval = d / 3
	}
}
