package heartbeat

import (
	"github.com/google/uuid"
	"github.com/yuninks/timerx/leader"
	"github.com/yuninks/timerx/logger"
	"github.com/yuninks/timerx/priority"
)

type Options struct {
	logger     logger.Logger      // 日志
	instanceId string             // 实例ID
	priority   *priority.Priority // 全局优先级
	leader     *leader.Leader     // Leader
	source     string             // 来源服务
}

func defaultOptions() Options {

	u, _ := uuid.NewV7()

	return Options{
		logger:     logger.NewLogger(),
		instanceId: u.String(),
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

func WithLogger(log logger.Logger) Option {
	return func(o *Options) {
		o.logger = log
	}
}

func WithPriority(p *priority.Priority) Option {
	return func(o *Options) {
		o.priority = p
	}
}

func WithLeader(l *leader.Leader) Option {
	return func(o *Options) {
		o.leader = l
	}
}

func WithInstanceId(instanceId string) Option {
	return func(o *Options) {
		o.instanceId = instanceId
	}
}

func WithSource(source string) Option {
	return func(o *Options) {
		o.source = source
	}
}
