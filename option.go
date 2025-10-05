package timerx

import (
	"time"

	"github.com/robfig/cron/v3"
	"github.com/yuninks/timerx/logger"
)

type Options struct {
	logger        logger.Logger
	location      *time.Location
	timeout       time.Duration // 任务最长执行时间
	usePriority   bool
	priorityVal   int64
	batchSize     int
	maxRetryCount int
	cronParser    *cron.Parser // cron表达式解析器
}

func defaultOptions() Options {

	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

	return Options{
		logger:        logger.NewLogger(),
		location:      time.Local,
		timeout:       time.Hour, //
		usePriority:   false,
		priorityVal:   0,
		batchSize:     100,
		maxRetryCount: 0,
		cronParser:    &parser,
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
		if count < 0 {
			count = 0
		}
		o.maxRetryCount = count
	}
}

// 添加cron表达式解析器
func WithCronParser(parser cron.Parser) Option {
	return func(o *Options) {
		o.cronParser = &parser
	}
}

// 设置cron表达式解析器 秒级
// "*/5 * * * * ?" => 每隔5秒执行一次
// "0 0 0 * * ?" => 每天零点执行一次
// "0 0 0 1 * ?" => 每月1日零点执行一次
// "0 */5 * * * ?" => 每隔5分钟执行一次
func WithCronParserSecond() Option {
	return func(o *Options) {
		parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
		o.cronParser = &parser
	}
}

// 设置cron表达式解析器
// cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor
func WithCronParserOption(options cron.ParseOption) Option {
	return func(o *Options) {
		parser := cron.NewParser(options)
		o.cronParser = &parser
	}
}

// Cron表达式 与Linux的定时任务兼容
func WithCronParserLinux() Option {
	return func(o *Options) {
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		o.cronParser = &parser
	}
}
