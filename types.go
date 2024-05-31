package timerx

import (
	"context"
	"time"
)

type timerStr struct {
	Callback   func(ctx context.Context, extendData interface{}) error        // 需要回调的方法
	CanRunning chan (struct{}) // 是否允许执行(only single)
	TaskId     string          // 任务ID 全局唯一键(only cluster)
	ExtendData interface{}     // 附加参数
	JobData    *JobData        // 任务时间数据
}

type JobType string

const (
	JobTypeEveryMonth  JobType = "every_month"  // 每月
	JobTypeEveryWeek   JobType = "every_week"   // 每周
	JobTypeEveryDay    JobType = "every_day"    // 每天
	JobTypeEveryHour   JobType = "every_hour"   // 每小时
	JobTypeEveryMinute JobType = "every_minute" // 每分钟
	JobTypeEverySecond JobType = "every_second" // 每秒
	JobTypeInterval    JobType = "interval"     // 指定时间间隔
)

type JobData struct {
	JobType      JobType       // 任务类型
	NextTime     time.Time     // 下次执行时间
	BaseTime     time.Time     // 基准时间(间隔的基准时间)
	CreateTime   time.Time     // 任务创建时间
	IntervalTime time.Duration // 任务间隔时间
	Month        time.Month    // 每年的第几个月
	Weekday      time.Weekday  // 每周的周几
	Day          int           // 每月的第几天
	Hour         int           // 每天的第几个小时
	Minute       int           // 每小时的第几分钟
	Second       int           // 每分钟的第几秒
}

// 定义各个回调函数
// type callback func(ctx context.Context, extendData interface{}) error
