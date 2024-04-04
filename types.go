package timerx

import (
	"context"
	"time"
)

type timerStr struct {
	Callback   callback        // 需要回调的方法
	CanRunning chan (struct{}) // 是否允许执行
	BeginTime  time.Time       // 初始化任务的时间
	NextTime   time.Time       // [删]下一次执行的时间
	SpaceTime  time.Duration   // 任务间隔时间
	TaskId     string          // 任务ID 全局唯一键
	ExtendData interface{}     // 附加参数
	JobType    JobType         // 任务类型
	JobData    *JobData        // 任务时间数据
}

type JobType string

const (
	JobTypeEveryDay    JobType = "every_day"
	JobTypeEveryHour   JobType = "every_hour"
	JobTypeEveryMinute JobType = "every_minute"
	JobTypeEverySecond JobType = "every_second"
	JobTypeEveryMonth  JobType = "every_month"
	// 根据间隔时间执行
	JobTypeInterval JobType = "interval"
)

type JobData struct {
	Month   *time.Month   // 每年的第几个月
	Weekday *time.Weekday // 每周的周几
	Day     *int          // 每月的第几天
	Hour    *int          // 每天的第几个小时
	Minute  *int          // 每小时的第几分钟
	Second  *int          // 每分钟的第几秒
}

var nextTime = time.Now() // 下一次执行的时间

// 定义各个回调函数
type callback func(ctx context.Context, extendData interface{}) error
