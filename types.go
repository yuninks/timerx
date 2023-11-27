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
	UniqueKey  string          // 全局唯一键
	ExtendData interface{}     // 附加参数
}


var nextTime = time.Now() // 下一次执行的时间


// 定义各个回调函数
type callback func(ctx context.Context, extendData interface{}) error
