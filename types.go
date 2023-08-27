package timer

import "time"

type timerStr struct {
	Callback   callback        // 需要回调的方法
	CanRunning chan (struct{}) // 是否允许执行
	BeginTime  time.Time       // 初始化任务的时间
	NextTime   time.Time       // [删]下一次执行的时间
	SpaceTime  time.Duration   // 任务间隔时间
	UniqueKey  string          // 全局唯一键
	Extend     ExtendParams    // 附加参数
}

// 扩展参数
type ExtendParams struct {
	Params map[string]interface{} // 带出去的参数
}
