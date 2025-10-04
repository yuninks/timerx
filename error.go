package timerx

import "errors"

var (
	// 定时器不存在
	ErrTimerNotFound = errors.New("timer not found")
	// 任务ID不能为空
	ErrTaskIdEmpty = errors.New("taskId can not be empty")
	// 每月的天数必须在0-31之间
	ErrMonthDay = errors.New("month day must be between 0 and 31")
	// 小时必须在0-23之间
	ErrHour = errors.New("hour must be between 0 and 23")
	// 分钟必须在0-59之间
	ErrMinute = errors.New("minute must be between 0 and 59")
	// 秒必须在0-59之间
	ErrSecond = errors.New("second must be between 0 and 59")
	// 回调函数不能为空
	ErrCallbackEmpty = errors.New("callback can not be empty")
	// 星期必须在0-6之间
	ErrWeekday = errors.New("weekday must be between Sunday and Saturday")
	// 创建时间不能为空
	ErrCreateTime = errors.New("create time can not be empty")
	// 基准时间不能为空
	ErrBaseTime = errors.New("base time can not be empty")
	// 间隔时间必须大于0
	ErrIntervalTime = errors.New("interval time must be greater than 0")
	// 任务Id已存在
	ErrTaskIdExists = errors.New("taskId already exists")
	// 任务已执行
	ErrTaskExecuted = errors.New("task already executed")
)
