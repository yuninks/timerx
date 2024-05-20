package timerx

import (
	"errors"
	"time"
)

// 计算该任务下次执行时间
// @param job *JobData 任务数据
// @return time.Time 下次执行时间
func GetNextTime(t time.Time,loc *time.Location, job JobData) (*time.Time, error) {

	var next time.Time

	switch job.JobType {
	case JobTypeEveryMonth:
		next = calculateNextMonthTime(t, job, loc)
	case JobTypeEveryWeek:
		next = calculateNextWeekTime(t, job, loc)
	case JobTypeEveryDay:
		next = calculateNextDayTime(t, job, loc)
	case JobTypeEveryHour:
		next = calculateNextHourTime(t, job, loc)
	case JobTypeEveryMinute:
		next = calculateNextMinuteTime(t, job, loc)
	case JobTypeInterval:
		next = calculateNextInterval(t, job)
	default:
		return nil, errors.New("未知的任务类型: " + string(job.JobType))
	}

	return &next, nil
}

func calculateNextInterval(t time.Time, job JobData) time.Time {
	// 从创建的时候开始计算
	cycle := t.Sub(job.CreateTime).Microseconds() / job.IntervalTime.Microseconds()
	return job.CreateTime.Add(job.IntervalTime * time.Duration(cycle+1))
}

func calculateNextMonthTime(t time.Time, job JobData, loc *time.Location) time.Time {
	// 判断是否可执行并返回下一个执行时间
	if canRun(t, job) {
		return time.Date(t.Year(), t.Month(), job.Day, job.Hour, job.Minute, job.Second, 0, loc)
	}
	// 下一个周期（下个月）
	return time.Date(t.Year(), t.Month()+1, job.Day, job.Hour, job.Minute, job.Second, 0, loc)
}

func calculateNextWeekTime(t time.Time, job JobData, loc *time.Location) time.Time {
	weekday := t.Weekday()
	days := int(job.Weekday - weekday)
	if days < 0 {
		days += 7
	}
	// 判断是否可执行并返回下一个执行时间
	if canRun(t, job) {
		return time.Date(t.Year(), t.Month(), t.Day(), job.Hour, job.Minute, job.Second, 0, loc)
	}
	// 下一个周期（下周）
	return time.Date(t.Year(), t.Month(), t.Day()+days+7, job.Hour, job.Minute, job.Second, 0, loc)
}

func calculateNextDayTime(t time.Time, job JobData, loc *time.Location) time.Time {
	// 判断是否可执行并返回下一个执行时间
	if canRun(t, job) {
		return time.Date(t.Year(), t.Month(), t.Day(), job.Hour, job.Minute, job.Second, 0, loc)
	}
	// 下一个周期（明天）
	return time.Date(t.Year(), t.Month(), t.Day()+1, job.Hour, job.Minute, job.Second, 0, loc)
}

func calculateNextHourTime(t time.Time, job JobData, loc *time.Location) time.Time {
	// 判断是否可执行并返回下一个执行时间
	if canRun(t, job) {
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), job.Minute, job.Second, 0, loc)
	}
	// 下一个周期（下个小时）
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, job.Minute, job.Second, 0, loc)
}

func calculateNextMinuteTime(t time.Time, job JobData, loc *time.Location) time.Time {
	// 判断是否可执行并返回下一个执行时间
	if canRun(t, job) {
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), job.Second, 0, loc)
	}
	// 下一个周期（下分钟）
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute()+1, job.Second, 0, loc)
}

// 检查是否本周期可以运行
func canRun(t time.Time, job JobData) bool {
	switch job.JobType {
	case JobTypeEveryMonth:
		return t.Day() < job.Day ||
			(t.Day() == job.Day && t.Hour() < job.Hour) ||
			(t.Day() == job.Day && t.Hour() == job.Hour && t.Minute() < job.Minute) ||
			(t.Day() == job.Day && t.Hour() == job.Hour && t.Minute() == job.Minute && t.Second() <= job.Second)
	case JobTypeEveryWeek:
		return t.Weekday() < job.Weekday ||
			(t.Weekday() == job.Weekday && t.Hour() < job.Hour) ||
			(t.Weekday() == job.Weekday && t.Hour() == job.Hour && t.Minute() < job.Minute) ||
			(t.Weekday() == job.Weekday && t.Hour() == job.Hour && t.Minute() == job.Minute && t.Second() <= job.Second)
	case JobTypeEveryDay:
		return t.Hour() < job.Hour ||
			(t.Hour() == job.Hour && t.Minute() < job.Minute) ||
			(t.Hour() == job.Hour && t.Minute() == job.Minute && t.Second() <= job.Second)
	case JobTypeEveryHour:
		return t.Minute() < job.Minute ||
			(t.Minute() == job.Minute && t.Second() <= job.Second)
	case JobTypeEveryMinute:
		return t.Second() <= job.Second
	default:
		return false
	}
}
