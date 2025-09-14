package timerx

import (
	"errors"
	"time"
)

// 计算该任务下次执行时间
// @param job *JobData 任务数据
// @param t time.Time 当前时间
// @return time.Time 下次执行时间
// @return error 错误信息
func GetNextTime(t time.Time, job JobData) (*time.Time, error) {

	if err := validateJobData(job); err != nil {
		return nil, err
	}

	var next *time.Time
	var err error

	switch job.JobType {
	case JobTypeEveryMonth:
		next, err = calculateNextMonthTime(t, job)
	case JobTypeEveryWeek:
		next, err = calculateNextWeekTime(t, job)
	case JobTypeEveryDay:
		next, err = calculateNextDayTime(t, job)
	case JobTypeEveryHour:
		next, err = calculateNextHourTime(t, job)
	case JobTypeEveryMinute:
		next, err = calculateNextMinuteTime(t, job)
	case JobTypeInterval:
		next, err = calculateNextInterval(t, job)
	default:
		return nil, errors.New("未知的任务类型: " + string(job.JobType))
	}

	if err != nil {
		return nil, err
	}

	return next, nil
}

// 参数校验
func validateJobData(job JobData) error {
	switch job.JobType {
	case JobTypeEveryMonth:
		if job.Day < 1 || job.Day > 31 {
			return ErrMonthDay
		}
	case JobTypeEveryWeek:
		if job.Weekday < time.Sunday || job.Weekday > time.Saturday {
			return ErrWeekday
		}
	case JobTypeEveryDay:
		if job.Hour < 0 || job.Hour > 23 {
			return ErrHour
		}
	case JobTypeEveryHour:
		if job.Minute < 0 || job.Minute > 59 {
			return ErrMinute
		}
	case JobTypeEveryMinute:
		if job.Second < 0 || job.Second > 59 {
			return ErrSecond
		}
	case JobTypeInterval:
		if job.IntervalTime <= 0 {
			return ErrIntervalTime
		}
		if job.CreateTime.IsZero() {
			return ErrCreateTime
		}
	}

	if job.Hour < 0 || job.Hour > 23 {
		return ErrHour
	}
	if job.Minute < 0 || job.Minute > 59 {
		return ErrMinute
	}
	if job.Second < 0 || job.Second > 59 {
		return ErrSecond
	}

	return nil
}

func calculateNextInterval(t time.Time, job JobData) (*time.Time, error) {
	if job.CreateTime.IsZero() {
		return nil, ErrCreateTime
	}
	if job.IntervalTime <= 0 {
		return nil, ErrIntervalTime
	}
	// 计算从创建时间到当前时间经过了多少个间隔
	elapsed := t.Sub(job.CreateTime)
	intervals := elapsed / job.IntervalTime

	// 计算下一个执行时间
	next := job.CreateTime.Add((intervals + 1) * job.IntervalTime)

	// 确保下次执行时间不早于当前时间
	if next.Before(t) || next.Equal(t) {
		next = next.Add(job.IntervalTime)
	}

	return &next, nil
}

func calculateNextMonthTime(t time.Time, job JobData) (*time.Time, error) {
	// 尝试光剑本月的执行四件
	currentMonthTime := time.Date(t.Year(), t.Month(), job.Day, job.Hour, job.Minute, job.Second, 0, t.Location())

	// 如果日期无效（比如2月30号），则调整到该月最后一天
	if currentMonthTime.Day() != job.Day {
		// 获取该月的最后一天
		lastDay := time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, t.Location()).Day()
		if job.Day > lastDay {
			currentMonthTime = time.Date(t.Year(), t.Month(), lastDay, job.Hour, job.Minute, job.Second, 0, t.Location())
		}
	}

	if currentMonthTime.After(t) {
		return &currentMonthTime, nil
	}

	// 计算下个月的同一天
	nextMonth := t.Month() + 1
	year := t.Year()
	if nextMonth > 12 {
		nextMonth = 1
		year++
	}

	nextMonthTime := time.Date(year, nextMonth, job.Day, job.Hour, job.Minute, job.Second, 0, t.Location())
	// 如果日期无效，调整到下个月的最后一天
	if nextMonthTime.Day() != job.Day {
		lastDay := time.Date(year, nextMonth+1, 0, 0, 0, 0, 0, t.Location()).Day()
		if job.Day > lastDay {
			nextMonthTime = time.Date(year, nextMonth, lastDay, job.Hour, job.Minute, job.Second, 0, t.Location())
		}
	}

	return &nextMonthTime, nil
}

func calculateNextWeekTime(t time.Time, job JobData) (*time.Time, error) {
	currentWeekday := t.Weekday()
	targetWeekday := job.Weekday

	// 计算距离目标星期几的天数
	daysToAdd := int(targetWeekday - currentWeekday)
	if daysToAdd < 0 {
		daysToAdd += 7
	}
	// 本周的目标时间
	thisWeekTime := time.Date(t.Year(), t.Month(), t.Day()+daysToAdd, job.Hour, job.Minute, job.Second, 0, t.Location())

	if thisWeekTime.After(t) {
		return &thisWeekTime, nil
	}

	// 下周的目标时间
	nextWeekTime := time.Date(t.Year(), t.Month(), t.Day()+daysToAdd+7, job.Hour, job.Minute, job.Second, 0, t.Location())
	return &nextWeekTime, nil
}

func calculateNextDayTime(t time.Time, job JobData) (*time.Time, error) {
	// 今天的目标时间
	todayTime := time.Date(t.Year(), t.Month(), t.Day(), job.Hour, job.Minute, job.Second, 0, t.Location())

	if todayTime.After(t) {
		return &todayTime, nil
	}

	// 明天的时间
	nextDayTime := time.Date(t.Year(), t.Month(), t.Day()+1, job.Hour, job.Minute, job.Second, 0, t.Location())
	return &nextDayTime, nil

}

func calculateNextHourTime(t time.Time, job JobData) (*time.Time, error) {
	// 计算当前小时的目标时间
	currentHourTime := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), job.Minute, job.Second, 0, t.Location())

	if currentHourTime.After(t) {
		return &currentHourTime, nil
	}

	// 下一个小时的时间
	nextHourTime := time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, job.Minute, job.Second, 0, t.Location())
	return &nextHourTime, nil
}

func calculateNextMinuteTime(t time.Time, job JobData) (*time.Time, error) {
	// 计算当前分钟的目标时间

	currentMinuteTime := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), job.Second, 0, t.Location())

	if currentMinuteTime.After(t) {
		return &currentMinuteTime, nil
	}

	// 下一分钟的时间
	nextMinuteTime := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute()+1, job.Second, 0, t.Location())
	return &nextMinuteTime, nil
}

// 检查是否本周期可以运行
// 检查是否本周期可以运行（已弃用，使用新的时间比较逻辑）
// 保留此函数用于向后兼容，但建议使用新的时间计算逻辑
func canRun(t time.Time, job JobData) bool {
	targetTime := time.Date(t.Year(), t.Month(), t.Day(), job.Hour, job.Minute, job.Second, 0, t.Location())

	switch job.JobType {
	case JobTypeEveryMonth:
		// 对于月任务，需要比较日期
		targetTime = time.Date(t.Year(), t.Month(), job.Day, job.Hour, job.Minute, job.Second, 0, t.Location())
		return !targetTime.Before(t)
	case JobTypeEveryWeek:
		// 对于周任务，需要比较星期
		currentWeekday := t.Weekday()
		if currentWeekday < job.Weekday {
			return true
		}
		if currentWeekday == job.Weekday {
			return targetTime.After(t) || targetTime.Equal(t)
		}
		return false
	case JobTypeEveryDay:
		return targetTime.After(t) || targetTime.Equal(t)
	case JobTypeEveryHour:
		hourTarget := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), job.Minute, job.Second, 0, t.Location())
		return hourTarget.After(t) || hourTarget.Equal(t)
	case JobTypeEveryMinute:
		minuteTarget := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), job.Second, 0, t.Location())
		return minuteTarget.After(t) || minuteTarget.Equal(t)
	default:
		return false
	}
}
