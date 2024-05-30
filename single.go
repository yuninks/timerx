package timerx

// 作者：黄新云

import (
	"context"
	"errors"
	"runtime/debug"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
)

// 简单定时器
// 1. 这个定时器的作用范围是本机
// 2. 适用简单的时间间隔定时任务

// 定时器结构体
var singleWorkerList sync.Map

var singleTimerIndex int // 当前定时数目

var singleOnceLimit sync.Once // 实现单例

type Single struct {
	ctx      context.Context
	logger   Logger
	location *time.Location
}

var sin *Single = nil

var singleNextTime = time.Now() // 下一次执行的时间

// 定时器类
// @param ctx context.Context 上下文
// @param opts ...Option 配置项
func InitSingle(ctx context.Context, opts ...Option) *Single {
	singleOnceLimit.Do(func() {
		op := newOptions(opts...)

		sin = &Single{
			ctx:      ctx,
			logger:   op.logger,
			location: op.location,
		}

		timer := time.NewTicker(time.Millisecond * 200)
		go func(ctx context.Context) {
		Loop:
			for {
				select {
				case t := <-timer.C:
					if t.Before(singleNextTime) {
						// 当前时间小于下次发送时间：跳过
						continue
					}
					// 迭代定时器
					sin.iterator(ctx)
					// fmt.Println("timer: 执行")
				case <-ctx.Done():
					// 跳出循环
					break Loop
				}
			}
			sin.logger.Infof(ctx, "timer: initend")
		}(ctx)
	})

	return sin
}

// 每月执行一次
// @param ctx 上下文
// @param taskId 任务ID
// @param day 每月的几号
// @param hour 小时
// @param minute 分钟
// @param second 秒
// @param callback 回调函数
// @param extendData 扩展数据
// @return error
func (c *Single) AddMonth(ctx context.Context, taskId string, day int, hour int, minute int, second int, callback callback, extendData interface{}) (int, error) {
	nowTime := time.Now()

	jobData := JobData{
		JobType:    JobTypeEveryMonth,
		CreateTime: nowTime,
		Day:        day,
		Hour:       hour,
		Minute:     minute,
		Second:     second,
	}

	return c.addJob(ctx, jobData, callback, extendData)
}

// 每周执行一次
// @param ctx context.Context 上下文
// @param taskId string 任务ID
// @param week time.Weekday 周
// @param hour int 小时
// @param minute int 分钟
// @param second int 秒
func (c *Single) AddWeek(ctx context.Context, taskId string, week time.Weekday, hour int, minute int, second int, callback callback, extendData interface{}) (int, error) {
	nowTime := time.Now()

	jobData := JobData{
		JobType:    JobTypeEveryWeek,
		CreateTime: nowTime,
		Weekday:    week,
		Hour:       hour,
		Minute:     minute,
		Second:     second,
	}

	return c.addJob(ctx, jobData, callback, extendData)
}

// 每天执行一次
func (c *Single) AddDay(ctx context.Context, taskId string, hour int, minute int, second int, callback callback, extendData interface{}) (int, error) {
	nowTime := time.Now()

	jobData := JobData{
		JobType:    JobTypeEveryDay,
		CreateTime: nowTime,
		Hour:       hour,
		Minute:     minute,
		Second:     second,
	}

	return c.addJob(ctx, jobData, callback, extendData)
}

// 每小时执行一次
func (c *Single) AddHour(ctx context.Context, taskId string, minute int, second int, callback callback, extendData interface{}) (int, error) {
	nowTime := time.Now()

	jobData := JobData{
		JobType:    JobTypeEveryHour,
		CreateTime: nowTime,
		Minute:     minute,
		Second:     second,
	}

	return c.addJob(ctx, jobData, callback, extendData)
}

// 每分钟执行一次
func (c *Single) AddMinute(ctx context.Context, taskId string, second int, callback callback, extendData interface{}) (int, error) {
	nowTime := time.Now()

	jobData := JobData{
		JobType:    JobTypeEveryMinute,
		CreateTime: nowTime,
		Second:     second,
	}

	return c.addJob(ctx, jobData, callback, extendData)
}

// 特定时间间隔
func (c *Single) AddSpace(ctx context.Context, taskId string, spaceTime time.Duration, callback callback, extendData interface{}) (int, error) {
	nowTime := time.Now()

	if spaceTime < 0 {
		c.logger.Errorf(ctx, "间隔时间不能小于0")
		return 0, errors.New("间隔时间不能小于0")
	}

	jobData := JobData{
		JobType:      JobTypeInterval,
		CreateTime:   nowTime,
		IntervalTime: spaceTime,
	}

	return c.addJob(ctx, jobData, callback, extendData)
}

// 间隔定时器
// @param space 间隔时间
// @param call 回调函数
// @param extend 附加参数
// @return int 定时器索引
// @return error 错误
func (l *Single) addJob(ctx context.Context, jobData JobData, call callback, extend interface{}) (int, error) {
	singleTimerIndex += 1

	_, err := GetNextTime(time.Now().In(l.location), jobData)
	if err != nil {
		l.logger.Errorf(ctx, "获取下次执行时间失败:%s", err.Error())
		return 0, err
	}

	t := timerStr{
		Callback:   call,
		CanRunning: make(chan struct{}, 1),
		ExtendData: extend,
		JobData:    &jobData,
	}

	singleWorkerList.Store(singleTimerIndex, t)

	return singleTimerIndex, nil
}

// 删除定时器
func (s *Single) Del(index int) {
	singleWorkerList.Delete(index)
}

// 迭代定时器列表
func (s *Single) iterator(ctx context.Context) {

	nowTime := time.Now().In(s.location)

	// 默认5秒后（如果没有值就暂停进来5秒）
	newNextTime := nowTime.Add(time.Second * 5)

	index := 0
	singleWorkerList.Range(func(k, v interface{}) bool {
		index++
		timeStr := v.(timerStr)

		if timeStr.JobData.NextTime.Before(nowTime) || timeStr.JobData.NextTime.Equal(nowTime) {
			// 可执行
			nextTime, _ := GetNextTime(nowTime, *timeStr.JobData)
			timeStr.JobData.NextTime = *nextTime

			if index == 1 {
				// 循环的第一个需要替换默认值
				newNextTime = timeStr.JobData.NextTime
			}

			if nextTime.Before(newNextTime) {
				// 本规则下次发送时间小于系统下次需要执行的时间：替换
				newNextTime = *nextTime
			}

			// 处理中就跳过本次
			go func(ctx context.Context, v timerStr) {
				select {
				case v.CanRunning <- struct{}{}:
					defer func() {
						// fmt.Printf("timer: 执行完成 %v %v \n", k, v.Tag)
						select {
						case <-v.CanRunning:
							return
						default:
							return
						}
					}()
					// fmt.Printf("timer: 准备执行 %v %v \n", k, v.Tag)
					s.doTask(ctx, v.Callback, v.ExtendData)
				default:
					// fmt.Printf("timer: 已在执行 %v %v \n", k, v.Tag)
					return
				}
			}(ctx, timeStr)

		}

		return true

	})

	// 实际下次时间小于预期下次时间：替换
	if singleNextTime.Before(newNextTime) {
		// 判断一下避免异常
		if newNextTime.Before(nowTime) {
			// 比当前时间小
			singleNextTime = nowTime
		} else {
			singleNextTime = newNextTime
		}
	}

	// fmt.Println("timer: one finish")
}

// 定时器操作类
// 这里不应painc
func (s *Single) doTask(ctx context.Context, call callback, extend interface{}) error {
	defer func() {
		if err := recover(); err != nil {
			s.logger.Errorf(ctx, "timer:回调任务panic err:%+v stack:%s", err, string(debug.Stack()))
		}
	}()

	ctx = context.WithValue(ctx, "trace_id", uuid.NewV4().String)

	return call(ctx, extend)
}
