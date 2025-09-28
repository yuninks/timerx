package timerx

// 作者：黄新云

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/yuninks/timerx/logger"
)

// 简单定时器
// 1. 这个定时器的作用范围是本机
// 2. 适用简单的时间间隔定时任务

type Single struct {
	ctx         context.Context
	cancel      context.CancelFunc
	logger      logger.Logger
	location    *time.Location
	nextTime    time.Time
	nextTimeMux sync.RWMutex
	wg          sync.WaitGroup
	workerList  sync.Map
	timerIndex  int64
	stopChan    chan struct{}
	hasRun      sync.Map
	timeout     time.Duration
}

// 定时器类
// @param ctx context.Context 上下文
// @param opts ...Option 配置项
func InitSingle(ctx context.Context, opts ...Option) *Single {
	op := newOptions(opts...)
	ctx, cancel := context.WithCancel(ctx)

	sin := &Single{
		ctx:      ctx,
		cancel:   cancel,
		logger:   op.logger,
		location: op.location,
		nextTime: time.Now(),
		stopChan: make(chan struct{}),
		timeout:  op.timeout,
	}

	sin.startDaemon()

	return sin
}

func (l *Single) startDaemon() {

	l.wg.Add(1)
	go l.timerLoop()

	l.wg.Add(1)
	go l.cleanupLoop()

}

// 停止所有定时任务
func (s *Single) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	close(s.stopChan)
	s.wg.Wait()

	// 清理所有资源
	s.workerList.Range(func(k, v interface{}) bool {
		if timer, ok := v.(timerStr); ok {
			close(timer.CanRunning)
		}
		s.workerList.Delete(k)
		return true
	})
}

// 获取任务数量
func (s *Single) TaskCount() int {
	count := 0
	s.workerList.Range(func(k, v interface{}) bool {
		count++
		return true
	})
	return count
}

func (l *Single) MaxIndex() int64 {
	return atomic.LoadInt64(&l.timerIndex) + 1
}

// 定时器主循环
func (s *Single) timerLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond) // 提高精度到100ms
	defer ticker.Stop()

	for {
		select {
		case t := <-ticker.C:
			s.nextTimeMux.RLock()
			nextTime := s.nextTime
			s.nextTimeMux.RUnlock()

			if t.Before(nextTime) {
				continue
			}

			s.iterator(s.ctx)

		case <-s.ctx.Done():
			s.logger.Infof(s.ctx, "timer: context cancelled, stopping timer loop")
			return
		case <-s.stopChan:
			s.logger.Infof(s.ctx, "timer: received stop signal, stopping timer loop")
			return
		}
	}
}

// 清理循环
func (s *Single) cleanupLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			cleanupTime := now.Add(-2 * time.Minute) // 清理2分钟前的记录

			s.hasRun.Range(func(k, v any) bool {
				t, ok := v.(time.Time)
				if !ok || t.Before(cleanupTime) {
					s.hasRun.Delete(k)
				}
				return true
			})

		case <-s.ctx.Done():
			s.logger.Infof(s.ctx, "timer: context cancelled, stopping cleanup loop")
			return
		case <-s.stopChan:
			s.logger.Infof(s.ctx, "timer: received stop signal, stopping cleanup loop")
			return
		}
	}
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
func (c *Single) EveryMonth(ctx context.Context, taskId string, day int, hour int, minute int, second int, callback func(ctx context.Context, extendData interface{}) error, extendData interface{}) (int64, error) {

	nowTime := time.Now().In(c.location)

	jobData := JobData{
		JobType:    JobTypeEveryMonth,
		TaskId:     taskId,
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
func (c *Single) EveryWeek(ctx context.Context, taskId string, week time.Weekday, hour int, minute int, second int, callback func(ctx context.Context, extendData interface{}) error, extendData interface{}) (int64, error) {
	nowTime := time.Now().In(c.location)

	jobData := JobData{
		JobType:    JobTypeEveryWeek,
		TaskId:     taskId,
		CreateTime: nowTime,
		Weekday:    week,
		Hour:       hour,
		Minute:     minute,
		Second:     second,
	}

	return c.addJob(ctx, jobData, callback, extendData)
}

// 每天执行一次
func (c *Single) EveryDay(ctx context.Context, taskId string, hour int, minute int, second int, callback func(ctx context.Context, extendData interface{}) error, extendData interface{}) (int64, error) {
	nowTime := time.Now().In(c.location)

	jobData := JobData{
		JobType:    JobTypeEveryDay,
		TaskId:     taskId,
		CreateTime: nowTime,
		Hour:       hour,
		Minute:     minute,
		Second:     second,
	}

	return c.addJob(ctx, jobData, callback, extendData)
}

// 每小时执行一次
func (c *Single) EveryHour(ctx context.Context, taskId string, minute int, second int, callback func(ctx context.Context, extendData interface{}) error, extendData interface{}) (int64, error) {
	nowTime := time.Now().In(c.location)

	jobData := JobData{
		JobType:    JobTypeEveryHour,
		TaskId:     taskId,
		CreateTime: nowTime,
		Minute:     minute,
		Second:     second,
	}

	return c.addJob(ctx, jobData, callback, extendData)
}

// 每分钟执行一次
func (c *Single) EveryMinute(ctx context.Context, taskId string, second int, callback func(ctx context.Context, extendData interface{}) error, extendData interface{}) (int64, error) {
	nowTime := time.Now().In(c.location)

	jobData := JobData{
		JobType:    JobTypeEveryMinute,
		TaskId:     taskId,
		CreateTime: nowTime,
		Second:     second,
	}

	return c.addJob(ctx, jobData, callback, extendData)
}

// 特定时间间隔
func (c *Single) EverySpace(ctx context.Context, taskId string, spaceTime time.Duration, callback func(ctx context.Context, extendData interface{}) error, extendData interface{}) (int64, error) {
	nowTime := time.Now().In(c.location)

	if spaceTime < 0 {
		c.logger.Errorf(ctx, "间隔时间不能小于0")
		return 0, errors.New("间隔时间不能小于0")
	}

	jobData := JobData{
		JobType:      JobTypeInterval,
		TaskId:       taskId,
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
func (l *Single) addJob(ctx context.Context, jobData JobData, call func(ctx context.Context, extendData interface{}) error, extend interface{}) (int64, error) {
	if jobData.TaskId == "" {
		l.logger.Errorf(ctx, "任务ID不能为空")
		return 0, ErrTaskIdEmpty
	}
	if jobData.Day < 0 || jobData.Day > 31 {
		l.logger.Errorf(ctx, "每月的天数必须在0-31之间")
		return 0, ErrMonthDay
	}
	if jobData.Hour < 0 || jobData.Hour > 23 {
		l.logger.Errorf(ctx, "小时必须在0-23之间")
		return 0, ErrHour
	}
	if jobData.Minute < 0 || jobData.Minute > 59 {
		l.logger.Errorf(ctx, "分钟必须在0-59之间")
		return 0, ErrMinute
	}
	if jobData.Second < 0 || jobData.Second > 59 {
		l.logger.Errorf(ctx, "秒必须在0-59之间")
		return 0, ErrSecond
	}
	if call == nil {
		l.logger.Errorf(ctx, "回调函数不能为空")
		return 0, ErrCallbackEmpty
	}

	nextTime, err := GetNextTime(time.Now().In(l.location), jobData)
	if err != nil {
		l.logger.Errorf(ctx, "获取下次执行时间失败:%s", err.Error())
		return 0, err
	}
	jobData.NextTime = *nextTime

	// 生成唯一索引
	index := atomic.AddInt64(&l.timerIndex, 1)

	t := timerStr{
		Callback:   call,
		CanRunning: make(chan struct{}, 1),
		ExtendData: extend,
		TaskId:     jobData.TaskId,
		JobData:    &jobData,
	}

	l.workerList.Store(index, t)

	// 计算下次执行时间(全局)
	l.updateNextTimeIfEarlier(*nextTime)

	return index, nil
}

// 如果更早则更新下次执行时间(全局)
func (s *Single) updateNextTimeIfEarlier(candidate time.Time) {
	s.nextTimeMux.Lock()
	defer s.nextTimeMux.Unlock()

	if candidate.Before(s.nextTime) {
		s.nextTime = candidate
	}
}

// 删除定时器
func (l *Single) Del(index int64) {
	if val, ok := l.workerList.Load(index); ok {
		if timer, ok := val.(timerStr); ok {
			close(timer.CanRunning)
		}
		l.workerList.Delete(index)
	}
}

// 迭代定时器列表
func (l *Single) iterator(ctx context.Context) {
	// 当前时间
	nowTime := time.Now().In(l.location)

	// 默认5秒后（如果没有值就暂停进来5秒）
	newNextTime := nowTime.Add(time.Second * 5)

	l.workerList.Range(func(k, v interface{}) bool {
		timeStr, ok := v.(timerStr)
		if !ok {
			l.logger.Errorf(ctx, "timer: 类型断言失败，跳过该任务")
			l.workerList.Delete(k)
			return true
		}

		if timeStr.JobData.NextTime.Before(nowTime) || timeStr.JobData.NextTime.Equal(nowTime) {

			originTime := timeStr.JobData.NextTime

			// 计算下次执行时间
			nextTime, err := GetNextTime(nowTime, *timeStr.JobData)
			if err != nil {
				l.logger.Errorf(ctx, "timer: 计算下次执行时间失败:%s", err.Error())
				return true
			}
			// 更新下次执行时间
			timeStr.JobData.NextTime = *nextTime

			if nextTime.Before(newNextTime) {
				// 本规则下次发送时间小于系统下次需要执行的时间：替换
				newNextTime = *nextTime
			}

			go l.executeTask(ctx, timeStr, originTime)

		}

		return true

	})

	l.updateNextTime(newNextTime)
}

// 执行任务
func (s *Single) executeTask(ctx context.Context, timer timerStr, originTime time.Time) {
	// 创建带追踪ID的上下文
	traceCtx := context.WithValue(ctx, "trace_id", uuid.NewV4().String())
	s.logger.Infof(traceCtx, "timer Single begin taskId:%s originTime:%d", timer.TaskId, originTime.UnixMilli())
	traceCtx, cancel := context.WithTimeout(traceCtx, s.timeout) // 设置执行超时
	defer cancel()

	select {
	case timer.CanRunning <- struct{}{}:
		defer func() {
			select {
			case <-timer.CanRunning:
			default:
			}
		}()

		// 执行回调
		if err := s.doTask(traceCtx, timer, originTime); err != nil {
			s.logger.Errorf(traceCtx, "timer: 任务执行失败: %s", err.Error())
		}

	default:
		// 任务正在执行中，跳过本次
		s.logger.Infof(traceCtx, "timer: 任务正在执行中，跳过本次 %s", timer.TaskId)
	}
}

// 定时器操作类
// 这里不应painc
func (l *Single) doTask(ctx context.Context, timeStr timerStr, originTime time.Time) error {
	// 检查任务是否已执行
	taskKey := fmt.Sprintf("%s:%d", timeStr.TaskId, originTime.UnixMilli())
	if _, loaded := l.hasRun.LoadOrStore(taskKey, time.Now()); loaded {
		l.logger.Errorf(ctx, "timer: 任务已执行，跳过本次执行 %s", timeStr.TaskId)
		return ErrTaskExecuted
	}

	defer func() {
		if err := recover(); err != nil {
			l.logger.Errorf(ctx, "timer Single call panic err:%+v stack:%s", err, string(debug.Stack()))
		}
	}()

	err := timeStr.Callback(ctx, timeStr.ExtendData)
	if err != nil {
		l.logger.Errorf(ctx, "timer Single call back %s, err: %v", timeStr.TaskId, err)
		return err
	}

	return nil
}

// 更新下次执行时间
func (s *Single) updateNextTime(newTime time.Time) {
	s.nextTimeMux.Lock()
	defer s.nextTimeMux.Unlock()

	now := time.Now()
	if newTime.Before(now) {
		s.nextTime = now
	} else {
		s.nextTime = newTime
	}
}
