package timerx

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/yuninks/cachex"
	"github.com/yuninks/lockx"
	"github.com/yuninks/timerx/heartbeat"
	"github.com/yuninks/timerx/leader"
	"github.com/yuninks/timerx/logger"
	"github.com/yuninks/timerx/priority"
)

// 功能描述

// 这是基于Redis的定时任务调度器，能够有效的在服务集群里面调度任务，避免了单点压力过高或单点故障问题
// 由于所有的服务代码是一致的，也就是一个定时任务将在所有的服务都有注册，具体调度到哪个服务运行看调度结果

type Cluster struct {
	ctx       context.Context       // context
	cancel    context.CancelFunc    // 取消函数
	redis     redis.UniversalClient // redis
	timeout   time.Duration         // job执行超时时间
	logger    logger.Logger         // 日志
	keyPrefix string                // key前缀
	location  *time.Location        // 根据时区计算的时间

	lockKey        string // 全局计算的key
	zsetKey        string // 有序集合的key
	listKey        string // 可执行的任务列表的key
	setKey         string // 重入集合的key
	executeInfoKey string // 执行情况的key

	wg         sync.WaitGroup // 等待组
	workerList sync.Map       // 注册的任务列表
	stopChan   chan struct{}  //
	instanceId string         // 实例ID

	priority    *priority.Priority // 全局优先级
	priorityKey string             // 全局优先级的key
	usePriority bool               // 是否使用优先级

	leader    *leader.Leader       // Leader
	heartbeat *heartbeat.HeartBeat // 心跳
	cache     *cachex.Cache        // 本地缓存
}

// 初始化定时器
// 全局只需要初始化一次
func InitCluster(ctx context.Context, red redis.UniversalClient, keyPrefix string, opts ...Option) (*Cluster, error) {

	if red == nil {
		return nil, errors.New("redis is nil")
	}

	op := newOptions(opts...)

	ctx, cancel := context.WithCancel(ctx)

	U, _ := uuid.NewV7()

	clu := &Cluster{
		ctx:            ctx,
		cancel:         cancel,
		redis:          red,
		cache:          cachex.NewCache(),
		timeout:        op.timeout,
		logger:         op.logger,
		keyPrefix:      keyPrefix,
		location:       op.location,
		lockKey:        "timer:cluster_globalLockKey" + keyPrefix,  // 定时器的全局锁
		zsetKey:        "timer:cluster_zsetKey" + keyPrefix,        // 有序集合
		listKey:        "timer:cluster_listKey" + keyPrefix,        // 列表
		setKey:         "timer:cluster_setKey" + keyPrefix,         // 重入集合
		priorityKey:    "timer:cluster_priorityKey" + keyPrefix,    // 全局优先级的key
		executeInfoKey: "timer:cluster_executeInfoKey" + keyPrefix, // 执行情况的key 有序集合
		usePriority:    op.usePriority,
		stopChan:       make(chan struct{}),
		instanceId:     U.String(),
	}

	// 初始化优先级

	if clu.usePriority {

		pri, err := priority.InitPriority(
			ctx,
			red,
			clu.keyPrefix,
			op.priorityVal,
			priority.WithLogger(clu.logger),
			priority.WithInstanceId(clu.instanceId),
			priority.WithSource("cluster"),
		)
		if err != nil {
			clu.logger.Errorf(ctx, "InitPriority err:%v", err)
			return nil, err
		}
		clu.priority = pri
	}

	// 初始化leader
	le, err := leader.InitLeader(
		ctx,
		clu.redis,
		clu.keyPrefix,
		leader.WithLogger(clu.logger),
		leader.WithPriority(clu.priority),
		leader.WithInstanceId(clu.instanceId),
		leader.WithSource("cluster"),
	)
	if err != nil {
		clu.logger.Infof(ctx, "InitLeader err:%v", err)
		return nil, err
	}
	clu.leader = le

	// 初始化心跳
	heart, err := heartbeat.InitHeartBeat(
		ctx,
		clu.redis,
		clu.keyPrefix,
		heartbeat.WithInstanceId(clu.instanceId),
		heartbeat.WithLeader(clu.leader),
		heartbeat.WithLogger(clu.logger),
		heartbeat.WithPriority(clu.priority),
		heartbeat.WithSource("once"),
	)
	if err != nil {
		clu.logger.Errorf(ctx, "InitHeartBeat err:%v", err)
		return nil, err
	}
	clu.heartbeat = heart

	// 启动守护进程
	clu.startDaemon()

	clu.logger.Infof(ctx, "InitCluster success keyPrefix:%s instanceId:%s", clu.keyPrefix, clu.instanceId)

	return clu, nil
}

// Stop 停止集群定时器
func (l *Cluster) Stop() {
	close(l.stopChan)
	if l.usePriority && l.priority != nil {
		l.priority.Close()
	}
	if l.leader != nil {
		l.leader.Close()
	}
	if l.heartbeat != nil {
		l.heartbeat.Close()
	}
	if l.cancel != nil {
		l.cancel()
	}

	l.wg.Wait()
}

// 守护任务
func (l *Cluster) startDaemon() {

	// 任务调度
	l.wg.Add(1)
	go l.scheduleTasks()

	// 任务执行
	l.wg.Add(1)
	go l.executeTasks()

	l.wg.Add(1)
	go l.cleanExecuteInfoLoop()

}

func (l *Cluster) cleanExecuteInfoLoop() {
	l.wg.Done()

	ticker := time.NewTicker(time.Minute * 5)
	defer ticker.Stop()

	for {
		select {
		case <-l.stopChan:
			return
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			if l.leader.IsLeader() {
				l.cleanExecuteInfo()
			}
		}
	}
}

// 清除过期任务
func (l *Cluster) cleanExecuteInfo() error {
	// 移除执行信息
	l.redis.ZRemRangeByScore(l.ctx, l.executeInfoKey, "0", strconv.FormatInt(time.Now().Add(-15*time.Minute).UnixMilli(), 10)).Err()
	return nil
}

// scheduleTasks 调度任务（只有leader执行）
func (c *Cluster) scheduleTasks() {
	defer c.wg.Done()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !c.leader.IsLeader() {
				continue
			}
			if c.usePriority && !c.priority.IsLatest(c.ctx) {
				continue
			}
			c.calculateNextTimes()
			c.moveReadyTasks()
		case <-c.stopChan:
			return
		case <-c.ctx.Done():
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
func (c *Cluster) EveryMonth(ctx context.Context, taskId string, day int, hour int, minute int, second int, callback func(ctx context.Context, extendData interface{}) error, extendData interface{}) error {
	// nowTime := time.Now().In(c.location)

	jobData := JobData{
		JobType: JobTypeEveryMonth,
		// CreateTime: nowTime,
		Day:    day,
		Hour:   hour,
		Minute: minute,
		Second: second,
	}

	return c.addJob(ctx, taskId, jobData, callback, extendData)
}

// 每周执行一次
// @param ctx context.Context 上下文
// @param taskId string 任务ID
// @param week time.Weekday 周
// @param hour int 小时
// @param minute int 分钟
// @param second int 秒
func (c *Cluster) EveryWeek(ctx context.Context, taskId string, week time.Weekday, hour int, minute int, second int, callback func(ctx context.Context, extendData interface{}) error, extendData interface{}) error {
	// nowTime := time.Now().In(c.location)

	jobData := JobData{
		JobType: JobTypeEveryWeek,
		// CreateTime: nowTime,
		Weekday: week,
		Hour:    hour,
		Minute:  minute,
		Second:  second,
	}

	return c.addJob(ctx, taskId, jobData, callback, extendData)
}

// 每天执行一次
func (c *Cluster) EveryDay(ctx context.Context, taskId string, hour int, minute int, second int, callback func(ctx context.Context, extendData interface{}) error, extendData interface{}) error {
	// nowTime := time.Now().In(c.location)

	jobData := JobData{
		JobType: JobTypeEveryDay,
		// CreateTime: nowTime,
		Hour:   hour,
		Minute: minute,
		Second: second,
	}

	return c.addJob(ctx, taskId, jobData, callback, extendData)
}

// 每小时执行一次
func (c *Cluster) EveryHour(ctx context.Context, taskId string, minute int, second int, callback func(ctx context.Context, extendData interface{}) error, extendData interface{}) error {
	// nowTime := time.Now().In(c.location)

	jobData := JobData{
		JobType: JobTypeEveryHour,
		// CreateTime: nowTime,
		Minute: minute,
		Second: second,
	}

	return c.addJob(ctx, taskId, jobData, callback, extendData)
}

// 每分钟执行一次
func (c *Cluster) EveryMinute(ctx context.Context, taskId string, second int, callback func(ctx context.Context, extendData interface{}) error, extendData interface{}) error {
	// nowTime := time.Now().In(c.location)

	jobData := JobData{
		JobType: JobTypeEveryMinute,
		// CreateTime: nowTime,
		Second: second,
	}

	return c.addJob(ctx, taskId, jobData, callback, extendData)
}

// 特定时间间隔
func (c *Cluster) EverySpace(ctx context.Context, taskId string, spaceTime time.Duration, callback func(ctx context.Context, extendData interface{}) error, extendData interface{}) error {
	nowTime := time.Now().In(c.location)

	if spaceTime < 0 {
		c.logger.Errorf(ctx, "间隔时间不能小于0")
		return errors.New("间隔时间不能小于0")
	}

	// 获取当天的零点时间
	zeroTime := time.Date(nowTime.Year(), nowTime.Month(), nowTime.Day(), 0, 0, 0, 0, nowTime.Location())

	jobData := JobData{
		JobType:      JobTypeInterval,
		BaseTime:     zeroTime, // 默认当天的零点
		IntervalTime: spaceTime,
	}

	return c.addJob(ctx, taskId, jobData, callback, extendData)
}

// 统一添加任务
// @param ctx context.Context 上下文
// @param taskId string 任务ID
// @param jobData *JobData 任务数据
// @param callback callback 回调函数
// @param extendData interface{} 扩展数据
// @return error
func (l *Cluster) addJob(ctx context.Context, taskId string, jobData JobData, callback func(ctx context.Context, extendData interface{}) error, extendData interface{}) error {
	// 判断是否重复
	_, ok := l.workerList.Load(taskId)
	if ok {
		l.logger.Errorf(ctx, "Cluster addJob taskId exits:%s", taskId)
		return ErrTaskIdExists
	}

	// 校验时间是否合法
	_, err := GetNextTime(time.Now().In(l.location), jobData)
	if err != nil {
		l.logger.Errorf(ctx, "Cluster addJob GetNextTime err:%s", err.Error())
		return err
	}

	t := timerStr{
		Callback:   callback,
		ExtendData: extendData,
		TaskId:     taskId,
		JobData:    &jobData,
	}

	l.workerList.Store(taskId, t)

	l.logger.Infof(ctx, "Cluster addJob taskId:%s", taskId)

	return nil
}

// 计算下一次执行的时间
func (l *Cluster) calculateNextTimes() {

	pipe := l.redis.Pipeline()

	// 根据内部注册的任务列表计算下一次执行的时间
	l.workerList.Range(func(key, value interface{}) bool {
		val := value.(timerStr)

		nextTime, err := GetNextTime(time.Now().In(l.location), *val.JobData)
		if err != nil {
			l.logger.Errorf(l.ctx, "Cluster calculateNextTimes GetNextTime err:%s %s", val.TaskId, err.Error())
			return true
		}

		// l.logger.Infof(l.ctx, "Cluster calculateNextTimes GetNextTime nextTime:%s %s", val.TaskId, nextTime.Format(time.RFC3339))

		// 使用Lua脚本原子性添加任务
		script := `
			local zsetKey = KEYS[1]
			local lockKey = KEYS[2]
			local score = ARGV[1]
			local taskID = ARGV[2]
			local expireTime = ARGV[3]

			-- 检查是否已存在
			local existing = redis.call('zscore', zsetKey, taskID)
			if existing and tonumber(existing) <= tonumber(score) then
				return 0
			end

			-- 设置NX锁避免重复计算
			local lockAcquired = redis.call('set', lockKey, 1, 'NX', 'EX', expireTime)
			if not lockAcquired then
				return 0
			end

			redis.call('zadd', zsetKey, score, taskID)
			return 1
			`

		lockKey := fmt.Sprintf("%s_%s_%d", l.keyPrefix, val.TaskId, nextTime.UnixMilli())
		_, err = pipe.Eval(l.ctx, script, []string{l.zsetKey, lockKey},
			nextTime.UnixMilli(), val.TaskId, 60).Result()
		if err != nil {
			l.logger.Errorf(l.ctx, "Failed to schedule task: %v", err)
		}

		return true
	})

	_, err := pipe.Exec(l.ctx)
	if err != nil {
		l.logger.Errorf(l.ctx, "Cluster Failed to schedule task: %v", err)
	}
}

// moveReadyTasks 移动就绪任务到执行列表
func (c *Cluster) moveReadyTasks() {
	script := `
	local zsetKey = KEYS[1]
	local listKey = KEYS[2]
	local maxTime = ARGV[1]
	local limit = ARGV[2]

	local tasks = redis.call('zrangebyscore', zsetKey, 0, maxTime, 'LIMIT', 0, limit)
	for i, taskID in ipairs(tasks) do
		redis.call('zrem', zsetKey, taskID)
		redis.call('lpush', listKey, taskID)
	end
	return #tasks
	`

	result, err := c.redis.Eval(c.ctx, script, []string{c.zsetKey, c.listKey},
		time.Now().UnixMilli(), 100).Result()
	if err != nil && err != redis.Nil {
		c.logger.Errorf(c.ctx, "Failed to move ready tasks: %v", err)
		return
	}

	if count, ok := result.(int64); ok && count > 0 {
		c.logger.Infof(c.ctx, "Cluster moveReadyTasks Moved %d tasks to ready list", count)
	}
}

// executeTasks 执行任务
func (c *Cluster) executeTasks() {
	defer c.wg.Done()

	for {
		select {
		case <-c.stopChan:
			return
		case <-c.ctx.Done():
			return
		default:
			if c.usePriority && !c.priority.IsLatest(c.ctx) {
				time.Sleep(5 * time.Second)
				continue
			}

			taskID, err := c.redis.BLPop(c.ctx, 10*time.Second, c.listKey).Result()
			if err != nil {
				if err != redis.Nil {
					c.logger.Errorf(c.ctx, "Failed to pop task: %v", err)
					// Redis 异常，休眠一会儿
					time.Sleep(5 * time.Second)
				}
				continue
			}

			if len(taskID) < 2 {
				continue
			}

			go c.processTask(taskID[1])
		}
	}

}

type ReJobData struct {
	TaskId string
	Times  int
}

// 执行任务
func (l *Cluster) processTask(taskId string) {
	begin := time.Now()

	ctx, cancel := context.WithTimeout(l.ctx, l.timeout)
	defer cancel()

	u, _ := uuid.NewV7()

	ctx = context.WithValue(ctx, "trace_id", u.String())

	l.logger.Infof(ctx, "doTask timer begin taskId:%s", taskId)

	// 上报执行情况
	executeVal := fmt.Sprintf("tid:%s|insId:%s|uuid:%s|time:%s", taskId, l.instanceId, u.String(), begin.Format(time.RFC3339Nano))
	l.redis.ZAdd(ctx, l.executeInfoKey, redis.Z{
		Score:  float64(begin.UnixMilli()),
		Member: executeVal,
	})

	val, ok := l.workerList.Load(taskId)
	if !ok {
		l.logger.Errorf(ctx, "doTask timer:任务不存在:%s", taskId)
		return
	}
	t, ok := val.(timerStr)
	if !ok {
		l.logger.Errorf(ctx, "doTask timer:任务不存在:%s", taskId)
		return
	}

	// 这里加一个全局锁
	lock, err := lockx.NewGlobalLock(ctx, l.redis, taskId)
	if err != nil {
		l.logger.Errorf(ctx, "doTask timer:获取锁失败:%s", taskId)
		return
	}
	if b, err := lock.Lock(); !b {
		l.logger.Errorf(ctx, "doTask timer:获取锁失败:%s %+v", taskId, err)
		return
	}
	defer lock.Unlock()

	defer func() {
		if err := recover(); err != nil {
			l.logger.Errorf(ctx, "doTask timer:回调任务panic err:%+v stack:%s", err, string(debug.Stack()))
		}

		l.logger.Infof(ctx, "doTask timer:执行任务耗时:%s %dms", taskId, time.Since(begin).Milliseconds())
	}()

	// 执行任务
	if err := t.Callback(ctx, t.ExtendData); err != nil {
		l.logger.Errorf(ctx, "doTask timer:执行任务失败:%s %+v", taskId, err)
		return
	}

}
