package timerx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	uuid "github.com/satori/go.uuid"
	"github.com/yuninks/cachex"
	"github.com/yuninks/lockx"
	"github.com/yuninks/timerx/logger"
	"github.com/yuninks/timerx/priority"
)

// 功能描述

// 这是基于Redis的定时任务调度器，能够有效的在服务集群里面调度任务，避免了单点压力过高或单点故障问题
// 由于所有的服务代码是一致的，也就是一个定时任务将在所有的服务都有注册，具体调度到哪个服务运行看调度结果

// 暂不支持删除定时器，因为这个定时器的设计是基于全局的，如果删除了，那么其他服务就不知道了

// 单例模式
// var clusterOnceLimit sync.Once

// 已注册的任务列表
var clusterWorkerList sync.Map

type Cluster struct {
	ctx       context.Context
	redis     redis.UniversalClient
	cache     *cachex.Cache
	timeout   time.Duration
	logger    logger.Logger
	keyPrefix string         // key前缀
	location  *time.Location // 根据时区计算的时间

	lockKey string // 全局计算的key
	zsetKey string // 有序集合的key
	listKey string // 可执行的任务列表的key
	setKey  string // 重入集合的key

	priority    *priority.Priority // 全局优先级
	priorityKey string             // 全局优先级的key
	usePriority bool
}

// var clu *Cluster = nil

// 初始化定时器
// 全局只需要初始化一次
func InitCluster(ctx context.Context, red redis.UniversalClient, keyPrefix string, opts ...Option) *Cluster {

	// clusterOnceLimit.Do(func() {
	op := newOptions(opts...)

	clu := &Cluster{
		ctx:         ctx,
		redis:       red,
		cache:       cachex.NewCache(),
		timeout:     op.timeout,
		logger:      op.logger,
		keyPrefix:   keyPrefix,
		location:    op.location,
		lockKey:     "timer:cluster_globalLockKey" + keyPrefix, // 定时器的全局锁
		zsetKey:     "timer:cluster_zsetKey" + keyPrefix,       // 有序集合
		listKey:     "timer:cluster_listKey" + keyPrefix,       // 列表
		setKey:      "timer:cluster_setKey" + keyPrefix,        // 重入集合
		priorityKey: "timer:cluster_priorityKey" + keyPrefix,   // 全局优先级的key
		usePriority: op.usePriority,
	}

	// 初始化优先级

	if clu.usePriority {
		clu.priority = priority.InitPriority(ctx, red, clu.priorityKey, op.priorityVal, priority.SetLogger(clu.logger))
	}

	// 监听任务
	go clu.watch()

	timer := time.NewTicker(time.Millisecond * 200)

	go func(ctx context.Context) {
	Loop:
		for {
			select {
			case <-timer.C:
				if clu.usePriority {
					if !clu.priority.IsLatest(ctx) {
						continue
					}
				}

				clu.getTask()
				clu.getNextTime()
			case <-ctx.Done():
				break Loop
			}
		}
	}(ctx)

	return clu
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
	nowTime := time.Now().In(c.location)

	jobData := JobData{
		JobType:    JobTypeEveryMonth,
		CreateTime: nowTime,
		Day:        day,
		Hour:       hour,
		Minute:     minute,
		Second:     second,
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
	nowTime := time.Now().In(c.location)

	jobData := JobData{
		JobType:    JobTypeEveryWeek,
		CreateTime: nowTime,
		Weekday:    week,
		Hour:       hour,
		Minute:     minute,
		Second:     second,
	}

	return c.addJob(ctx, taskId, jobData, callback, extendData)
}

// 每天执行一次
func (c *Cluster) EveryDay(ctx context.Context, taskId string, hour int, minute int, second int, callback func(ctx context.Context, extendData interface{}) error, extendData interface{}) error {
	nowTime := time.Now().In(c.location)

	jobData := JobData{
		JobType:    JobTypeEveryDay,
		CreateTime: nowTime,
		Hour:       hour,
		Minute:     minute,
		Second:     second,
	}

	return c.addJob(ctx, taskId, jobData, callback, extendData)
}

// 每小时执行一次
func (c *Cluster) EveryHour(ctx context.Context, taskId string, minute int, second int, callback func(ctx context.Context, extendData interface{}) error, extendData interface{}) error {
	nowTime := time.Now().In(c.location)

	jobData := JobData{
		JobType:    JobTypeEveryHour,
		CreateTime: nowTime,
		Minute:     minute,
		Second:     second,
	}

	return c.addJob(ctx, taskId, jobData, callback, extendData)
}

// 每分钟执行一次
func (c *Cluster) EveryMinute(ctx context.Context, taskId string, second int, callback func(ctx context.Context, extendData interface{}) error, extendData interface{}) error {
	nowTime := time.Now().In(c.location)

	jobData := JobData{
		JobType:    JobTypeEveryMinute,
		CreateTime: nowTime,
		Second:     second,
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
		CreateTime:   nowTime,
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
func (c *Cluster) addJob(ctx context.Context, taskId string, jobData JobData, callback func(ctx context.Context, extendData interface{}) error, extendData interface{}) error {
	_, ok := clusterWorkerList.Load(taskId)
	if ok {
		c.logger.Errorf(ctx, "key已存在:%s", taskId)
		return errors.New("key已存在")
	}

	_, err := GetNextTime(time.Now().In(c.location), jobData)
	if err != nil {
		c.logger.Errorf(ctx, "获取下次执行时间失败:%s", err.Error())
		return err
	}

	// ctx, cancel := context.WithCancel(ctx)
	// defer cancel()

	// lock := lockx.NewGlobalLock(ctx, c.redis, taskId)
	// tB := lock.Try(2)
	// if !tB {
	// 	c.logger.Errorf(ctx, "添加失败:%s", taskId)
	// 	return errors.New("添加失败")
	// }
	// defer lock.Unlock()

	t := timerStr{
		Callback:   callback,
		ExtendData: extendData,
		TaskId:     taskId,
		JobData:    &jobData,
	}

	clusterWorkerList.Store(taskId, t)

	return err
}

// 计算下一次执行的时间
// TODO:注册的任务需放在Redis集中存储，因为本地的话，如果有多个服务，那么就会出现不一致的情况。但是要注意服务如何进行下线，由于是主动上报的，需要有一个机制进行删除过期的任务（添加任务&定时器轮训注册）
// TODO:考虑不同实例系统时间不一样，可能计算的下次时间不一致，会有重复执行的可能
func (c *Cluster) getNextTime() {

	lock := lockx.NewGlobalLock(c.ctx, c.redis, c.lockKey)
	// 获取锁
	lockBool := lock.Lock()
	if !lockBool {
		// log.Println("timer:获取锁失败")
		return
	}
	defer lock.Unlock()

	// 计算下一次时间

	// p := c.redis.Pipeline()

	// 根据内部注册的任务列表计算下一次执行的时间
	clusterWorkerList.Range(func(key, value interface{}) bool {
		val := value.(timerStr)

		nextTime, _ := GetNextTime(time.Now().In(c.location), *val.JobData)

		// fmt.Println(val.ExtendData, val.JobData, nextTime)

		// 内部判定是否重复
		cacheKey := fmt.Sprintf("%s_%s_%d", c.keyPrefix, val.TaskId, nextTime.UnixMilli())
		cacheVal, err := c.cache.Get(cacheKey)
		if err == nil {
			// 缓存已有值
			return true
		}
		valueNum := int(0)
		if cacheVal != nil {
			valueNum = cacheVal.(int)
		}
		if valueNum > 2 {
			// 重试2次还是失败就不执行了
			return true
		}
		// fmt.Println("计算时间1", val.ExtendData, time.UnixMilli(nextTime.UnixMilli()).Format("2006-01-02 15:04:05"))

		// redis lua脚本，尝试设置nx锁时间为一分钟，如果能设置进去则添加到有序集合zsetKey
		script := `
		local zsetKey = KEYS[1]

		local cacheKey = ARGV[1]
		local expireTime = ARGV[2]
	
		local score = ARGV[3]
		local member = ARGV[4]
	
		local res = redis.call('set', cacheKey, '', 'nx', 'ex', expireTime)
	
		if res then
			redis.call('zadd', zsetKey, score, member)
			return "SUCCESS"
		end
		return "ERROR"
		`

		// TODO:
		expireTime := time.Minute

		res, err := c.redis.Eval(c.ctx, script, []string{c.zsetKey}, cacheKey, expireTime.Seconds(), nextTime.UnixMilli(), val.TaskId).Result()

		valueNum++

		if err == nil && res.(string) == "SUCCESS" {
			// 设置成功
			valueNum = 10

			// fmt.Println("计算时间2", val.ExtendData, time.UnixMilli(nextTime.UnixMilli()).Format("2006-01-02 15:04:05"))
		}

		c.cache.Set(cacheKey, valueNum, expireTime)

		return true
	})

	// _, err := p.Exec(ctx)
	// _ = err
}

// 获取任务
func (c *Cluster) getTask() {
	// 定时去Redis获取任务
	script := `
	local token = redis.call('zrangebyscore',KEYS[1],ARGV[1],ARGV[2])
	for i,v in ipairs(token) do
		redis.call('zrem',KEYS[1],v)
		redis.call('lpush',KEYS[2],v)
	end
	return "OK"
	`
	c.redis.Eval(c.ctx, script, []string{c.zsetKey, c.listKey}, 0, time.Now().UnixMilli()).Result()

}

// 监听任务
func (c *Cluster) watch() {
	// 执行任务
	go func() {
		for {
			if c.usePriority {
				if !c.priority.IsLatest(c.ctx) {
					// 如果全局优先级不满足就不执行
					time.Sleep(time.Second * 5)
					continue
				}
			}

			keys, err := c.redis.BLPop(c.ctx, time.Second*10, c.listKey).Result()
			if err != nil {
				if err != redis.Nil {
					c.logger.Errorf(c.ctx, "BLPop watch err:%+v", err)
				}
				continue
			}
			_, ok := clusterWorkerList.Load(keys[1])
			if !ok {
				c.logger.Errorf(c.ctx, "watch timer:任务不存在%+v", keys[1])

				rd := ReJobData{
					TaskId: keys[1],
					Times:  1,
				}
				rdb, _ := json.Marshal(rd)

				c.redis.SAdd(c.ctx, c.setKey, string(rdb))
				continue
			}
			go c.doTask(c.ctx, keys[1])
		}
	}()

	// 处理重入任务
	go func() {
		for {
			if c.usePriority {
				if !c.priority.IsLatest(c.ctx) {
					// 如果全局优先级不满足就不执行
					time.Sleep(time.Second * 5)
					continue
				}
			}

			res, err := c.redis.SPop(c.ctx, c.setKey).Result()
			if err != nil {
				if err == redis.Nil {
					// 已经是空了就不要浪费资源了
					time.Sleep(time.Second)
				} else {
					c.logger.Errorf(c.ctx, "SPop watch err:%+v", err)
				}
				continue
			}

			var rd ReJobData
			err = json.Unmarshal([]byte(res), &rd)
			if err != nil {
				c.logger.Errorf(c.ctx, "json.Unmarshal err:%+v", err)
				continue
			}

			_, ok := clusterWorkerList.Load(rd.TaskId)
			if !ok {
				c.logger.Errorf(c.ctx, "watch timer:任务不存在%+v", rd.TaskId)

				if rd.Times >= 3 {
					// 重试3次还是失败就不执行了
					continue
				}
				rd.Times++

				rdb, _ := json.Marshal(rd)

				c.redis.SAdd(c.ctx, c.setKey, string(rdb))
				continue
			}
			go c.doTask(c.ctx, rd.TaskId)
		}
	}()

}

type ReJobData struct {
	TaskId string
	Times  int
}

// 执行任务
func (c *Cluster) doTask(ctx context.Context, taskId string) {

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	val, ok := clusterWorkerList.Load(taskId)
	if !ok {
		c.logger.Errorf(ctx, "doTask timer:任务不存在:%s", taskId)
		return
	}
	t,ok := val.(timerStr)
	if !ok {
		c.logger.Errorf(ctx, "doTask timer:任务不存在:%s", taskId)
		return
	}


	// 这里加一个全局锁
	lock := lockx.NewGlobalLock(ctx, c.redis, taskId)
	tB := lock.Lock()
	if !tB {
		c.logger.Errorf(ctx, "doTask timer:获取锁失败:%s", taskId)
		return
	}
	defer lock.Unlock()

	defer func() {
		if err := recover(); err != nil {
			c.logger.Errorf(ctx, "timer:回调任务panic err:%+v stack:%s", err, string(debug.Stack()))
		}
	}()

	ctx = context.WithValue(ctx, "trace_id", uuid.NewV4().String())

	// 执行任务
	t.Callback(ctx, t.ExtendData)
}
