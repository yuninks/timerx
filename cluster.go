package timerx

import (
	"context"
	"encoding/json"
	"errors"
	"runtime/debug"
	"sync"
	"time"

	"code.yun.ink/pkg/lockx"
	"github.com/go-redis/redis/v8"
)

// 功能描述

// 这是基于Redis的定时任务调度器，能够有效的在服务集群里面调度任务，避免了单点压力过高或单点故障问题
// 由于所有的服务代码是一致的，也就是一个定时任务将在所有的服务都有注册，具体调度到哪个服务运行看调度结果

// 暂不支持删除定时器，因为这个定时器的设计是基于全局的，如果删除了，那么其他服务就不知道了

// 单例模式
var clusterOnceLimit sync.Once

// 已注册的任务列表
var clusterWorkerList sync.Map

type Cluster struct {
	ctx     context.Context
	redis   *redis.Client
	logger  Logger
	lockKey string // 全局计算的key
	nextKey string // 下一次执行的key
	zsetKey string // 有序集合的key
	listKey string // 可执行的任务列表的key
	setKey  string // 重入集合的key
}

var clu *Cluster = nil

// 初始化定时器
// 全局只需要初始化一次
func InitCluster(ctx context.Context, red *redis.Client, keyPrefix string, opts ...Option) *Cluster {
	op := newOptions(opts...)

	clusterOnceLimit.Do(func() {
		clu = &Cluster{
			ctx:     ctx,
			redis:   red,
			logger:  op.logger,
			lockKey: keyPrefix + "timer:cluster_globalLockKey", // 定时器的全局锁
			nextKey: keyPrefix + "timer:cluster_nextKey",       // 下一次
			zsetKey: keyPrefix + "timer:cluster_zsetKey",       // 有序集合
			listKey: keyPrefix + "timer:cluster_listKey",       // 列表
			setKey:  keyPrefix + "timer:cluster_setKey",        // 重入集合
		}

		// 监听任务
		go clu.watch()

		timer := time.NewTicker(time.Millisecond * 200)

		go func(ctx context.Context, red *redis.Client) {
		Loop:
			for {
				select {
				case <-timer.C:
					clu.getTask()
					clu.getNextTime()
				case <-ctx.Done():
					break Loop
				}
			}
		}(ctx, red)
	})
	return clu
}

// TODO:指定执行时间
// 1.每月的1号2点执行（如果当月没有这个号就不执行）
// 2.每周的周一2点执行
// 3.每天的2点执行
// 4.每小时的2分执行
// 5.每分钟的2秒执行
func (c *Cluster) AddEveryMonth(ctx context.Context, taskId string, day int, hour int, minute int, second int, callback callback, extendData interface{}) error {
	nowTime := time.Now()
	// 计算下一次执行的时间
	nextTime := time.Date(nowTime.Year(), nowTime.Month(), day, hour, minute, second, 0, nowTime.Location())
	if nextTime.Before(nowTime) {
		nextTime = nextTime.AddDate(0, 1, 0)
	}
	return c.addJob(ctx, taskId, nextTime, time.Hour*24*30, callback, extendData, JobTypeEveryMonth, &JobData{Day: &day, Hour: &hour, Minute: &minute, Second: &second})
}

func (c *Cluster) AddEveryWeek(ctx context.Context, taskId string, week time.Weekday, hour int, minute int, second int, callback callback, extendData interface{}) error {
	nowTime := time.Now()
	// 计算下一次执行的时间
	nextTime := time.Date(nowTime.Year(), nowTime.Month(), nowTime.Day(), hour, minute, second, 0, nowTime.Location())
	for nextTime.Weekday() != week {
		nextTime = nextTime.AddDate(0, 0, 1)
	}
	if nextTime.Before(nowTime) {
		nextTime = nextTime.AddDate(0, 0, 7)
	}
	return c.addJob(ctx, taskId, nextTime, time.Hour*24*7, callback, extendData, JobTypeInterval, nil)
}

func (c *Cluster) AddEveryDay(ctx context.Context, taskId string, hour int, minute int, second int, callback callback, extendData interface{}) error {
	nowTime := time.Now()
	// 计算下一次执行的时间
	nextTime := time.Date(nowTime.Year(), nowTime.Month(), nowTime.Day(), hour, minute, second, 0, nowTime.Location())
	if nextTime.Before(nowTime) {
		nextTime = nextTime.AddDate(0, 0, 1)
	}
	return c.addJob(ctx, taskId, nextTime, time.Hour*24, callback, extendData, JobTypeInterval, nil)
}

func (c *Cluster) AddEveryHour(ctx context.Context, taskId string, minute int, second int, callback callback, extendData interface{}) error {
	nowTime := time.Now()
	// 计算下一次执行的时间
	nextTime := time.Date(nowTime.Year(), nowTime.Month(), nowTime.Day(), nowTime.Hour(), minute, second, 0, nowTime.Location())
	if nextTime.Before(nowTime) {
		nextTime = nextTime.Add(time.Hour)
	}
	return c.addJob(ctx, taskId, nextTime, time.Hour, callback, extendData, JobTypeInterval, nil)
}

func (c *Cluster) AddEveryMinute(ctx context.Context, taskId string, second int, callback callback, extendData interface{}) error {
	nowTime := time.Now()
	// 计算下一次执行的时间
	nextTime := time.Date(nowTime.Year(), nowTime.Month(), nowTime.Day(), nowTime.Hour(), nowTime.Minute(), second, 0, nowTime.Location())
	if nextTime.Before(nowTime) {
		nextTime = nextTime.Add(time.Minute)
	}
	return c.addJob(ctx, taskId, nextTime, time.Minute, callback, extendData, JobTypeInterval, nil)
}

func (c *Cluster) Add(ctx context.Context, taskId string, spaceTime time.Duration, callback callback, extendData interface{}) error {
	return c.addJob(ctx, taskId, time.Now(), spaceTime, callback, extendData, JobTypeInterval, nil)
}

// 指定时间间隔
// TODO:
// 1.不同服务定的时间间隔不一致问题
// 2.后起的服务计算了时间覆盖前面原有的时间问题
func (c *Cluster) addJob(ctx context.Context, taskId string, beginTime time.Time, spaceTime time.Duration, callback callback, extendData interface{}, jobType JobType, jobData *JobData) error {
	_, ok := clusterWorkerList.Load(taskId)
	if ok {
		c.logger.Errorf(ctx, "key已存在:%s", taskId)
		return errors.New("key已存在")
	}

	if spaceTime != spaceTime.Abs() {
		c.logger.Errorf(ctx, "时间间隔不能为负数:%s", taskId)
		return errors.New("时间间隔不能为负数")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	lock := lockx.NewGlobalLock(ctx, c.redis, taskId)
	tB := lock.Try(10)
	if !tB {
		c.logger.Errorf(ctx, "添加失败:%s", taskId)
		return errors.New("添加失败")
	}
	defer lock.Unlock()

	t := timerStr{
		BeginTime:  beginTime,
		NextTime:   beginTime,
		SpaceTime:  spaceTime,
		Callback:   callback,
		ExtendData: extendData,
		TaskId:     taskId,
		JobType:    jobType,
		JobData:    jobData,
	}

	clusterWorkerList.Store(taskId, t)

	cacheStr, _ := c.redis.Get(ctx, c.nextKey).Result()
	execTime := make(map[string]time.Time)
	json.Unmarshal([]byte(cacheStr), &execTime)

	p := c.redis.Pipeline()

	p.ZAdd(ctx, c.zsetKey, &redis.Z{
		Score:  float64(nextTime.UnixMilli()),
		Member: taskId,
	})
	execTime[taskId] = nextTime
	n, _ := json.Marshal(execTime)
	// fmt.Println("execTime:", execTime, string(n))
	p.Set(ctx, c.nextKey, string(n), 0)

	_, err := p.Exec(ctx)

	// fmt.Println("添加", err)

	return err
}

// 计算下一次执行的时间
// TODO:注册的任务需放在Redis集中存储，因为本地的话，如果有多个服务，那么就会出现不一致的情况。但是要注意服务如何进行下线，由于是主动上报的，需要有一个机制进行删除过期的任务（添加任务&定时器轮训注册）
func (c *Cluster) getNextTime() {

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	lock := lockx.NewGlobalLock(ctx, c.redis, c.lockKey)
	// 获取锁
	lockBool := lock.Lock()
	if !lockBool {
		// log.Println("timer:获取锁失败")
		return
	}
	defer lock.Unlock()

	// 计算下一次时间

	// 读取执行的缓存
	cacheStr, _ := c.redis.Get(ctx, c.nextKey).Result()
	execTime := make(map[string]time.Time)
	json.Unmarshal([]byte(cacheStr), &execTime)

	p := c.redis.Pipeline()

	nowTime := time.Now()

	clusterWorkerList.Range(func(key, value interface{}) bool {
		val := value.(timerStr)
		beforeTime := execTime[val.TaskId]
		if beforeTime.After(nowTime) {
			return true
		}
		nextTime := getNextExecTime(val)
		execTime[val.TaskId] = nextTime

		p.ZAdd(ctx, c.zsetKey, &redis.Z{
			Score:  float64(nextTime.UnixMilli()),
			Member: val.TaskId,
		})
		// log.Println("computeTime add", c.zsetKey, val.taskId, nextTime.UnixMilli())
		return true
	})

	// 更新缓存
	b, _ := json.Marshal(execTime)
	p.Set(ctx, c.nextKey, string(b), 0)

	_, err := p.Exec(ctx)
	_ = err
}

// 递归遍历获取执行时间
// TODO:需要根据不同的任务类型计算下次定时时间
func getNextExecTime(ts timerStr) time.Time {
	nowTime := time.Now()
	if ts.NextTime.After(nowTime) {
		return ts.NextTime
	}
	nextTime := ts.NextTime.Add(ts.SpaceTime)
	ts.NextTime = nextTime
	if nextTime.Before(nowTime) {
		nextTime = getNextExecTime(ts)
	}
	return nextTime
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
			keys, err := c.redis.BLPop(c.ctx, time.Second*10, c.listKey).Result()
			if err != nil {
				if err != redis.Nil {
					c.logger.Errorf(c.ctx, "BLPop watch err:%+v", err)
				}
				continue
			}
			_, ok := clusterWorkerList.Load(keys[1])
			if !ok {
				c.logger.Errorf(c.ctx, "watch timer:任务不存在", keys[1])
				c.redis.SAdd(c.ctx, c.setKey, keys[1])
				continue
			}
			go c.doTask(c.ctx, c.redis, keys[1])
		}
	}()

	go func() {
		for {
			taskId, err := c.redis.SPop(c.ctx, c.setKey).Result()
			if err != nil {
				if err == redis.Nil {
					// 已经是空了就不要浪费资源了
					time.Sleep(time.Second)
				} else {
					c.logger.Errorf(c.ctx, "SPop watch err:%+v", err)
				}
				continue
			}
			_, ok := clusterWorkerList.Load(taskId)
			if !ok {
				c.logger.Errorf(c.ctx, "watch timer:任务不存在", taskId)
				c.redis.SAdd(c.ctx, c.setKey, taskId)
				continue
			}
			go c.doTask(c.ctx, c.redis, taskId)
		}
	}()

}

// 执行任务
func (c *Cluster) doTask(ctx context.Context, red *redis.Client, taskId string) {

	defer func() {
		if err := recover(); err != nil {
			c.logger.Errorf(ctx, "timer:定时器出错 err:%+v stack:%s", err, string(debug.Stack()))
		}
	}()

	val, ok := clusterWorkerList.Load(taskId)
	if !ok {
		c.logger.Errorf(ctx, "doTask timer:任务不存在", taskId)
		return
	}
	t := val.(timerStr)

	// 这里加一个全局锁
	lock := lockx.NewGlobalLock(ctx, red, taskId)
	tB := lock.Lock()
	if !tB {
		c.logger.Errorf(ctx, "doTask timer:获取锁失败", taskId)
		return
	}
	defer lock.Unlock()

	// 执行任务
	t.Callback(ctx, t.ExtendData)
}
