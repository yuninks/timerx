package timerx

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	uuid "github.com/satori/go.uuid"
)

// 功能描述
// 1. 任务可以多节点发布
// 2. 每个任务的执行在全局仅会执行一次
// 3. 任务执行失败支持快捷重新加入队列

// 单次的任务队列
type Once struct {
	ctx       context.Context
	logger    Logger
	zsetKey   string
	listKey   string
	redis     redis.UniversalClient
	worker    Callback
	keyPrefix string
}

type OnceWorkerResp struct {
	Retry      bool // 是否重试 true
	DelayTime  time.Duration
	AttachData interface{}
}

// 需要考虑执行失败重新放入队列的情况
type Callback interface {
	// 任务执行
	// @param jobType string 任务类型
	// @param uniTaskId string 任务唯一标识
	// @param data interface{} 任务数据
	// @return WorkerCode 任务执行结果
	// @return time.Duration 任务执行时间间隔
	Worker(ctx context.Context, taskType string, taskId string, attachData interface{}) *OnceWorkerResp
}

var wo *Once = nil
var once sync.Once

type extendData struct {
	Delay time.Duration
	Data  interface{}
}

// 初始化
func InitOnce(ctx context.Context, re redis.UniversalClient, keyPrefix string, call Callback, opts ...Option) *Once {
	if re == nil {
		panic("redis client is nil")
	}

	op := newOptions(opts...)
	once.Do(func() {
		wo = &Once{
			ctx:       ctx,
			logger:    op.logger,
			zsetKey:   "timer:once_zsetkey" + keyPrefix,
			listKey:   "timer:once_listkey" + keyPrefix,
			redis:     re,
			worker:    call,
			keyPrefix: keyPrefix,
		}
		go wo.getTask()
		go wo.watch()
	})

	return wo
}

// 添加任务(覆盖)
// 重复插入就代表覆盖
// @param jobType string 任务类型
// @param uniTaskId string 任务唯一标识
// @param delayTime time.Duration 延迟时间
// @param attachData interface{} 附加数据
func (w *Once) Save(taskType string, taskId string, delayTime time.Duration, attachData interface{}) error {
	if delayTime.Abs() != delayTime {
		return fmt.Errorf("时间间隔不能为负数")
	}
	if delayTime == 0 {
		return fmt.Errorf("时间间隔不能为0")
	}

	redisKey := fmt.Sprintf("%s[:]%s", taskType, taskId)

	ed := extendData{
		Delay: delayTime,
		Data:  attachData,
	}
	b, _ := json.Marshal(ed)

	// 写入附加数据
	_, err := w.redis.SetEX(w.ctx, w.keyPrefix+redisKey, b, delayTime+time.Second*5).Result()
	if err != nil {
		return err
	}

	// 写入执行时间
	_, err = w.redis.ZAdd(w.ctx, w.zsetKey, &redis.Z{
		Score:  float64(time.Now().Add(delayTime).UnixMilli()),
		Member: redisKey,
	}).Result()

	return err
}

// 添加任务(不覆盖)
func (l *Once) Create(taskType string, taskId string, delayTime time.Duration, attachData interface{}) error {
	redisKey := fmt.Sprintf("%s[:]%s", taskType, taskId)
	// 判断有序集合Key是否存在，存在则报错，不存在则写入
	if l.redis.ZScore(l.ctx, l.zsetKey, redisKey).Val() == 0 {

		ed := extendData{
			Delay: delayTime,
			Data:  attachData,
		}
		b, _ := json.Marshal(ed)

		// 写入附加数据
		_, err := l.redis.SetEX(l.ctx, l.keyPrefix+redisKey, b, delayTime+time.Second*5).Result()
		if err != nil {
			return err
		}
		_, err = l.redis.ZAdd(l.ctx, l.zsetKey, &redis.Z{
			Score:  float64(time.Now().Add(delayTime).UnixMilli()),
			Member: redisKey,
		}).Result()
		if err != nil {
			return err
		}
	}

	return nil
}

// 删除任务
func (w *Once) Delete(taskType string, taskId string) error {
	redisKey := fmt.Sprintf("%s[:]%s", taskType, taskId)

	w.redis.Del(w.ctx, redisKey).Result()

	w.redis.ZRem(w.ctx, w.zsetKey, redisKey).Result()

	return nil
}

// 获取任务
func (l *Once) Get(taskType string, taskId string) {
	//
}

// 获取任务
func (w *Once) getTask() {
	timer := time.NewTicker(time.Millisecond * 200)
	defer timer.Stop()

Loop:
	for {
		select {
		case <-timer.C:
			script := `
				local token = redis.call('zrangebyscore',KEYS[1],ARGV[1],ARGV[2])
				for i,v in ipairs(token) do
					redis.call('zrem',KEYS[1],v)
					redis.call('lpush',KEYS[2],v)
				end
				return "OK"
				`
			w.redis.Eval(w.ctx, script, []string{w.zsetKey, w.listKey}, 0, time.Now().UnixMilli()).Result()
			// fmt.Println(i, err)

		case <-w.ctx.Done():
			break Loop
		}
	}
}

// 监听任务
func (w *Once) watch() {
	for {
		keys, err := w.redis.BLPop(w.ctx, time.Second*10, w.listKey).Result()
		if err != nil {
			// fmt.Println("watch err:", err)
			continue
		}

		ctx := context.WithValue(w.ctx, "trace_id", uuid.NewV4().String())

		go w.doTask(ctx, keys[1])

	}
}

// 执行任务
func (l *Once) doTask(ctx context.Context, key string) {
	fmt.Println("任务时间:", time.Now().Format("2006-01-02 15:04:05"))
	defer func() {
		if err := recover(); err != nil {
			l.logger.Errorf(ctx, "timer:回调任务panic:%s stack:%s", err, string(debug.Stack()))
		}
	}()

	s := strings.Split(key, "[:]")

	// 读取数据
	redisKey := l.keyPrefix + key
	str, err := l.redis.Get(ctx, redisKey).Result()
	if err != nil {
		l.logger.Errorf(ctx, "获取数据失败 err:%s", err)
		return
	}

	fmt.Println("参数：", str)

	ed := extendData{}
	json.Unmarshal([]byte(str), &ed)

	resp := l.worker.Worker(ctx, s[0], s[1], ed.Data)
	if resp == nil {
		return
	}

	if resp.Retry {
		// 重新放入队列
		if resp.DelayTime != 0 && resp.DelayTime == resp.DelayTime.Abs() {
			ed.Delay = resp.DelayTime
		}
		ed.Data = resp.AttachData
		l.logger.Infof(ctx, "任务重新放入队列:%s", key)
		fmt.Println("重入时间:", time.Now().Format("2006-01-02 15:04:05"))
		l.Create(s[0], s[1], ed.Delay, ed.Data)
	}
}
