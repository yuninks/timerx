package timerx

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
	ctx     context.Context
	logger  Logger
	zsetKey string
	listKey string
	redis   redis.UniversalClient
	worker  Callback
}

type WorkerCode int

const (
	WorkerCodeSuccess WorkerCode = 0  // 处理完成(不需要重入)
	WorkerCodeAgain   WorkerCode = -1 // 需要继续定时,默认原来的时间
)

// 需要考虑执行失败重新放入队列的情况
type Callback interface {
	// 任务执行
	// @param jobType string 任务类型
	// @param uniTaskId string 任务唯一标识
	// @param data interface{} 任务数据
	// @return WorkerCode 任务执行结果
	// @return time.Duration 任务执行时间间隔
	Worker(ctx context.Context, jobType string, uniTaskId string, attachData interface{}) (WorkerCode, time.Duration)
}

var wo *Once = nil
var once sync.Once

type extendData struct {
	Delay time.Duration
	Data  interface{}
}

// 初始化
func InitOnce(ctx context.Context, re *redis.Client, keyPrefix string, call Callback, opts ...Option) *Once {
	op := newOptions(opts...)
	once.Do(func() {
		wo = &Once{
			ctx:     ctx,
			logger:  op.logger,
			zsetKey: "timer:once_zsetkey" + keyPrefix,
			listKey: "timer:once_listkey" + keyPrefix,
			redis:   re,
			worker:  call,
		}
		go wo.getTask()
		go wo.watch()
	})

	return wo
}

// 添加任务
// 重复插入就代表覆盖
// @param jobType string 任务类型
// @param uniTaskId string 任务唯一标识
// @param delayTime time.Duration 延迟时间
// @param attachData interface{} 附加数据
func (w *Once) Add(jobType string, uniTaskId string, delayTime time.Duration, attachData interface{}) error {
	if delayTime.Abs() != delayTime {
		return fmt.Errorf("时间间隔不能为负数")
	}
	if delayTime == 0 {
		return fmt.Errorf("时间间隔不能为0")
	}

	redisKey := fmt.Sprintf("%s[:]%s", jobType, uniTaskId)

	ed := extendData{
		Delay: delayTime,
		Data:  attachData,
	}
	b, _ := json.Marshal(ed)

	// 写入附加数据
	_, err := w.redis.SetEX(w.ctx, redisKey, b, delayTime+time.Second*5).Result()
	if err != nil {
		return err
	}

	// 吸入执行时间
	_, err = w.redis.ZAdd(w.ctx, w.zsetKey, &redis.Z{
		Score:  float64(time.Now().Add(delayTime).UnixMilli()),
		Member: redisKey,
	}).Result()

	return err
}

// 删除任务
func (w *Once) Del(jobType string, uniTaskId string) error {
	redisKey := fmt.Sprintf("%s[:]%s", jobType, uniTaskId)

	w.redis.Del(w.ctx, redisKey).Result()

	w.redis.ZRem(w.ctx, w.zsetKey, redisKey).Result()

	return nil
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
			fmt.Println("watch err:", err)
			continue
		}

		go w.doTask(keys[1])

	}
}

// 执行任务
func (w *Once) doTask(key string) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("timer:回调任务panic", err)
			log.Println("errStack", string(debug.Stack()))
		}
	}()

	s := strings.Split(key, "[:]")

	// 读取数据
	str, err := w.redis.Get(w.ctx, key).Result()
	if err != nil {
		fmt.Println("execJob err:", err)
		return
	}
	ed := extendData{}
	json.Unmarshal([]byte(str), &ed)

	fmt.Println("开始时间：", time.Now().Format("2006-01-02 15:04:05"))

	ctx := context.WithValue(context.Background(), "trace_id", uuid.NewV4().String)

	code, t := w.worker.Worker(ctx, s[0], s[1], ed.Data)

	if code == WorkerCodeAgain {
		// 重新放入队列
		fmt.Println("重入时间：", time.Now().Format("2006-01-02 15:04:05"))
		if t != 0 && t == t.Abs() {
			ed.Delay = t
		}
		w.Add(s[0], s[1], ed.Delay, ed.Data)
	}
}
