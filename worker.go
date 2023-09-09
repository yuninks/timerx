package timer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// 单次的任务队列
type worker struct {
	ctx     context.Context
	zsetKey string
	listKey string
	redis   *redis.Client
	worker  WorkerInterface
}

type WorkerCode int

const (
	WorkerCodeSuccess WorkerCode = 0
	WorkerCodeAgain   WorkerCode = -1
)

// 需要考虑执行失败重新放入队列的情况
type WorkerInterface interface {
	Worker(uniqueKey string, jobType string, data map[string]interface{}) WorkerCode
}

var wo *worker = nil
var once sync.Once

type extendData struct {
	Delay time.Duration
	Data  map[string]interface{}
}

func InitWorker(ctx context.Context, re *redis.Client, w WorkerInterface) *worker {

	once.Do(func() {
		wo = &worker{
			ctx:     ctx,
			zsetKey: "timer:job_zsetkey",
			listKey: "timer:job_listkey",
			redis:   re,
			worker:  w,
		}
		go wo.getTask()
		go wo.execTask()
	})

	return wo
}

// 添加任务
// 重复插入就代表覆盖
func (w *worker) Add(uniqueKey string, jobType string, delayTime time.Duration, data map[string]interface{}) error {
	if delayTime.Abs() != delayTime {
		return fmt.Errorf("时间间隔不能为负数")
	}
	if delayTime == 0 {
		return fmt.Errorf("时间间隔不能为0")
	}

	redisKey := fmt.Sprintf("%s[:]%s", uniqueKey, jobType)

	ed := extendData{
		Delay: delayTime,
		Data:  data,
	}
	b, _ := json.Marshal(ed)

	_, err := w.redis.SetEX(w.ctx, redisKey, b, delayTime+time.Second*5).Result()
	if err != nil {
		return err
	}

	_, err = w.redis.ZAdd(w.ctx, w.zsetKey, &redis.Z{
		Score:  float64(time.Now().Add(delayTime).UnixMilli()),
		Member: redisKey,
	}).Result()

	return err
}

// 删除任务
func (w *worker) Del(uniqueKey string, jobType string) error {
	redisKey := fmt.Sprintf("%s[:]%s", uniqueKey, jobType)

	w.redis.Del(w.ctx, redisKey).Result()

	w.redis.ZRem(w.ctx, w.zsetKey, redisKey).Result()

	return nil
}

// 获取任务
func (w *worker) getTask() {
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

// 执行任务
func (w *worker) execTask() {
	for {
		keys, err := w.redis.BLPop(w.ctx, time.Second*10, w.listKey).Result()
		if err != nil {
			fmt.Println("watch err:", err)
			continue
		}

		go func() {
			s := strings.Split(keys[1], "[:]")

			// 读取数据
			str, err := w.redis.Get(w.ctx, keys[1]).Result()
			if err != nil {
				fmt.Println("execJob err:", err)
				return
			}
			ed := extendData{}
			json.Unmarshal([]byte(str), &ed)

			fmt.Println("开始时间：", time.Now().Format("2006-01-02 15:04:05"))
			code := w.worker.Worker(s[0], s[1], ed.Data)

			if code == WorkerCodeAgain {
				// 重新放入队列
				fmt.Println("重入时间：", time.Now().Format("2006-01-02 15:04:05"))
				w.Add(s[0], s[1], ed.Delay, ed.Data)
			}
		}()
	}
}
