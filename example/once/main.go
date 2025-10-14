package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yuninks/timerx"
)

var save_path = "./once.log"

const (
	OnceTaskTypeNormal timerx.OnceTaskType = "normal"
	OnceTaskTypeUrgent timerx.OnceTaskType = "urgent"
)

func main() {

	ctx := context.Background()

	client := getRedis()
	once, err := timerx.InitOnce(ctx, client, "once_test", &OnceWorker{}, timerx.WithBatchSize(1000))
	if err != nil {
		panic(err)
	}

	// intervalSaveTime(ctx, once)
	// intervalSave(ctx, once)
	// intervalcreateTask(ctx, once)
	intervalCreateTime(ctx, once)
	// benchmarkJob(ctx, once)

	select {}

}

// 指定时间测试
func intervalSaveTime(ctx context.Context, once *timerx.Once) {

	beginTime := time.Now()

	for i := 1; i < 100; i++ {

		execTime := beginTime.Add(time.Second * time.Duration(i))

		err := once.SaveByTime(ctx, timerx.OnceTaskType("intervalSaveTime"), fmt.Sprintf("intervalSaveTime_task_%d", i), execTime, fmt.Sprintf("任务数据_%d 预期时间%s", i, execTime.Format("2006-01-02 15:04:05")))
		fmt.Println(err)
	}

}

// 间隔测试
func intervalSave(ctx context.Context, once *timerx.Once) {

	beginTime := time.Now()

	for i := 1; i < 100; i++ {

		execTime := beginTime.Add(time.Second * time.Duration(i))

		err := once.Save(ctx, timerx.OnceTaskType("intervalSaveTime"), fmt.Sprintf("intervalSaveTime_task_%d", i), time.Until(execTime), fmt.Sprintf("任务数据_%d 预期时间%s", i, execTime.Format("2006-01-02 15:04:05")))
		fmt.Println(err)
	}

}

// 创建间隔测试
func intervalcreateTask(ctx context.Context, once *timerx.Once) {

	beginTime := time.Now()

	for i := 1; i < 100; i++ {

		execTime := beginTime.Add(time.Second * time.Duration(i))

		err := once.Create(ctx, timerx.OnceTaskType("intervalSaveTime"), fmt.Sprintf("intervalSaveTime_task_%d", i), time.Until(execTime), fmt.Sprintf("任务数据A_%d 预期时间%s", i, execTime.Format("2006-01-02 15:04:05")))
		fmt.Println(err)
		err = once.Create(ctx, timerx.OnceTaskType("intervalSaveTime"), fmt.Sprintf("intervalSaveTime_task_%d", i), time.Until(execTime), fmt.Sprintf("任务数据B_%d 预期时间%s", i, execTime.Format("2006-01-02 15:04:05")))
		fmt.Println(err)
	}

}

func intervalCreateTime(ctx context.Context, once *timerx.Once) {

	beginTime := time.Now()

	for i := 1; i < 100; i++ {

		execTime := beginTime.Add(time.Second * time.Duration(i))

		err := once.CreateByTime(ctx, timerx.OnceTaskType("intervalSaveTime"), fmt.Sprintf("intervalSaveTime_task_%d", i), execTime, fmt.Sprintf("任务数据A_%d 预期时间%s", i, execTime.Format("2006-01-02 15:04:05")))
		fmt.Println(err)
		err = once.CreateByTime(ctx, timerx.OnceTaskType("intervalSaveTime"), fmt.Sprintf("intervalSaveTime_task_%d", i), execTime, fmt.Sprintf("任务数据B_%d 预期时间%s", i, execTime.Format("2006-01-02 15:04:05")))
		fmt.Println(err)
	}

}

// 压力测试
func benchmarkJob(ctx context.Context, once *timerx.Once) {

	t := time.Now()

	ch := make(chan ChanStatus, 1000)

	go func() {
		for a := 0; a < 100; a++ {
			go func(a int) {
				for status := range ch {
					// fmt.Println("协程", a, "处理任务", status)
					// time.Sleep(10 * time.Millisecond) // 模拟处理时间
					err := once.SaveByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("task_%d_%d", status.I, status.J), status.T, fmt.Sprintf("任务数据_%d_%d 预期时间%s", status.I, status.J, status.T.Format("2006-01-02 15:04:05")))
					if err != nil {
						fmt.Println("保存任务失败:", err)
					}
				}
			}(a)
		}
	}()

	go func() {
		// 一千万任务，每个任务间隔1秒

		for i := 0; i < 100; i++ {
			runTime := t.Add(time.Duration(i) * time.Second)
			for j := 0; j < 100; j++ {
				ch <- ChanStatus{
					I: i,
					J: j,
					T: runTime,
				}
			}
		}
	}()
}

type ChanStatus struct {
	I int
	J int
	T time.Time
}

func getRedis() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1" + ":" + "6379",
		Password: "123456", // no password set
		DB:       0,        // use default DB
	})
	if client == nil {
		panic("redis init error")
	}
	return client
}

type OnceWorker struct {
}

func (l *OnceWorker) Worker(ctx context.Context, taskType timerx.OnceTaskType, taskId string, attachData interface{}) *timerx.OnceWorkerResp {
	// 任务处理逻辑
	callback(ctx, attachData)

	return &timerx.OnceWorkerResp{}
}

func callback(ctx context.Context, extendData any) error {

	fmt.Println("任务执行了", extendData, "时间:", time.Now().Format("2006-01-02 15:04:05"))

	// 追加到文件
	file, err := os.OpenFile(save_path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("打开文件失败:", err)
		return err
	}
	defer file.Close()
	_, err = file.WriteString(fmt.Sprintf("执行时间:%v %s\n", extendData, time.Now().Format("2006-01-02 15:04:05")))
	if err != nil {
		fmt.Println("写入文件失败:", err)
		return err
	}

	return nil
}
