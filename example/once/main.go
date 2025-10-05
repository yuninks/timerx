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
	t := time.Now()
	client := getRedis()
	once, err := timerx.InitOnce(ctx, client, "once_test", &OnceWorker{}, timerx.WithBatchSize(1000))
	if err != nil {
		panic(err)
	}

	go func() {
		// 一千万任务，每个任务间隔1秒

		for i := 0; i < 10000000; i++ {
			runTime := t.Add(time.Duration(i) * time.Second)
			for j := 0; j < 50000; j++ {
				once.CreateByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("task_%d_%d", i, j), runTime, fmt.Sprintf("任务数据_%d_%d 预期时间%s", i, j, runTime.Format("2006-01-02 15:04:05")))
			}
		}
	}()

	select {}

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
