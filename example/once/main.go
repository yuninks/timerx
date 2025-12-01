package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
	once, err := timerx.InitOnce(ctx, client, "once_01", &OnceWorker{}, timerx.WithBatchSize(1000))
	if err != nil {
		panic(err)
	}

	// intervalSaveTime(ctx, once)
	// intervalSave(ctx, once)
	// intervalcreateTask(ctx, once)
	// intervalCreateTime(ctx, once)
	// benchmarkJob(ctx, once)
	stabilityTest(ctx, once)

	select {}

}

// 稳定性测试
func stabilityTest(ctx context.Context, once *timerx.Once) {

	timer := time.NewTicker(time.Second)

	for {
		select {
		case t := <-timer.C:
			fmt.Println("time:", t)

			str := t.Format("2006-01-02 15:04:05")

			once.Create(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_0_%d", time.Now().Unix()), 0, "Create 间隔0s ["+str+"]")
			once.Create(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_1_%d", time.Now().Unix()), time.Second*1, "Create 间隔1s ["+str+"]")
			once.Create(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_10_%d", time.Now().Unix()), time.Second*10, "Create 间隔10s ["+str+"]")
			once.Create(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_15_%d", time.Now().Unix()), time.Second*15, "Create 间隔15s ["+str+"]")
			once.Create(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_30_%d", time.Now().Unix()), time.Second*30, "Create 间隔30s ["+str+"]")
			once.Create(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_60_%d", time.Now().Unix()), time.Second*60, "Create 间隔60s ["+str+"]")
			once.Create(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_120_%d", time.Now().Unix()), time.Second*120, "Create 间隔120s ["+str+"]")    // 2分钟
			once.Create(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_300_%d", time.Now().Unix()), time.Second*300, "Create 间隔300s ["+str+"]")    // 5分钟
			once.Create(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_900_%d", time.Now().Unix()), time.Second*900, "Create 间隔900s ["+str+"]")    // 15分钟
			once.Create(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_1800_%d", time.Now().Unix()), time.Second*1800, "Create 间隔1800s ["+str+"]") // 30分钟

			once.Save(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_0_%d", time.Now().Unix()), 0, "Save 间隔0s ["+str+"]")
			once.Save(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_1_%d", time.Now().Unix()), time.Second*1, "Save 间隔1s ["+str+"]")
			once.Save(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_10_%d", time.Now().Unix()), time.Second*10, "Save 间隔10s ["+str+"]")
			once.Save(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_15_%d", time.Now().Unix()), time.Second*15, "Save 间隔15s ["+str+"]")
			once.Save(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_30_%d", time.Now().Unix()), time.Second*30, "Save 间隔30s ["+str+"]")
			once.Save(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_60_%d", time.Now().Unix()), time.Second*60, "Save 间隔60s ["+str+"]")
			once.Save(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_120_%d", time.Now().Unix()), time.Second*120, "Save 间隔120s ["+str+"]")    // 2分钟
			once.Save(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_300_%d", time.Now().Unix()), time.Second*300, "Save 间隔300s ["+str+"]")    // 5分钟
			once.Save(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_900_%d", time.Now().Unix()), time.Second*900, "Save 间隔900s ["+str+"]")    // 15分钟
			once.Save(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_1800_%d", time.Now().Unix()), time.Second*1800, "Save 间隔1800s ["+str+"]") // 30分钟

			once.CreateByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_by_0_%d", time.Now().Unix()), time.Now(), "CreateByTime 当前时间 ["+str+"]")
			once.CreateByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_by_1_%d", time.Now().Unix()), time.Now().Add(time.Second*1), "CreateByTime 当前时间+1s ["+str+"]")
			once.CreateByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_by_10_%d", time.Now().Unix()), time.Now().Add(time.Second*10), "CreateByTime 当前时间+10s ["+str+"]")
			once.CreateByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_by_15_%d", time.Now().Unix()), time.Now().Add(time.Second*15), "CreateByTime 当前时间+15s ["+str+"]")
			once.CreateByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_by_30_%d", time.Now().Unix()), time.Now().Add(time.Second*30), "CreateByTime 当前时间+30s ["+str+"]")
			once.CreateByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_by_60_%d", time.Now().Unix()), time.Now().Add(time.Second*60), "CreateByTime 当前时间+60s ["+str+"]")
			once.CreateByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_by_120_%d", time.Now().Unix()), time.Now().Add(time.Second*120), "CreateByTime 当前时间+120s ["+str+"]")    // 2分钟
			once.CreateByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_by_300_%d", time.Now().Unix()), time.Now().Add(time.Second*300), "CreateByTime 当前时间+300s ["+str+"]")    // 5分钟
			once.CreateByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_by_900_%d", time.Now().Unix()), time.Now().Add(time.Second*900), "CreateByTime 当前时间+900s ["+str+"]")    // 15分钟
			once.CreateByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_create_by_1800_%d", time.Now().Unix()), time.Now().Add(time.Second*1800), "CreateByTime 当前时间+1800s ["+str+"]") // 30分钟

			once.SaveByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_by_0_%d", time.Now().Unix()), time.Now(), "SaveByTime 当前时间 ["+str+"]")
			once.SaveByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_by_1_%d", time.Now().Unix()), time.Now().Add(time.Second*1), "SaveByTime 当前时间+1s ["+str+"]")
			once.SaveByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_by_10_%d", time.Now().Unix()), time.Now().Add(time.Second*10), "SaveByTime 当前时间+10s ["+str+"]")
			once.SaveByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_by_15_%d", time.Now().Unix()), time.Now().Add(time.Second*15), "SaveByTime 当前时间+15s ["+str+"]")
			once.SaveByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_by_30_%d", time.Now().Unix()), time.Now().Add(time.Second*30), "SaveByTime 当前时间+30s ["+str+"]")
			once.SaveByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_by_60_%d", time.Now().Unix()), time.Now().Add(time.Second*60), "SaveByTime 当前时间+60s ["+str+"]")
			once.SaveByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_by_120_%d", time.Now().Unix()), time.Now().Add(time.Second*120), "SaveByTime 当前时间+120s ["+str+"]")    // 2分钟
			once.SaveByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_by_300_%d", time.Now().Unix()), time.Now().Add(time.Second*300), "SaveByTime 当前时间+300s ["+str+"]")    // 5分钟
			once.SaveByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_by_900_%d", time.Now().Unix()), time.Now().Add(time.Second*900), "SaveByTime 当前时间+900s ["+str+"]")    // 15分钟
			once.SaveByTime(ctx, OnceTaskTypeNormal, fmt.Sprintf("stabilityTest_task_save_by_1800_%d", time.Now().Unix()), time.Now().Add(time.Second*1800), "SaveByTime 当前时间+1800s ["+str+"]") // 30分钟
		}
	}

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

	// 解析文件路径，每天一个文件
	path, _ := filepath.Abs("./")
	// 拼接文件路径
	save_path = filepath.Join(path, "/cache/once/"+time.Now().Format("2006-01-02")+".log")
	// 创建文件夹
	dir := filepath.Dir(save_path)
	os.MkdirAll(dir, 0755)

	// 追加到文件
	file, err := os.OpenFile(save_path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("打开文件失败:", err)
		return err
	}
	defer file.Close()
	_, err = file.WriteString(fmt.Sprintf("执行时间:%v [%s] \n", extendData, time.Now().Format("2006-01-02 15:04:05")))
	if err != nil {
		fmt.Println("写入文件失败:", err)
		return err
	}

	return nil
}
