package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/yuninks/timerx"
	"github.com/yuninks/timerx/priority"
)

func main() {
	// m := make(map[string]time.Time)
	// m["sss"] = time.Now()

	// b, _ := json.Marshal(m)

	// fmt.Println(string(b))

	// mm := make(map[string]time.Time)
	// json.Unmarshal(b, &mm)

	// fmt.Println(mm)

	// re()
	// d()
	cluster()
	// once()
	// prioritys()

	select {}

}

func prioritys() {

	client := getRedis()
	ctx := context.Background()
	pro, _ := priority.InitPriority(ctx, client, "test", 10)

	for {
		b := pro.IsLatest(ctx)
		fmt.Println("isLatest", b)
		time.Sleep(time.Millisecond * 100)
	}

}

func once() {
	client := getRedis()
	ctx := context.Background()
	w := OnceWorker{}

	ver, err := priority.PriorityByVersion("v2.2.3.4.5")
	if err != nil {
		panic(err)
	}

	ops := []timerx.Option{
		timerx.WithPriority(ver),
	}

	one, err := timerx.InitOnce(ctx, client, "test_once", w, ops...)
	if err != nil {
		panic(err)
	}

	d := OnceData{
		Num: 3,
	}
	// dy, _ := json.Marshal(d)

	err = one.Create("test", "test3", 1*time.Second, d)
	if err != nil {
		fmt.Println(err)
	}
	// d = OnceData{
	// 	Num: 4,
	// }
	// dd := 123
	// dy, _ = json.Marshal(d)
	// err = one.Save("test", "test4", 2*time.Second, dd)
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// err = one.Save("test", "test5", 5*time.Second, dd)
	// if err != nil {
	// 	fmt.Println(err)
	// }

}

type OnceData struct {
	Num int
}

type OnceWorker struct{}

func (l OnceWorker) Worker(ctx context.Context, taskType timerx.OnceTaskType, taskId string, attachData interface{}) *timerx.OnceWorkerResp {
	// 追加写入文件
	file, err := os.OpenFile("./test.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	file.WriteString(fmt.Sprintf("执行时间:%s\n", time.Now().Format("2006-01-02 15:04:05")))

	fmt.Println("执行时间:", time.Now().Format("2006-01-02 15:04:05"))
	// fmt.Println(taskType, taskId)

	// fmt.Printf("原来的参数：%+v %T\n", attachData, attachData)

	// v, ok := attachData.(int64)
	// fmt.Println("vvvvvvv", v, ok)
	// fmt.Printf()

	// d := OnceData{}

	// json.Unmarshal(ab, &d)

	// d.Num++

	// fmt.Println(d)

	// dy, _ := json.Marshal(d)

	return &timerx.OnceWorkerResp{
		Retry:      true,
		AttachData: attachData,
		DelayTime:  time.Second,
	}
}

func cluster() {
	client := getRedis()
	ctx := context.Background()

	// log := loggerx.NewLogger(ctx,loggerx.SetToConsole(),loggerx.SetEscapeHTML(false))
	// _ = log

	cluster, _ := timerx.InitCluster(ctx, client, "test", timerx.WithPriority(103))
	err := cluster.EverySpace(ctx, "test_space1", 1*time.Second, aa, "这是秒任务1")
	fmt.Println(err)
	err = cluster.EverySpace(ctx, "test_space2", 2*time.Second, aa, "这是秒任务2")
	fmt.Println(err)
	err = cluster.EverySpace(ctx, "test_space3", 5*time.Second, aa, "这是秒任务3")
	fmt.Println(err)

	err = cluster.EveryMinute(ctx, "test_min1", 15, aa, "这是分钟任务1")
	fmt.Println(err)
	err = cluster.EveryMinute(ctx, "test_min2", 30, aa, "这是分钟任务2")
	fmt.Println(err)

	err = cluster.EveryHour(ctx, "test_hour1", 30, 0, aa, "这是小时任务1")
	fmt.Println(err)
	err = cluster.EveryHour(ctx, "test_hour2", 30, 15, aa, "这是小时任务2")
	fmt.Println(err)

	err = cluster.EveryDay(ctx, "test_day1", 5, 0, 0, aa, "这是天任务1")
	fmt.Println(err)
	err = cluster.EveryDay(ctx, "test_day2", 9, 20, 0, aa, "这是天任务2")
	fmt.Println(err)
	err = cluster.EveryDay(ctx, "test_day3", 10, 30, 30, aa, "这是天任务3")
	fmt.Println(err)
}

func worker() {
	// client := getRedis()
	// w := timerx.InitOnce(context.Background(), client, "test", &OnceWorker{})
	// w.Save("test", "test", 1*time.Second, map[string]interface{}{
	// 	"test": "test",
	// })
	// w.Save("test2", "test", 1*time.Second, map[string]interface{}{
	// 	"test": "test",
	// })
	// w.Save("test3", "test", 1*time.Second, map[string]interface{}{
	// 	"test": "test",
	// })
	// w.Save("test4", "test", 1*time.Second, map[string]interface{}{
	// 	"test": "test",
	// })
	// w.Save("test5", "test", 1*time.Second, map[string]interface{}{
	// 	"test": "test",
	// })

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

func re() {

	client := getRedis()

	ctx := context.Background()
	cl, _ := timerx.InitCluster(ctx, client, "kkkk")
	cl.EverySpace(ctx, "test1", 1*time.Millisecond, aa, "data")
	cl.EverySpace(ctx, "test2", 1*time.Millisecond, aa, "data")
	cl.EverySpace(ctx, "test3", 1*time.Millisecond, aa, "data")
	cl.EverySpace(ctx, "test4", 1*time.Millisecond, aa, "data")
	cl.EverySpace(ctx, "test5", 1*time.Millisecond, aa, "data")
	cl.EverySpace(ctx, "test6", 1*time.Millisecond, aa, "data")

	select {}
}

func aa(ctx context.Context, data interface{}) error {

	fmt.Println("-执行时间:", data, time.Now().Format("2006-01-02 15:04:05"))
	// fmt.Println(data)
	// time.Sleep(time.Second * 5)

	// 追加到文件
	file, err := os.OpenFile("./test.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("打开文件失败:", err)
		return err
	}
	defer file.Close()
	_, err = file.WriteString(fmt.Sprintf("-执行时间:%v %s\n", data, time.Now().Format("2006-01-02 15:04:05")))
	if err != nil {
		fmt.Println("写入文件失败:", err)
		return err
	}

	return nil
}

func d() {

	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1" + ":" + "6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	if client == nil {
		fmt.Println("redis init error")
		return
	}

	client.ZAdd(context.Background(), "lockx:test2", &redis.Z{
		Score:  50,
		Member: "test",
	})

	script := `
	local token = redis.call('zrangebyscore',KEYS[1],ARGV[1],ARGV[2])
	for i,v in ipairs(token) do
		redis.call('zrem',KEYS[1],v)
		redis.call('lpush',KEYS[2],v)
	end
	return "OK"
	`
	res, err := client.Eval(context.Background(), script, []string{"lockx:test2", "lockx:push"}, 0, 100).Result()
	fmt.Println(res, err)

	for i := 0; i < 10; i++ {
		l, e := client.RPop(context.Background(), "lockx:push").Result()
		fmt.Println(l, e)
	}

}
