package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/yuninks/timerx"
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
	// cluster()
	once()

	select {}

}

func once() {
	client := getRedis()
	ctx := context.Background()
	w := OnceWorker{}
	one := timerx.InitOnce(ctx, client, "test", w)

	d := OnceData{
		Num: 1,
	}
	dy, _ := json.Marshal(d)

	err := one.Create("test", "test", 1*time.Second, dy)
	if err != nil {
		fmt.Println(err)
	}

}

type OnceData struct {
	Num int
}

type OnceWorker struct{}

func (l OnceWorker) Worker(ctx context.Context, taskType string, taskId string, attachData []byte) *timerx.OnceWorkerResp {
	fmt.Println("执行时间:", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(taskId, taskType)
	fmt.Printf("原来的参数：%s\n", string(attachData))

	d := OnceData{}

	json.Unmarshal(attachData, &d)

	d.Num++

	fmt.Println(d)

	dy, _ := json.Marshal(d)

	return &timerx.OnceWorkerResp{
		Retry:      true,
		AttachData: dy,
		DelayTime:  1 * time.Second,
	}
}

func cluster() {
	client := getRedis()
	ctx := context.Background()
	cluster := timerx.InitCluster(ctx, client, "test")
	err := cluster.EverySpace(ctx, "test_space", 1*time.Second, aa, "这是秒任务")
	fmt.Println(err)
	err = cluster.EveryMinute(ctx, "test_min", 15, aa, "这是分钟任务")
	fmt.Println(err)
	err = cluster.EveryHour(ctx, "test_hour", 30, 0, aa, "这是小时任务")
	fmt.Println(err)
	err = cluster.EveryDay(ctx, "test_day", 11, 0, 0, aa, "这是天任务")
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
	cl := timerx.InitCluster(ctx, client, "kkkk")
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
