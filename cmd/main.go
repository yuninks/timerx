package main

import (
	"context"
	"fmt"
	"time"

	"code.yun.ink/pkg/timerx"
	"github.com/go-redis/redis/v8"
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
	worker()

}

func worker() {
	client := getRedis()
	w := timerx.InitOnce(context.Background(), client, &Worker{})
	w.Add("test", "test", 1*time.Second, map[string]interface{}{
		"test": "test",
	})
	w.Add("test2", "test", 1*time.Second, map[string]interface{}{
		"test": "test",
	})
	w.Add("test3", "test", 1*time.Second, map[string]interface{}{
		"test": "test",
	})
	w.Add("test4", "test", 1*time.Second, map[string]interface{}{
		"test": "test",
	})
	w.Add("test5", "test", 1*time.Second, map[string]interface{}{
		"test": "test",
	})

	select {}
}

type Worker struct{}

func (w *Worker) Worker(uniqueKey string, jobType string, data interface{}) (timerx.WorkerCode, time.Duration) {
	fmt.Println("执行时间:", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(uniqueKey, jobType)
	fmt.Println(data)
	return timerx.WorkerCodeAgain,time.Second
}

func getRedis() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1" + ":" + "6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	if client == nil {
		panic("redis init error")
	}
	return client
}

func re() {

	client := getRedis()

	ctx := context.Background()
	cl := timerx.InitCluster(ctx, client)
	cl.Add(ctx, "test1", 1*time.Millisecond, aa, "data")
	cl.Add(ctx, "test2", 1*time.Millisecond, aa, "data")
	cl.Add(ctx, "test3", 1*time.Millisecond, aa, "data")
	cl.Add(ctx, "test4", 1*time.Millisecond, aa, "data")
	cl.Add(ctx, "test5", 1*time.Millisecond, aa, "data")
	cl.Add(ctx, "test6", 1*time.Millisecond, aa, "data")

	select {}
}

func aa(ctx context.Context, data interface{}) error {
	fmt.Println("执行时间:", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(data)
	time.Sleep(time.Second * 5)
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
