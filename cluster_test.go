package timerx_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/yuninks/timerx"
)

func TestCluster_AddEveryMonth(t *testing.T) {
	ctx := context.Background()
	redis := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer redis.Close()

	cluster := timerx.InitCluster(ctx, redis, "test")

	taskId := "testTask"
	hour := 2
	minute := 3
	second := 4
	callback := func(ctx context.Context, data interface{}) error {
		// do something
		fmt.Println("Task executed:", data)
		return nil
	}
	extendData := "testData"

	err := cluster.AddEveryMonth(ctx, taskId, 1, hour, minute, second, callback, extendData)
	if err != nil {
		t.Errorf("AddEveryMonth failed, err: %v", err)
	}

	// TODO: verify the job is added to the cluster and can be executed at the specified time
}

func TestCluster_AddEveryWeek(t *testing.T) {
	ctx := context.Background()
	redis := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer redis.Close()

	cluster := timerx.InitCluster(ctx, redis, "test")

	taskId := "testTask"
	week := time.Sunday
	hour := 2
	minute := 3
	second := 4
	callback := func(ctx context.Context, data interface{}) error {
		// do something
		fmt.Println("Task executed:", data)
		return nil
	}
	extendData := "testData"

	err := cluster.AddEveryWeek(ctx, taskId, week, hour, minute, second, callback, extendData)
	if err != nil {
		t.Errorf("AddEveryWeek failed, err: %v", err)
	}

	// TODO: verify the job is added to the cluster and can be executed at the specified time
}

func TestCluster_AddEveryDay(t *testing.T) {
	ctx := context.Background()
	redis := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer redis.Close()

	cluster := timerx.InitCluster(ctx, redis, "test")

	taskId := "testTask"
	hour := 2
	minute := 3
	second := 4
	callback := func(ctx context.Context, data interface{}) error {
		// do something
		fmt.Println("Task executed:", data)
		return nil
	}
	extendData := "testData"

	err := cluster.AddEveryDay(ctx, taskId, hour, minute, second, callback, extendData)
	if err != nil {
		t.Errorf("AddEveryDay failed, err: %v", err)
	}

	// TODO: verify the job is added to the cluster and can be executed at the specified time
}

func TestCluster_AddEveryHour(t *testing.T) {
	ctx := context.Background()
	redis := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer redis.Close()

	cluster := timerx.InitCluster(ctx, redis, "test")

	taskId := "testTask"
	minute := 3
	second := 4
	callback := func(ctx context.Context, data interface{}) error{
		// do something
		fmt.Println("Task executed:", data)
		return nil
	}
	extendData := "testData"

	err := cluster.AddEveryHour(ctx, taskId, minute, second, callback, extendData)
	if err != nil {
		t.Errorf("AddEveryHour failed, err: %v", err)
	}

	// TODO: verify the job is added to the cluster and can be executed at the specified time
}

func TestCluster_AddEveryMinute(t *testing.T) {
	ctx := context.Background()
	redis := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer redis.Close()

	cluster := timerx.InitCluster(ctx, redis, "test")

	taskId := "testTask"
	second := 4
	callback := func(ctx context.Context, data interface{}) error{
		// do something
		fmt.Println("Task executed:", data)
		return nil
	}
	extendData := "testData"

	err := cluster.AddEveryMinute(ctx, taskId, second, callback, extendData)
	if err != nil {
		t.Errorf("AddEveryMinute failed, err: %v", err)
	}

	// TODO: verify the job is added to the cluster and can be executed at the specified time
}

func TestCluster_Add(t *testing.T) {
	fmt.Println("66666")
	ctx := context.Background()
	fmt.Println("66666")
	redis := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer redis.Close()

	t.Log("6666")

	cluster := timerx.InitCluster(ctx, redis, "test")

	taskId := "testTask"
	dur := time.Second
	callback := func(ctx context.Context, data interface{}) error {
		// do something
		fmt.Println("Task executed:", data)
		return nil
	}
	extendData := "testData"

	err := cluster.Add(ctx, taskId, dur, callback, extendData)
	if err != nil {
		t.Errorf("Add failed, err: %v", err)
	}

	time.Sleep(time.Second * 20)


	// TODO: verify the job is added to the cluster and can be executed after the specified duration
}

func TestMain(m *testing.M) {
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1" + ":" + "6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	if client == nil {
		fmt.Println("redis init error")
		return
	}
	// Redis = client

}

func TestRedis(t *testing.T) {
	fmt.Println("6666")
	t.Log("fffff")
	// t.Fail()
	// t.Error("ffff")
	// Redis.Set(context.Background(), "dddd", "dddd", 0)
	// str, err := Redis.Get(context.Background(), "dddd").Result()
	// fmt.Println("ssss", str, err)
	// t.Log(str, err)
	// t.Fail()
}
