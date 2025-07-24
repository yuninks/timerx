package priority_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/yuninks/timerx/priority"
)

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

func TestPriority(t *testing.T) {
	re := getRedis()
	ctx := context.Background()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fmt.Println("ff")

	go func() {
		time.Sleep(time.Second * 5)

		ctx, cancel := context.WithCancel(ctx)

		pro := priority.InitPriority(ctx, re, "test", 10, priority.SetUpdateInterval(time.Second*1))

		for i := 0; i < 10; i++ {
			bb := pro.IsLatest(ctx)
			fmt.Println("cc:", bb)
			time.Sleep(time.Second)
		}
		
		cancel()
	}()

	pro := priority.InitPriority(ctx, re, "test", 0, priority.SetUpdateInterval(time.Second*1))

	for i := 0; i < 25; i++ {
		bb := pro.IsLatest(ctx)
		fmt.Println("bb:", bb)
		time.Sleep(time.Second)
	}

}
