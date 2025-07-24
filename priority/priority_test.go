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

	ctx,cancel := context.WithCancel(ctx)
	defer cancel()

	fmt.Println("ff")

	pro := priority.InitPriority(ctx, re, "test", priority.SetUpdateInterval(time.Second*5))

	fmt.Println(pro)

	for i := 0; i < 20; i++ {
		bb := pro.IsLatest(ctx)
		fmt.Println("bb:", bb)
		time.Sleep(time.Second)
	}

}
