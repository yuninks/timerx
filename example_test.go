package timer

import (
	"fmt"
	"testing"

	"github.com/go-redis/redis/v8"
)

// 示例测试

// func exampleDemo(ctx context.Context) bool {
// 	fmt.Println("fff")
// 	return false
// }

// func ExampleB() {
// 	ctx := context.Background()
// 	timer.InitSingle(ctx)
// 	timer.AddToTimer(1, exampleDemo)
// 	// OutPut:
// }

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
