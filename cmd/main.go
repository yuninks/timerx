package main

import (
	"context"
	"fmt"
	"time"

	"code.yun.ink/open/timer"
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

	re()

}

func re() {
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1" + ":" + "6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	if client == nil {
		fmt.Println("redis init error")
		return
	}

	ctx := context.Background()
	cl := timer.InitCluster(ctx, client)
	cl.AddTimer(ctx, "test1", 1*time.Second, aa, timer.ExtendParams{
		Params: map[string]interface{}{
			"test": "text1",
		},
	})
	cl.AddTimer(ctx, "test2", 1*time.Second, aa, timer.ExtendParams{
		Params: map[string]interface{}{
			"test": "text2",
		},
	})
	cl.AddTimer(ctx, "test3", 1*time.Second, aa, timer.ExtendParams{
		Params: map[string]interface{}{
			"test": "text3",
		},
	})
	cl.AddTimer(ctx, "test4", 1*time.Second, aa, timer.ExtendParams{
		Params: map[string]interface{}{
			"test": "text4",
		},
	})
	cl.AddTimer(ctx, "test5", 1*time.Second, aa, timer.ExtendParams{
		Params: map[string]interface{}{
			"test": "text5",
		},
	})
	cl.AddTimer(ctx, "test6", 1*time.Second, aa, timer.ExtendParams{
		Params: map[string]interface{}{
			"test": "text6",
		},
	})
	cl.AddTimer(ctx, "test7", 1*time.Second, aa, timer.ExtendParams{
		Params: map[string]interface{}{
			"test": "text7",
		},
	})
	cl.AddTimer(ctx, "test8", 1*time.Second, aa, timer.ExtendParams{
		Params: map[string]interface{}{
			"test": "text8",
		},
	})
	cl.AddTimer(ctx, "test9", 1*time.Second, aa, timer.ExtendParams{
		Params: map[string]interface{}{
			"test": "text9",
		},
	})

	select {}
}

func aa(ctx context.Context) bool {
	// fmt.Println(time.Now().Format(time.RFC3339))
	// fmt.Println("gggggggggggggggggggggggggggg")
	a, err := timer.GetExtendParams(ctx)
	fmt.Printf("%+v %+v \n\n", a, err)
	return true
}
