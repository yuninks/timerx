package timer_test

import (
	"context"
	"fmt"
	"code.yun.ink/open/timer"
)

// 示例测试

func exampleDemo(ctx context.Context) bool {
	fmt.Println("fff")
	return false
}

func ExampleB() {
	ctx := context.Background()
	timer.InitSingle(ctx)
	timer.AddToTimer(1, exampleDemo)
	// OutPut:
}
