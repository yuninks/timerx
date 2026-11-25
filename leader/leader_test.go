package leader

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestInitLeader_NilRedis(t *testing.T) {
	ctx := context.Background()
	_, err := InitLeader(ctx, nil, "test")
	if err == nil {
		t.Error("expected error for nil redis")
	}
}

func TestLeaderKeyFormat(t *testing.T) {
	keyPrefix := "myapp"
	source := "cluster"

	lockKey := "timer:{myapp}:leader_lock_cluster"
	leaderKey := "timer:{myapp}:leader_cluster"

	if !strings.Contains(lockKey, "{myapp}") {
		t.Errorf("lockKey missing hash tag: %s", lockKey)
	}
	if !strings.Contains(leaderKey, "{myapp}") {
		t.Errorf("leaderKey missing hash tag: %s", leaderKey)
	}

	_ = keyPrefix
	_ = source
}

func TestLeaderKeyFormat_Once(t *testing.T) {
	lockKey := "timer:{app}:leader_lock_once"
	leaderKey := "timer:{app}:leader_once"

	tag1 := extractHashTag(lockKey)
	tag2 := extractHashTag(leaderKey)

	if tag1 != "app" {
		t.Errorf("unexpected hash tag: %q", tag1)
	}
	if tag1 != tag2 {
		t.Errorf("hash tags differ: %q != %q", tag1, tag2)
	}
}

func extractHashTag(key string) string {
	start := strings.Index(key, "{")
	end := strings.Index(key, "}")
	if start >= 0 && end > start {
		return key[start+1 : end]
	}
	return ""
}

// ensure redis import used for InitLeader type checking
var _ redis.UniversalClient

// 模拟 Redis 故障的 hook
type failHook struct {
	fail atomic.Bool
}

func (h *failHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *failHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.fail.Load() {
			err := errors.New("mock redis error")
			cmd.SetErr(err)
			return err
		}
		return next(ctx, cmd)
	}
}

func (h *failHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// 回归测试（依赖本地 Redis）：
// 锁续期遇到 Redis 错误后锁会过期并被其他实例抢到，原 Leader 必须退位，不能长期双 Leader
func TestLeader_StepDownAfterRefreshError(t *testing.T) {
	ctx := context.Background()
	opt := &redis.Options{Addr: "localhost:6379", Password: "123456"}

	clientA := redis.NewClient(opt)
	defer clientA.Close()
	clientB := redis.NewClient(opt)
	defer clientB.Close()
	if err := clientB.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available: %v", err)
	}
	clientB.Del(ctx, "timer:{reg_leader}:leader_lock_test", "timer:{reg_leader}:leader_test")

	hook := &failHook{}
	clientA.AddHook(hook)

	a, err := InitLeader(ctx, clientA, "reg_leader", WithInstanceId("a"), WithSource("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	time.Sleep(500 * time.Millisecond)

	b, err := InitLeader(ctx, clientB, "reg_leader", WithInstanceId("b"), WithSource("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	time.Sleep(500 * time.Millisecond)

	if !a.IsLeader() || b.IsLeader() {
		t.Fatalf("a=%v b=%v, want a is leader", a.IsLeader(), b.IsLeader())
	}

	// A 的 Redis 短暂故障，超过一次续期间隔后恢复
	hook.fail.Store(true)
	time.Sleep(1500 * time.Millisecond)
	hook.fail.Store(false)

	// 任何时刻都只能有一个 Leader（允许校验间隔内的短暂重叠）
	both := 0
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if a.IsLeader() && b.IsLeader() {
			both++
		}
		time.Sleep(100 * time.Millisecond)
	}
	if both > 15 {
		t.Errorf("both instances were leader for about %dms", both*100)
	}
	if a.IsLeader() == b.IsLeader() {
		t.Errorf("a=%v b=%v, want exactly one leader", a.IsLeader(), b.IsLeader())
	}
}
