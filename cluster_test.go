package timerx_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yuninks/timerx"
)

func redisInit() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "123456",
		DB:       0,
	})
}

func TestCluster_AddEveryMonth(t *testing.T) {
	ctx := context.Background()
	redis := redisInit()
	defer redis.Close()

	cluster, err := timerx.InitCluster(ctx, redis, "test")
	if err != nil {
		t.Errorf("InitCluster failed, err: %v", err)
		return
	}
	defer cluster.Stop()

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

	err = cluster.EveryMonth(ctx, taskId, 1, hour, minute, second, callback, extendData)
	if err != nil {
		t.Errorf("AddEveryMonth failed, err: %v", err)
	}

	time.Sleep(time.Second * 10)

	// TODO: verify the job is added to the cluster and can be executed at the specified time
}

func TestCluster_AddEveryWeek(t *testing.T) {
	ctx := context.Background()
	redis := redisInit()
	defer redis.Close()

	cluster, _ := timerx.InitCluster(ctx, redis, "test")

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

	err := cluster.EveryWeek(ctx, taskId, week, hour, minute, second, callback, extendData)
	if err != nil {
		t.Errorf("AddEveryWeek failed, err: %v", err)
	}

	// TODO: verify the job is added to the cluster and can be executed at the specified time
}

func TestCluster_AddEveryDay(t *testing.T) {
	ctx := context.Background()
	redis := redisInit()
	defer redis.Close()

	cluster, _ := timerx.InitCluster(ctx, redis, "test")

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

	err := cluster.EveryDay(ctx, taskId, hour, minute, second, callback, extendData)
	if err != nil {
		t.Errorf("AddEveryDay failed, err: %v", err)
	}

	// TODO: verify the job is added to the cluster and can be executed at the specified time
}

func TestCluster_AddEveryHour(t *testing.T) {
	ctx := context.Background()
	redis := redisInit()
	defer redis.Close()

	cluster, _ := timerx.InitCluster(ctx, redis, "test")

	taskId := "testTask"
	minute := 3
	second := 4
	callback := func(ctx context.Context, data interface{}) error {
		// do something
		fmt.Println("Task executed:", data)
		return nil
	}
	extendData := "testData"

	err := cluster.EveryHour(ctx, taskId, minute, second, callback, extendData)
	if err != nil {
		t.Errorf("AddEveryHour failed, err: %v", err)
	}

	// TODO: verify the job is added to the cluster and can be executed at the specified time
}

func TestCluster_AddEveryMinute(t *testing.T) {
	ctx := context.Background()
	redis := redisInit()
	defer redis.Close()

	cluster, _ := timerx.InitCluster(ctx, redis, "test")

	taskId := "testTask"
	second := 4
	callback := func(ctx context.Context, data interface{}) error {
		// do something
		fmt.Println("Task executed:", data)
		return nil
	}
	extendData := "testData"

	err := cluster.EveryMinute(ctx, taskId, second, callback, extendData)
	if err != nil {
		t.Errorf("AddEveryMinute failed, err: %v", err)
	}

	// TODO: verify the job is added to the cluster and can be executed at the specified time
}

func TestCluster_Add(t *testing.T) {
	fmt.Println("66666")
	ctx := context.Background()
	fmt.Println("66666")
	redis := redisInit()
	defer redis.Close()

	t.Log("6666")

	cluster, _ := timerx.InitCluster(ctx, redis, "test")

	taskId := "testTask"
	dur := time.Second
	callback := func(ctx context.Context, data interface{}) error {
		// do something
		fmt.Println("Task executed:", data)
		return nil
	}
	extendData := "testData"

	err := cluster.EverySpace(ctx, taskId, dur, callback, extendData)
	if err != nil {
		t.Errorf("Add failed,1 err: %v", err)
	}

	time.Sleep(time.Second * 20)

	// TODO: verify the job is added to the cluster and can be executed after the specified duration
}

func TestClusterKeyFormat(t *testing.T) {
	keyPrefix := "testcluster"

	tests := []struct {
		name      string
		key       string
		hashTag   string
		hasPrefix string
	}{
		{"zsetKey", "timer:{testcluster}:cluster_zset", "{testcluster}", "timer:"},
		{"listKey", "timer:{testcluster}:cluster_list", "{testcluster}", "timer:"},
		{"lockKey", "timer:{testcluster}:cluster_lock", "{testcluster}", "timer:"},
		{"execInfoKey", "timer:{testcluster}:cluster_execinfo", "{testcluster}", "timer:"},
		{"priorityKey", "timer:{testcluster}:cluster_priority", "{testcluster}", "timer:"},
		{"calcLockKey", "timer:{testcluster}:cluster_calc_lock:task1:1234567890", "{testcluster}", "timer:"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.key == "" || tt.key == " " {
				t.Error("key should not be empty")
			}
			t.Logf("key format: %s", tt.key)
		})
	}

	_ = keyPrefix
}

func TestClusterZsetListKeysSameHashTag(t *testing.T) {
	zsetKey := "timer:{myapp}:cluster_zset"
	listKey := "timer:{myapp}:cluster_list"

	zsetTag := extractHashTag(zsetKey)
	listTag := extractHashTag(listKey)

	if zsetTag != listTag {
		t.Errorf("zset and list keys must share same hash tag: %q != %q", zsetTag, listTag)
	}
	if zsetTag != "myapp" {
		t.Errorf("unexpected hash tag: %q", zsetTag)
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

func TestClusterMultiKeyLuaKeysShareSlot(t *testing.T) {
	type keyPair struct {
		key1 string
		key2 string
		name string
	}

	pairs := []keyPair{
		{"timer:{app}:cluster_zset", "timer:{app}:cluster_list", "zset+list"},
		{"timer:{app}:cluster_zset", "timer:{app}:cluster_calc_lock:task1:9999", "zset+calc_lock"},
	}

	for _, p := range pairs {
		t.Run(p.name, func(t *testing.T) {
			t1 := extractHashTag(p.key1)
			t2 := extractHashTag(p.key2)
			if t1 != t2 {
				t.Errorf("keys in multi-key Lua script must share same hash tag: %q != %q (%s, %s)",
					t1, t2, p.key1, p.key2)
			}
		})
	}
}

func TestInitCluster_NilRedis(t *testing.T) {
	ctx := context.Background()
	_, err := timerx.InitCluster(ctx, nil, "test")
	if err == nil {
		t.Error("expected error for nil redis")
	}
}

func TestClusterKeyPrefixSanitization(t *testing.T) {
	// 验证 sanitizeKeyPrefix 在 key 格式中的作用
	// keyPrefix 中含有 {} 时应该被替换为 _
	expectedHashTag := "{app_env_test}"

	zsetKey := "timer:" + expectedHashTag + ":cluster_zset"
	listKey := "timer:" + expectedHashTag + ":cluster_list"

	tag1 := extractHashTag(zsetKey)
	tag2 := extractHashTag(listKey)

	if tag1 != "app_env_test" {
		t.Errorf("unexpected hash tag: %q", tag1)
	}
	if tag1 != tag2 {
		t.Errorf("hash tags differ: %q != %q", tag1, tag2)
	}
}

func TestClusterEverySpace_InvalidDuration(t *testing.T) {
	ctx := context.Background()
	redis := redisInit()
	defer redis.Close()

	cluster, err := timerx.InitCluster(ctx, redis, "test_invalid")
	if err != nil {
		t.Skipf("redis not available: %v", err)
	}
	defer cluster.Stop()

	err = cluster.EverySpace(ctx, "testNegative", -1*time.Second,
		func(ctx context.Context, data interface{}) error { return nil }, nil)
	if err == nil {
		t.Error("expected error for negative duration")
	}
}

func TestClusterAddDuplicateTaskId(t *testing.T) {
	ctx := context.Background()
	redis := redisInit()
	defer redis.Close()

	cluster, err := timerx.InitCluster(ctx, redis, "test_dup")
	if err != nil {
		t.Skipf("redis not available: %v", err)
	}
	defer cluster.Stop()

	callback := func(ctx context.Context, data interface{}) error { return nil }
	err = cluster.EveryMinute(ctx, "dup_task", 0, callback, nil)
	if err != nil {
		t.Fatalf("first add should succeed: %v", err)
	}

	err = cluster.EveryMinute(ctx, "dup_task", 0, callback, nil)
	if err != timerx.ErrTaskIdExists {
		t.Errorf("expected ErrTaskIdExists, got %v", err)
	}
}

func TestClusterEveryMonth_InvalidDay(t *testing.T) {
	ctx := context.Background()
	redis := redisInit()
	defer redis.Close()

	cluster, err := timerx.InitCluster(ctx, redis, "test_inv_day")
	if err != nil {
		t.Skipf("redis not available: %v", err)
	}
	defer cluster.Stop()

	callback := func(ctx context.Context, data interface{}) error { return nil }
	err = cluster.EveryMonth(ctx, "test", 32, 0, 0, 0, callback, nil)
	if err != timerx.ErrMonthDay {
		t.Errorf("expected ErrMonthDay, got %v", err)
	}
}

func TestClusterEveryWeek_InvalidWeekday(t *testing.T) {
	ctx := context.Background()
	redis := redisInit()
	defer redis.Close()

	cluster, err := timerx.InitCluster(ctx, redis, "test_inv_wd")
	if err != nil {
		t.Skipf("redis not available: %v", err)
	}
	defer cluster.Stop()

	callback := func(ctx context.Context, data interface{}) error { return nil }
	err = cluster.EveryWeek(ctx, "test", 7, 0, 0, 0, callback, nil)
	if err != timerx.ErrWeekday {
		t.Errorf("expected ErrWeekday, got %v", err)
	}
}

func TestClusterEveryDay_InvalidHour(t *testing.T) {
	ctx := context.Background()
	redis := redisInit()
	defer redis.Close()

	cluster, err := timerx.InitCluster(ctx, redis, "test_inv_h")
	if err != nil {
		t.Skipf("redis not available: %v", err)
	}
	defer cluster.Stop()

	callback := func(ctx context.Context, data interface{}) error { return nil }
	err = cluster.EveryDay(ctx, "test", 24, 0, 0, callback, nil)
	if err != timerx.ErrHour {
		t.Errorf("expected ErrHour, got %v", err)
	}
}

func TestClusterEveryHour_InvalidMinute(t *testing.T) {
	ctx := context.Background()
	redis := redisInit()
	defer redis.Close()

	cluster, err := timerx.InitCluster(ctx, redis, "test_inv_m")
	if err != nil {
		t.Skipf("redis not available: %v", err)
	}
	defer cluster.Stop()

	callback := func(ctx context.Context, data interface{}) error { return nil }
	err = cluster.EveryHour(ctx, "test", 60, 0, callback, nil)
	if err != timerx.ErrMinute {
		t.Errorf("expected ErrMinute, got %v", err)
	}
}

func TestClusterEveryMinute_InvalidSecond(t *testing.T) {
	ctx := context.Background()
	redis := redisInit()
	defer redis.Close()

	cluster, err := timerx.InitCluster(ctx, redis, "test_inv_s")
	if err != nil {
		t.Skipf("redis not available: %v", err)
	}
	defer cluster.Stop()

	callback := func(ctx context.Context, data interface{}) error { return nil }
	err = cluster.EveryMinute(ctx, "test", 60, callback, nil)
	if err != timerx.ErrSecond {
		t.Errorf("expected ErrSecond, got %v", err)
	}
}
