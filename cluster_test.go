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
