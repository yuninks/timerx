package priority

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

		pro, _ := InitPriority(ctx, re, "test", 10, WithUpdateInterval(time.Second*1))

		for i := 0; i < 10; i++ {
			bb := pro.IsLatest(ctx)
			fmt.Println("cc:", bb)
			time.Sleep(time.Second)
		}

		cancel()
	}()

	pro, _ := InitPriority(ctx, re, "test", 0, WithUpdateInterval(time.Second*1))

	for i := 0; i < 25; i++ {
		bb := pro.IsLatest(ctx)
		fmt.Println("bb:", bb)
		time.Sleep(time.Second)
	}

}

// MockRedisClient 模拟Redis客户端
type MockRedisClient struct {
	redis.UniversalClient
	mock.Mock
}

func (m *MockRedisClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	arguments := m.Called(ctx, script, keys, args)
	return arguments.Get(0).(*redis.Cmd)
}

func (m *MockRedisClient) Get(ctx context.Context, key string) *redis.StringCmd {
	arguments := m.Called(ctx, key)
	return arguments.Get(0).(*redis.StringCmd)
}

func (m *MockRedisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	arguments := m.Called(ctx, key, value, expiration)
	return arguments.Get(0).(*redis.StatusCmd)
}

func TestInitPriority(t *testing.T) {
	ctx := context.Background()

	// 测试正常初始化
	priority, _ := InitPriority(ctx, getRedis(), "test", 100)
	assert.NotNil(t, priority)
	assert.Equal(t, int64(100), priority.priority)
}

func TestSetPriorityScenarios(t *testing.T) {
	testCases := []struct {
		name           string
		currentRedis   interface{}
		newPriority    int64
		expectedStatus string
		expectedValue  int64
	}{
		{
			name:           "首次设置优先级",
			currentRedis:   nil, // Redis中不存在key
			newPriority:    100,
			expectedStatus: "SET",
			expectedValue:  100,
		},
		{
			name:           "更新更高优先级",
			currentRedis:   "50", // Redis中存在较低优先级
			newPriority:    100,
			expectedStatus: "UPDATED",
			expectedValue:  100,
		},
		{
			name:           "保持相同优先级",
			currentRedis:   "100", // Redis中存在相同优先级
			newPriority:    100,
			expectedStatus: "EXTENDED",
			expectedValue:  100,
		},
		{
			name:           "忽略较低优先级",
			currentRedis:   "150", // Redis中存在更高优先级
			newPriority:    100,
			expectedStatus: "IGNORED",
			expectedValue:  150,
		},
	}

	ctx := context.Background()

	redisConn := getRedis()
	// 删除Key
	redisConn.Del(ctx, "timer:priority_test22")

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			priority, _ := InitPriority(ctx, redisConn, "test22", tc.newPriority)
			defer priority.Close()

			time.Sleep(time.Second * 1)

			_, err := priority.setPriority()

			assert.NoError(t, err)
			// assert.Equal(t, tc.expectedStatus, sta)
		})
	}
}

// 并发安全测试
func TestConcurrentAccess(t *testing.T) {
	ctx := context.Background()

	priority, _ := InitPriority(ctx, getRedis(), "testacc", 100)

	time.Sleep(time.Second * 1)

	// 并发读取IsLatest
	var wg sync.WaitGroup
	results := make(chan bool, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- priority.IsLatest(ctx)
		}()
	}

	wg.Wait()
	close(results)

	// 所有结果应该相同
	firstResult := <-results
	for result := range results {
		t.Log(result)
		assert.Equal(t, firstResult, result)
	}
}

// 错误处理测试
func TestErrorScenarios(t *testing.T) {
	t.Run("Redis连接失败", func(t *testing.T) {
		ctx := context.Background()

		priority, _ := InitPriority(ctx, getRedis(), "test", 100)
		_, err := priority.setPriority()

		assert.Error(t, err)
	})

	t.Run("Redis返回值解析错误", func(t *testing.T) {
		ctx := context.Background()

		priority := &Priority{
			redis:    getRedis(),
			redisKey: "timer:priority_test",
			priority: 100,
			ctx:      ctx,
		}

		_, err := priority.getCurrentPriority()
		assert.Error(t, err)
	})
}
