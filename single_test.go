package timerx_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/yuninks/timerx"
)

// MockLogger 用于测试的日志记录器
type MockLogger struct {
	Infos  []string
	Errors []string
	Warns  []string
	mu     sync.Mutex
}

func (m *MockLogger) Infof(ctx context.Context, format string, args ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Infos = append(m.Infos, fmt.Sprintf(format, args...))
}

func (m *MockLogger) Errorf(ctx context.Context, format string, args ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Errors = append(m.Errors, fmt.Sprintf(format, args...))
}

func (m *MockLogger) Warnf(ctx context.Context, format string, args ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Warns = append(m.Warns, fmt.Sprintf(format, args...))
}

func (m *MockLogger) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Infos = nil
	m.Errors = nil
	m.Warns = nil
}

// 测试基础功能
func TestSingleTimer_Basic(t *testing.T) {
	ctx := context.Background()
	mockLogger := &MockLogger{}

	timer := timerx.InitSingle(ctx,
		timerx.WithLogger(mockLogger),
		timerx.WithLocation(time.UTC))
	defer timer.Stop()

	// 测试任务计数
	assert.Equal(t, 0, timer.TaskCount())

	var executionCount int32
	taskFunc := func(ctx context.Context, data interface{}) error {
		atomic.AddInt32(&executionCount, 1)
		return nil
	}

	// 添加间隔任务
	index, err := timer.EverySpace(ctx, "test-task", 100*time.Millisecond, taskFunc, nil)
	assert.NoError(t, err)
	assert.Greater(t, index, int64(0))
	assert.Equal(t, 1, timer.TaskCount())

	// 等待任务执行
	time.Sleep(300 * time.Millisecond)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&executionCount), int32(2))

	// 删除任务
	timer.Del(index)
	assert.Equal(t, 0, timer.TaskCount())
}

// 测试错误参数
func TestSingleTimer_InvalidParams(t *testing.T) {
	ctx := context.Background()
	timer := timerx.InitSingle(ctx)
	defer timer.Stop()

	validFunc := func(ctx context.Context, data interface{}) error { return nil }

	// 测试空taskId
	_, err := timer.EverySpace(ctx, "", time.Second, validFunc, nil)
	assert.Error(t, err)

	// 测试nil回调函数
	_, err = timer.EverySpace(ctx, "test", time.Second, nil, nil)
	assert.Error(t, err)

	// 测试无效间隔时间
	_, err = timer.EverySpace(ctx, "test", -time.Second, validFunc, nil)
	assert.Error(t, err)
	_, err = timer.EverySpace(ctx, "test", 0, validFunc, nil)
	assert.Error(t, err)
}

// 测试任务去重
func TestSingleTimer_Deduplication(t *testing.T) {
	ctx := context.Background()
	mockLogger := &MockLogger{}

	timer := timerx.InitSingle(ctx, timerx.WithLogger(mockLogger))
	defer timer.Stop()

	var executionCount int32
	taskFunc := func(ctx context.Context, data interface{}) error {
		atomic.AddInt32(&executionCount, 1)
		time.Sleep(100 * time.Millisecond) // 模拟耗时任务
		return nil
	}

	// 添加短间隔任务
	_, err := timer.EverySpace(ctx, "dedup-test", 50*time.Millisecond, taskFunc, nil)
	assert.NoError(t, err)

	// 等待一段时间，检查去重是否生效
	time.Sleep(250 * time.Millisecond)

	// 应该只有1次执行（因为任务执行需要100ms，50ms的间隔会被去重）
	assert.Equal(t, int32(1), atomic.LoadInt32(&executionCount))

	// t.Logf("warn: %+v", mockLogger.Warns)
	// t.Logf("info: %+v", mockLogger.Infos)
	fmt.Println("info:", mockLogger.Infos)
	fmt.Println("warn:", mockLogger.Warns)

	// 检查是否有去重日志
	assert.Contains(t, mockLogger.Infos, "timer: 任务正在执行中，跳过本次 dedup-test")
}

// 测试并发安全
func TestSingleTimer_Concurrency(t *testing.T) {
	ctx := context.Background()
	timer := timerx.InitSingle(ctx)
	defer timer.Stop()

	var wg sync.WaitGroup
	var executionCount int32

	// 并发添加任务
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			taskFunc := func(ctx context.Context, data interface{}) error {
				atomic.AddInt32(&executionCount, 1)
				return nil
			}

			_, err := timer.EverySpace(ctx, fmt.Sprintf("concurrent-%d", i),
				time.Duration(i+1)*100*time.Millisecond, taskFunc, nil)
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()
	assert.Equal(t, 10, timer.TaskCount())

	// 等待任务执行
	time.Sleep(500 * time.Millisecond)
	assert.Greater(t, atomic.LoadInt32(&executionCount), int32(0))

	// 并发删除任务
	timer.TaskCount()
	maxIndex := timer.MaxIndex()
	for i := int64(1); i < maxIndex; i++ {
		wg.Add(1)
		go func(index int64) {
			defer wg.Done()
			timer.Del(index)
		}(i)
	}

	wg.Wait()
	assert.Equal(t, 0, timer.TaskCount())
}

// 测试任务超时
func TestSingleTimer_Timeout(t *testing.T) {
	ctx := context.Background()
	mockLogger := &MockLogger{}

	timer := timerx.InitSingle(ctx, timerx.WithLogger(mockLogger), timerx.WithTimeout(1*time.Second))
	defer timer.Stop()

	// 长时间运行的任务
	longTask := func(ctx context.Context, data interface{}) error {
		fmt.Println("long task start")
		select {
		case <-time.After(2 * time.Second): // 超过超时时间
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	_, err := timer.EverySpace(ctx, "timeout-test", 100*time.Millisecond, longTask, nil)
	assert.NoError(t, err)

	time.Sleep(time.Second * 5)

	// 检查是否有超时相关的错误日志
	if len(mockLogger.Errors) == 0 {
		t.Fatalf("expected timeout error log, got none")
	}
	isTimeout := false
	for _, err := range mockLogger.Errors {
		isTimeout = strings.Contains(err, "context deadline exceeded")
		if isTimeout {
			break
		}
	}
	assert.True(t, isTimeout)
}

// 测试panic恢复
func TestSingleTimer_PanicRecovery(t *testing.T) {
	ctx := context.Background()
	mockLogger := &MockLogger{}

	timer := timerx.InitSingle(ctx, timerx.WithLogger(mockLogger))
	defer timer.Stop()

	panicTask := func(ctx context.Context, data interface{}) error {
		panic("test panic")
	}

	_, err := timer.EverySpace(ctx, "panic-test", 100*time.Millisecond, panicTask, nil)
	assert.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	// 检查是否有panic恢复日志
	if len(mockLogger.Errors) == 0 {
		t.Fatalf("expected panic recovery log, got none")
	}
	isPanic := false
	for _, err := range mockLogger.Errors {
		isPanic = strings.Contains(err, "timer Single call panic err")
		if isPanic {
			break
		}
	}
	assert.True(t, isPanic)

}

// 测试不同时间类型的任务
func TestSingleTimer_DifferentJobTypes(t *testing.T) {
	ctx := context.Background()
	timer := timerx.InitSingle(ctx, timerx.WithLocation(time.UTC))
	defer timer.Stop()

	var counts struct {
		month  int32
		week   int32
		day    int32
		hour   int32
		minute int32
		space  int32
	}

	now := time.Now().UTC()

	// 月任务（下个月同一天）
	_, err := timer.EveryMonth(ctx, "month-job", now.Day(), now.Hour(), now.Minute(), now.Second()+1,
		func(ctx context.Context, data interface{}) error {
			atomic.AddInt32(&counts.month, 1)
			return nil
		}, nil)
	assert.NoError(t, err)

	// 周任务（下周同一天）
	_, err = timer.EveryWeek(ctx, "week-job", now.Weekday(), now.Hour(), now.Minute(), now.Second()+1,
		func(ctx context.Context, data interface{}) error {
			atomic.AddInt32(&counts.week, 1)
			return nil
		}, nil)
	assert.NoError(t, err)

	// 间隔任务（立即执行）
	_, err = timer.EverySpace(ctx, "space-job", 100*time.Millisecond,
		func(ctx context.Context, data interface{}) error {
			atomic.AddInt32(&counts.space, 1)
			return nil
		}, nil)
	assert.NoError(t, err)

	time.Sleep(time.Second)

	// 只有间隔任务应该执行
	assert.Equal(t, int32(9), atomic.LoadInt32(&counts.space))
	assert.Equal(t, int32(1), atomic.LoadInt32(&counts.month))
	assert.Equal(t, int32(1), atomic.LoadInt32(&counts.week))
}

// 测试上下文取消
func TestSingleTimer_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mockLogger := &MockLogger{}

	timer := timerx.InitSingle(ctx, timerx.WithLogger(mockLogger))

	var executionCount int32
	_, err := timer.EverySpace(ctx, "cancel-test", 100*time.Millisecond,
		func(ctx context.Context, data interface{}) error {
			atomic.AddInt32(&executionCount, 1)
			return nil
		}, nil)
	assert.NoError(t, err)

	// 让任务执行一次
	time.Sleep(150 * time.Millisecond)
	initialCount := atomic.LoadInt32(&executionCount)

	// 取消上下文
	cancel()
	time.Sleep(100 * time.Millisecond) // 等待停止

	// 检查是否停止了执行
	finalCount := atomic.LoadInt32(&executionCount)
	assert.Equal(t, initialCount, finalCount) // 计数不应该再增加

	// 检查是否有停止日志
	assert.Contains(t, mockLogger.Infos, "timer: context cancelled, stopping timer loop")
}

// 测试扩展数据传递
func TestSingleTimer_ExtendData(t *testing.T) {
	ctx := context.Background()
	timer := timerx.InitSingle(ctx)
	defer timer.Stop()

	type TestData struct {
		Message string
		Count   int
	}

	testData := &TestData{Message: "hello", Count: 42}
	var receivedData *TestData

	_, err := timer.EverySpace(ctx, "data-test", 100*time.Millisecond,
		func(ctx context.Context, data interface{}) error {
			fmt.Println("data:", data)
			if data != nil {
				receivedData = data.(*TestData)
			}
			return nil
		}, testData)
	assert.NoError(t, err)

	time.Sleep(time.Second)

	t.Logf("receivedData: %+v", receivedData)

	assert.NotNil(t, receivedData)
	assert.Equal(t, "hello", receivedData.Message)
	assert.Equal(t, 42, receivedData.Count)
}

// 测试任务删除
func TestSingleTimer_TaskDeletion(t *testing.T) {
	ctx := context.Background()
	timer := timerx.InitSingle(ctx)
	defer timer.Stop()

	var executionCount int32

	// 添加多个任务
	index1, err := timer.EverySpace(ctx, "task-1", 100*time.Millisecond,
		func(ctx context.Context, data interface{}) error {
			atomic.AddInt32(&executionCount, 1)
			return nil
		}, nil)
	assert.NoError(t, err)

	index2, err := timer.EverySpace(ctx, "task-2", 100*time.Millisecond,
		func(ctx context.Context, data interface{}) error {
			atomic.AddInt32(&executionCount, 1)
			return nil
		}, nil)
	assert.NoError(t, err)

	assert.Equal(t, 2, timer.TaskCount())

	// 删除一个任务
	timer.Del(index1)
	assert.Equal(t, 1, timer.TaskCount())

	// 等待执行
	time.Sleep(200 * time.Millisecond)
	count := atomic.LoadInt32(&executionCount)

	// 应该只有task-2执行
	assert.True(t, count >= 1 && count <= 2)

	// 删除另一个任务
	timer.Del(index2)
	assert.Equal(t, 0, timer.TaskCount())
}

// 测试GetNextTime函数（需要根据实际实现调整）
func TestGetNextTime2(t *testing.T) {
	now := time.Now().UTC()

	// 测试间隔任务
	jobData := timerx.JobData{
		JobType:      timerx.JobTypeInterval,
		IntervalTime: time.Minute,
		// CreateTime:   now,
		BaseTime: now,
	}

	tt := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, time.UTC)

	nextTime, err := timerx.GetNextTime(now, jobData)
	assert.NoError(t, err)
	assert.WithinDuration(t, tt.Add(time.Minute), *nextTime, time.Second)
}

// 基准测试
func BenchmarkSingleTimer_AddAndExecute(b *testing.B) {
	ctx := context.Background()
	timer := timerx.InitSingle(ctx)
	defer timer.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		timer.EverySpace(ctx, fmt.Sprintf("bench-%d", i), time.Millisecond,
			func(ctx context.Context, data interface{}) error {
				return nil
			}, nil)
	}
}

// 测试日志记录
func TestSingleTimer_Logging(t *testing.T) {
	ctx := context.Background()
	mockLogger := &MockLogger{}

	timer := timerx.InitSingle(ctx, timerx.WithLogger(mockLogger))
	defer timer.Stop()

	// 添加会panic的任务
	_, err := timer.EverySpace(ctx, "logging-test", 100*time.Millisecond,
		func(ctx context.Context, data interface{}) error {
			panic("test panic for logging")
		}, nil)
	assert.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	// 检查日志记录
	assert.NotEmpty(t, mockLogger.Errors)

	if len(mockLogger.Errors) == 0 {
		t.Fatalf("expected panic recovery log, got none")
	}
	isPanic := false
	for _, err := range mockLogger.Errors {
		isPanic = strings.Contains(err, "test panic for logging")
	}
	assert.True(t, isPanic)

}

// 测试时区处理
func TestSingleTimer_Timezone(t *testing.T) {
	// 测试不同时区
	locations := []*time.Location{
		time.UTC,
		time.FixedZone("TEST+8", 8*60*60),
		time.FixedZone("TEST-5", -5*60*60),
	}

	for _, loc := range locations {
		t.Run(loc.String(), func(t *testing.T) {
			ctx := context.Background()
			timer := timerx.InitSingle(ctx, timerx.WithLocation(loc))
			defer timer.Stop()

			var executed bool
			// now := time.Now().In(loc)

			// 添加下一秒执行的任务
			_, err := timer.EverySpace(ctx, "tz-test", time.Second,
				func(ctx context.Context, data interface{}) error {
					fmt.Println("executed in location:", loc)
					executed = true
					return nil
				}, nil)
			assert.NoError(t, err)

			time.Sleep(5 * time.Second)
			assert.True(t, executed)
		})
	}
}
