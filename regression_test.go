package timerx

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// 回归测试：依赖本地 Redis（localhost:6379 密码 123456）

type silentLogger struct{}

func (silentLogger) Infof(ctx context.Context, format string, v ...any)  {}
func (silentLogger) Errorf(ctx context.Context, format string, v ...any) {}

type onceWorkerFunc func(ctx context.Context, taskType OnceTaskType, taskId string, attachData any) *OnceWorkerResp

func (f onceWorkerFunc) Worker(ctx context.Context, taskType OnceTaskType, taskId string, attachData any) *OnceWorkerResp {
	return f(ctx, taskType, taskId, attachData)
}

// 返回 Redis 客户端并清理该前缀下的 key
func regressionRedis(t *testing.T, keyPrefix string) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: "123456"})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available: %v", err)
	}
	clean := func() {
		keys, _ := client.Keys(ctx, fmt.Sprintf("timer:{%s}:*", keyPrefix)).Result()
		if len(keys) > 0 {
			client.Del(ctx, keys...)
		}
	}
	clean()
	t.Cleanup(func() {
		clean()
		client.Close()
	})
	return client
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout after %v: %s", timeout, msg)
}

// 未到期的任务也要参与下次唤醒时间的计算，不能晚执行
func TestRegression_SingleRunsOnTime(t *testing.T) {
	s := InitSingle(context.Background(), WithLogger(silentLogger{}))
	defer s.Stop()

	now := time.Now()
	wantA := now.Add(2 * time.Second).Truncate(time.Second)
	wantB := now.Add(4 * time.Second).Truncate(time.Second)

	var mu sync.Mutex
	ran := map[string]time.Time{}
	call := func(ctx context.Context, data any) error {
		mu.Lock()
		defer mu.Unlock()
		if _, ok := ran[data.(string)]; !ok {
			ran[data.(string)] = time.Now()
		}
		return nil
	}

	if _, err := s.EveryMinute(context.Background(), "a", wantA.Second(), call, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EveryMinute(context.Background(), "b", wantB.Second(), call, "b"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Until(wantB) + time.Second)

	mu.Lock()
	defer mu.Unlock()
	for id, want := range map[string]time.Time{"a": wantA, "b": wantB} {
		got, ok := ran[id]
		if !ok {
			t.Errorf("task %s not executed", id)
			continue
		}
		if late := got.Sub(want); late < 0 || late > 500*time.Millisecond {
			t.Errorf("task %s executed %v after schedule, want within 500ms", id, late)
		}
	}
}

func TestRegression_StopTwice(t *testing.T) {
	s := InitSingle(context.Background(), WithLogger(silentLogger{}))
	s.Stop()
	s.Stop()

	client := regressionRedis(t, "reg_stop")

	c, err := InitCluster(context.Background(), client, "reg_stop", WithLogger(silentLogger{}))
	if err != nil {
		t.Fatal(err)
	}
	c.Stop()
	c.Stop()

	o, err := InitOnce(context.Background(), client, "reg_stop", onceWorkerFunc(func(ctx context.Context, taskType OnceTaskType, taskId string, attachData any) *OnceWorkerResp {
		return nil
	}), WithLogger(silentLogger{}))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	begin := time.Now()
	o.Close()
	if cost := time.Since(begin); cost > 2*time.Second {
		t.Errorf("Close cost %v, want < 2s", cost)
	}
	o.Close()
}

// 延迟为0代表立即执行
func TestRegression_OnceZeroDelay(t *testing.T) {
	client := regressionRedis(t, "reg_zero")
	ctx := context.Background()

	var mu sync.Mutex
	ran := map[string]bool{}
	o, err := InitOnce(ctx, client, "reg_zero", onceWorkerFunc(func(ctx context.Context, taskType OnceTaskType, taskId string, attachData any) *OnceWorkerResp {
		mu.Lock()
		ran[taskId] = true
		mu.Unlock()
		return nil
	}), WithLogger(silentLogger{}))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	if err := o.Save(ctx, "t", "save", 0, nil); err != nil {
		t.Errorf("Save delay=0 err:%v", err)
	}
	if err := o.Create(ctx, "t", "create", 0, nil); err != nil {
		t.Errorf("Create delay=0 err:%v", err)
	}
	if err := o.CreateByTime(ctx, "t", "byTime", time.Now(), nil); err != nil {
		t.Errorf("CreateByTime now err:%v", err)
	}
	if err := o.SaveByTime(ctx, "t", "zero", time.Time{}, nil); err != ErrExecuteTime {
		t.Errorf("SaveByTime zero time err:%v, want ErrExecuteTime", err)
	}
	if err := o.SaveByList(ctx, "t", "list", []time.Time{time.Now().Add(-time.Minute)}, nil, 0); err != ErrDelayTime {
		t.Errorf("SaveByList expired err:%v, want ErrDelayTime", err)
	}

	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return ran["save"] && ran["create"] && ran["byTime"]
	}, "zero delay tasks not executed")
}

// Retry 不填 DelayTime 代表立即重试，不能被丢弃
func TestRegression_OnceRetryWithoutDelay(t *testing.T) {
	client := regressionRedis(t, "reg_retry")
	ctx := context.Background()

	var runs int32
	o, err := InitOnce(ctx, client, "reg_retry", onceWorkerFunc(func(ctx context.Context, taskType OnceTaskType, taskId string, attachData any) *OnceWorkerResp {
		if atomic.AddInt32(&runs, 1) < 3 {
			return &OnceWorkerResp{Retry: true}
		}
		return nil
	}), WithLogger(silentLogger{}))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	if err := o.Save(ctx, "t", "retry", 100*time.Millisecond, "x"); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 5*time.Second, func() bool { return atomic.LoadInt32(&runs) == 3 }, "task not retried 3 times")

	waitFor(t, 2*time.Second, func() bool {
		data, _ := o.Get("t", "retry")
		return data == nil
	}, "task data not deleted after finish")
}

// 执行期间重新保存同一个任务，执行结束后不能把新任务删掉
func TestRegression_OnceSaveDuringExecution(t *testing.T) {
	client := regressionRedis(t, "reg_resave")
	ctx := context.Background()

	var runs int32
	var o *Once
	var saveErr error
	o, err := InitOnce(ctx, client, "reg_resave", onceWorkerFunc(func(ctx context.Context, taskType OnceTaskType, taskId string, attachData any) *OnceWorkerResp {
		if atomic.AddInt32(&runs, 1) == 1 {
			saveErr = o.Save(ctx, taskType, taskId, time.Second, "second")
			if err := o.Create(ctx, taskType, taskId, time.Second, "third"); err != ErrTaskExists {
				t.Errorf("Create during execution err:%v, want ErrTaskExists", err)
			}
		}
		return nil
	}), WithLogger(silentLogger{}))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	if err := o.Save(ctx, "t", "x", 100*time.Millisecond, "first"); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 5*time.Second, func() bool { return atomic.LoadInt32(&runs) == 2 }, "task saved during execution was lost")
	if saveErr != nil {
		t.Errorf("Save during execution err:%v", saveErr)
	}
}

// 并发 Create 同一个ID：一个成功，其余返回 ErrTaskExists
func TestRegression_OnceConcurrentCreate(t *testing.T) {
	client := regressionRedis(t, "reg_create")
	ctx := context.Background()

	o, err := InitOnce(ctx, client, "reg_create", nil, WithLogger(silentLogger{}), WithAllowNilFunc(true))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	var success, exists, other int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch err := o.Create(ctx, "t", "same", time.Minute, nil); err {
			case nil:
				atomic.AddInt32(&success, 1)
			case ErrTaskExists:
				atomic.AddInt32(&exists, 1)
			default:
				atomic.AddInt32(&other, 1)
				t.Errorf("unexpected err:%v", err)
			}
		}()
	}
	wg.Wait()

	if success != 1 || exists != 19 || other != 0 {
		t.Errorf("success=%d exists=%d other=%d, want 1/19/0", success, exists, other)
	}
}

// 关闭时：没有空闲 worker 的任务必须留在 Redis，不能被取出后丢弃
func TestRegression_OnceCloseKeepsPendingTasks(t *testing.T) {
	client := regressionRedis(t, "reg_close")
	ctx := context.Background()

	var started int32
	o, err := InitOnce(ctx, client, "reg_close", onceWorkerFunc(func(ctx context.Context, taskType OnceTaskType, taskId string, attachData any) *OnceWorkerResp {
		atomic.AddInt32(&started, 1)
		<-ctx.Done()
		return nil
	}), WithLogger(silentLogger{}), WithMaxWorkers(1))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if err := o.Save(ctx, "t", fmt.Sprintf("id%d", i), 0, i); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 3*time.Second, func() bool {
		n, _ := client.LLen(ctx, o.listKey).Result()
		return atomic.LoadInt32(&started) == 1 && n == 2
	}, "expect 1 running and 2 pending")

	begin := time.Now()
	o.Close()
	if cost := time.Since(begin); cost > 2*time.Second {
		t.Errorf("Close cost %v, want < 2s", cost)
	}

	time.Sleep(500 * time.Millisecond)
	if n := atomic.LoadInt32(&started); n != 1 {
		t.Errorf("started=%d, want 1", n)
	}
	if n, _ := client.LLen(ctx, o.listKey).Result(); n != 2 {
		t.Errorf("pending tasks in list=%d, want 2", n)
	}
}

// 实例崩溃后，它已取出但未处理完的任务由 leader 放回并重新执行
func TestRegression_OnceRecoverTaskFromDeadInstance(t *testing.T) {
	client := regressionRedis(t, "reg_dead")
	ctx := context.Background()

	// 模拟崩溃的实例：任务在它的处理中队列，且早已不再上报存活
	publisher, err := InitOnce(ctx, client, "reg_dead", nil, WithLogger(silentLogger{}), WithAllowNilFunc(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.Save(ctx, "t", "lost", time.Hour, "x"); err != nil {
		t.Fatal(err)
	}
	member := publisher.buildRedisKey("t", "lost")
	client.ZRem(ctx, publisher.zsetKey, member)
	client.LPush(ctx, publisher.queue.processingPrefix+"dead-instance", member)
	client.ZAdd(ctx, publisher.queue.instancesKey, redis.Z{Score: 1, Member: "dead-instance"})

	var runs int32
	o, err := InitOnce(ctx, client, "reg_dead", onceWorkerFunc(func(ctx context.Context, taskType OnceTaskType, taskId string, attachData any) *OnceWorkerResp {
		if taskId == "lost" {
			atomic.AddInt32(&runs, 1)
		}
		return nil
	}), WithLogger(silentLogger{}))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	waitFor(t, 15*time.Second, func() bool { return atomic.LoadInt32(&runs) == 1 }, "task of dead instance not recovered")

	waitFor(t, 2*time.Second, func() bool {
		n, _ := client.ZScore(ctx, o.queue.instancesKey, "dead-instance").Result()
		return n == 0
	}, "dead instance not removed")
}

func TestRegression_TaskQueue(t *testing.T) {
	client := regressionRedis(t, "reg_queue")
	ctx := context.Background()

	newQueue := func(instanceId string) *taskQueue {
		return newTaskQueue(client, silentLogger{}, "timer:{reg_queue}:list", "timer:{reg_queue}:instances", "timer:{reg_queue}:processing:", instanceId)
	}
	a, b := newQueue("a"), newQueue("b")

	if a.ready() {
		t.Error("queue should not be ready before heartbeat")
	}
	a.heartbeat(ctx)
	b.heartbeat(ctx)
	if !a.ready() {
		t.Fatal("queue should be ready after heartbeat")
	}

	listLen := func() int64 {
		n, _ := client.LLen(ctx, a.listKey).Result()
		return n
	}
	processingLen := func(q *taskQueue) int64 {
		n, _ := client.LLen(ctx, q.processingKey).Result()
		return n
	}

	// 先进先出
	client.LPush(ctx, a.listKey, "t1")
	client.LPush(ctx, a.listKey, "t2")
	client.LPush(ctx, a.listKey, "t3")
	for _, want := range []string{"t1", "t2", "t3"} {
		got, err := a.pop(ctx)
		if err != nil || got != want {
			t.Fatalf("pop got %q err:%v, want %q", got, err, want)
		}
	}
	if _, err := a.pop(ctx); err != redis.Nil {
		t.Errorf("pop empty err:%v, want redis.Nil", err)
	}

	// ack 移除，nack 放回
	a.ack(ctx, "t1")
	a.nack(ctx, "t2")
	if processingLen(a) != 1 || listLen() != 1 {
		t.Errorf("processing=%d list=%d, want 1/1", processingLen(a), listLen())
	}

	// 有任务在执行时不整理
	a.reconcile(ctx)
	if processingLen(a) != 1 {
		t.Errorf("reconcile moved inflight task")
	}

	// 存活的实例不回收
	b.recoverDead(ctx)
	if processingLen(a) != 1 || listLen() != 1 {
		t.Errorf("alive instance recovered: processing=%d list=%d", processingLen(a), listLen())
	}

	// 失活的实例：任务放回就绪队列，实例被移除
	client.ZAdd(ctx, a.instancesKey, redis.Z{Score: 1, Member: "a"})
	b.recoverDead(ctx)
	if processingLen(a) != 0 || listLen() != 2 {
		t.Errorf("dead instance not recovered: processing=%d list=%d", processingLen(a), listLen())
	}
	if exists, _ := client.ZScore(ctx, a.instancesKey, "a").Result(); exists != 0 {
		t.Errorf("dead instance not removed")
	}

	// 没有任务在执行时，遗留的任务放回
	client.LPush(ctx, b.processingKey, "left1")
	b.reconcile(ctx)
	if processingLen(b) != 1 {
		t.Errorf("reconcile should wait for %d idle rounds", queueReconcileRounds)
	}
	b.idleRounds = queueReconcileRounds
	b.reconcile(ctx)
	if processingLen(b) != 0 || listLen() != 3 {
		t.Errorf("reconcile requeue: processing=%d list=%d, want 0/3", processingLen(b), listLen())
	}
}

// Cluster 停止时：没有空闲 worker 的任务必须留在 Redis
func TestRegression_ClusterStopKeepsPendingTasks(t *testing.T) {
	client := regressionRedis(t, "reg_cstop")
	ctx := context.Background()

	c, err := InitCluster(ctx, client, "reg_cstop", WithLogger(silentLogger{}), WithMaxWorkers(1))
	if err != nil {
		t.Fatal(err)
	}

	var started int32
	call := func(ctx context.Context, data any) error {
		atomic.AddInt32(&started, 1)
		<-ctx.Done()
		return nil
	}
	// 测试期间不会到期的任务，手动放入就绪队列
	hour := (time.Now().Hour() + 12) % 24
	for _, id := range []string{"t1", "t2", "t3"} {
		if err := c.EveryDay(ctx, id, hour, 0, 0, call, nil); err != nil {
			t.Fatal(err)
		}
		client.LPush(ctx, c.listKey, id)
	}

	waitFor(t, 3*time.Second, func() bool {
		n, _ := client.LLen(ctx, c.listKey).Result()
		return atomic.LoadInt32(&started) == 1 && n == 2
	}, "expect 1 running and 2 pending")

	begin := time.Now()
	c.Stop()
	if cost := time.Since(begin); cost > 2*time.Second {
		t.Errorf("Stop cost %v, want < 2s", cost)
	}

	time.Sleep(500 * time.Millisecond)
	if n := atomic.LoadInt32(&started); n != 1 {
		t.Errorf("started=%d, want 1", n)
	}
	if n, _ := client.LLen(ctx, c.listKey).Result(); n != 2 {
		t.Errorf("pending tasks in list=%d, want 2", n)
	}
}

// 执行期间重新保存且回调要求重试：以新保存的为准
func TestRegression_OnceSaveDuringExecutionWithRetry(t *testing.T) {
	client := regressionRedis(t, "reg_resave_retry")
	ctx := context.Background()

	var mu sync.Mutex
	var got []any
	var o *Once
	o, err := InitOnce(ctx, client, "reg_resave_retry", onceWorkerFunc(func(ctx context.Context, taskType OnceTaskType, taskId string, attachData any) *OnceWorkerResp {
		mu.Lock()
		got = append(got, attachData)
		first := len(got) == 1
		mu.Unlock()
		if first {
			if err := o.Save(ctx, taskType, taskId, 0, "second"); err != nil {
				t.Errorf("Save during execution err:%v", err)
			}
			// 等新任务进入就绪队列
			time.Sleep(500 * time.Millisecond)
			return &OnceWorkerResp{Retry: true, AttachData: "retry"}
		}
		return nil
	}), WithLogger(silentLogger{}))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	if err := o.Save(ctx, "t", "x", 0, "first"); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) >= 2
	}, "task saved during execution was lost")
	time.Sleep(time.Second)

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Errorf("executed with %v, want [first second]", got)
	}
}

// Redis Cluster（localhost:7001-7003，不可用时跳过）：处理中队列与就绪队列在同一个 slot
func TestRegression_RedisCluster(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:       []string{"localhost:7001", "localhost:7002", "localhost:7003"},
		DialTimeout: time.Second,
	})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis cluster not available: %v", err)
	}

	const keyPrefix = "reg_rc"
	clean := func() {
		client.ForEachMaster(ctx, func(ctx context.Context, c *redis.Client) error {
			keys, _ := c.Keys(ctx, fmt.Sprintf("timer:{%s}:*", keyPrefix)).Result()
			for _, key := range keys {
				client.Del(ctx, key)
			}
			return nil
		})
	}
	clean()
	defer clean()

	var onceRuns int32
	o, err := InitOnce(ctx, client, keyPrefix, onceWorkerFunc(func(ctx context.Context, taskType OnceTaskType, taskId string, attachData any) *OnceWorkerResp {
		if atomic.AddInt32(&onceRuns, 1) == 1 {
			return &OnceWorkerResp{Retry: true}
		}
		return nil
	}), WithLogger(silentLogger{}))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	// 失活实例遗留的任务
	if err := o.Save(ctx, "t", "lost", time.Hour, nil); err != nil {
		t.Fatal(err)
	}
	member := o.buildRedisKey("t", "lost")
	client.ZRem(ctx, o.zsetKey, member)
	client.LPush(ctx, o.queue.processingPrefix+"dead-instance", member)
	client.ZAdd(ctx, o.queue.instancesKey, redis.Z{Score: 1, Member: "dead-instance"})

	for i := 0; i < 20; i++ {
		if err := o.Save(ctx, "t", fmt.Sprintf("id%d", i), 0, i); err != nil {
			t.Fatal(err)
		}
	}

	c, err := InitCluster(ctx, client, keyPrefix, WithLogger(silentLogger{}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	var clusterRuns int32
	if err := c.EverySpace(ctx, "space", time.Second, func(ctx context.Context, data any) error {
		atomic.AddInt32(&clusterRuns, 1)
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}

	// 20 个任务 + 第一个任务重试 1 次 + 失活实例遗留的 1 个
	waitFor(t, 20*time.Second, func() bool { return atomic.LoadInt32(&onceRuns) == 22 }, "once tasks not all executed on redis cluster")
	waitFor(t, 10*time.Second, func() bool { return atomic.LoadInt32(&clusterRuns) >= 3 }, "cluster task not executed on redis cluster")

	for _, q := range []*taskQueue{o.queue, c.queue} {
		waitFor(t, 3*time.Second, func() bool {
			n, err := client.LLen(ctx, q.processingKey).Result()
			return err == nil && n == 0
		}, "processing list not empty after ack")
	}
}

// 让匹配的 EVAL 失败指定次数，模拟 Redis 故障
type failEvalHook struct {
	arg    string // EVAL 参数中包含该值则失败
	remain atomic.Int32
}

func (h *failEvalHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *failEvalHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "eval" && h.remain.Load() > 0 {
			for _, arg := range cmd.Args() {
				if arg == h.arg {
					h.remain.Add(-1)
					err := fmt.Errorf("mock redis error")
					cmd.SetErr(err)
					return err
				}
			}
		}
		return next(ctx, cmd)
	}
}

func (h *failEvalHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// 重试写入 Redis 失败时任务不能丢，放回就绪队列重新执行
func TestRegression_OnceRetrySaveFailure(t *testing.T) {
	client := regressionRedis(t, "reg_retry_fail")
	ctx := context.Background()

	hook := &failEvalHook{arg: string(saveModeUnchanged)}
	hook.remain.Store(1)
	client.AddHook(hook)

	var runs int32
	o, err := InitOnce(ctx, client, "reg_retry_fail", onceWorkerFunc(func(ctx context.Context, taskType OnceTaskType, taskId string, attachData any) *OnceWorkerResp {
		if atomic.AddInt32(&runs, 1) < 3 {
			return &OnceWorkerResp{Retry: true}
		}
		return nil
	}), WithLogger(silentLogger{}))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	if err := o.Save(ctx, "t", "x", 0, nil); err != nil {
		t.Fatal(err)
	}

	// 第1次执行后重试保存失败 -> 重新执行 -> 重试 -> 完成
	waitFor(t, 8*time.Second, func() bool { return atomic.LoadInt32(&runs) == 3 }, "task lost after retry save failure")
	if hook.remain.Load() != 0 {
		t.Errorf("retry save failure was not injected")
	}
}

// 已到期、正在等待 worker 的任务不能被 Create 覆盖
func TestRegression_OnceCreateWhileQueued(t *testing.T) {
	client := regressionRedis(t, "reg_create_queued")
	ctx := context.Background()

	release := make(chan struct{})
	var mu sync.Mutex
	got := map[string]any{}
	o, err := InitOnce(ctx, client, "reg_create_queued", onceWorkerFunc(func(ctx context.Context, taskType OnceTaskType, taskId string, attachData any) *OnceWorkerResp {
		if taskId == "busy" {
			<-release
			return nil
		}
		mu.Lock()
		got[taskId] = attachData
		mu.Unlock()
		return nil
	}), WithLogger(silentLogger{}), WithMaxWorkers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	if err := o.Save(ctx, "t", "busy", 0, nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		n, _ := client.LLen(ctx, o.queue.processingKey).Result()
		return n == 1
	}, "busy task not started")

	if err := o.Create(ctx, "t", "x", 0, "first"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		n, _ := client.LLen(ctx, o.listKey).Result()
		return n == 1
	}, "task not queued")

	if err := o.Create(ctx, "t", "x", 0, "second"); err != ErrTaskExists {
		t.Errorf("Create queued task err:%v, want ErrTaskExists", err)
	}
	close(release)

	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return got["x"] != nil
	}, "queued task not executed")
	mu.Lock()
	defer mu.Unlock()
	if got["x"] != "first" {
		t.Errorf("executed with %v, want first", got["x"])
	}
}

// 就绪队列里的旧记录不能让重新保存到以后执行的任务提前执行
func TestRegression_OnceStaleEntryDoesNotRunEarly(t *testing.T) {
	client := regressionRedis(t, "reg_stale")
	ctx := context.Background()

	var runs int32
	o, err := InitOnce(ctx, client, "reg_stale", onceWorkerFunc(func(ctx context.Context, taskType OnceTaskType, taskId string, attachData any) *OnceWorkerResp {
		atomic.AddInt32(&runs, 1)
		return nil
	}), WithLogger(silentLogger{}))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	if err := o.Save(ctx, "t", "x", time.Hour, "later"); err != nil {
		t.Fatal(err)
	}
	// 失活实例遗留的旧记录被放回就绪队列
	member := o.buildRedisKey("t", "x")
	client.LPush(ctx, o.queue.processingPrefix+"dead-instance", member)
	client.ZAdd(ctx, o.queue.instancesKey, redis.Z{Score: 1, Member: "dead-instance"})

	waitFor(t, 15*time.Second, func() bool {
		_, err := client.ZScore(ctx, o.queue.instancesKey, "dead-instance").Result()
		return err == redis.Nil
	}, "dead instance not recovered")
	time.Sleep(2 * time.Second)

	if n := atomic.LoadInt32(&runs); n != 0 {
		t.Errorf("task executed %d times before its time", n)
	}
	if _, err := client.ZScore(ctx, o.zsetKey, member).Result(); err != nil {
		t.Errorf("schedule lost err:%v", err)
	}
	if data, _ := o.Get("t", "x"); data == nil {
		t.Errorf("task data lost")
	}
}

// 任务结束（panic、时间列表执行完）后数据被清理，同ID可以再次 Create
func TestRegression_OnceFinishedTaskCanBeCreatedAgain(t *testing.T) {
	client := regressionRedis(t, "reg_finished")
	ctx := context.Background()

	var runs int32
	o, err := InitOnce(ctx, client, "reg_finished", onceWorkerFunc(func(ctx context.Context, taskType OnceTaskType, taskId string, attachData any) *OnceWorkerResp {
		atomic.AddInt32(&runs, 1)
		if taskId == "panic" {
			panic("mock panic")
		}
		// 时间列表只有一个时间，执行完后没有下一次
		return &OnceWorkerResp{Retry: true}
	}), WithLogger(silentLogger{}))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	if err := o.Create(ctx, "t", "panic", 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := o.CreateByList(ctx, "t", "list", []time.Time{time.Now().Add(200 * time.Millisecond)}, nil); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 5*time.Second, func() bool { return atomic.LoadInt32(&runs) == 2 }, "tasks not executed")

	for _, id := range []string{"panic", "list"} {
		waitFor(t, 3*time.Second, func() bool {
			return o.Create(ctx, "t", id, time.Hour, nil) == nil
		}, "finished task "+id+" can not be created again")
	}
}
