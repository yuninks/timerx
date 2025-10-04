package timerx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/yuninks/lockx"
	"github.com/yuninks/timerx/heartbeat"
	"github.com/yuninks/timerx/leader"
	"github.com/yuninks/timerx/logger"
	"github.com/yuninks/timerx/priority"
)

// 功能描述
// 1. 任务可以多节点发布
// 2. 每个任务的执行在全局仅会执行一次
// 3. 任务执行失败支持快捷重新加入队列

// 单次的任务队列
type Once struct {
	ctx              context.Context       // ctx
	cancel           context.CancelFunc    // cancel
	logger           logger.Logger         // 日志
	zsetKey          string                // 任务列表 有序集合
	listKey          string                // 列表
	globalLockPrefix string                // 全局锁的前缀
	redis            redis.UniversalClient // Redis
	worker           Callback              // 回调
	keyPrefix        string                //
	priority         *priority.Priority    // 全局优先级
	usePriority      bool                  //
	batchSize        int                   // 每批大小
	wg               sync.WaitGroup        //

	stopChan   chan struct{} //
	instanceId string        // 实例ID

	leader    *leader.Leader // 领导
	heartbeat *heartbeat.HeartBeat

	executeInfoKey string        // 执行情况的key 有序集合
	keySeparator   string        // 分割符
	timeout        time.Duration // 任务执行超时时间

	maxRetryCount int // 最大重试次数 0代表不限
}

type OnceWorkerResp struct {
	Retry      bool          // 是否重试 true
	DelayTime  time.Duration // 等待时间
	AttachData any           // 附加数据
}

type OnceTaskType string

// 需要考虑执行失败重新放入队列的情况
type Callback interface {
	// 任务执行
	// @param jobType string 任务类型
	// @param uniTaskId string 任务唯一标识
	// @param data interface{} 任务数据
	// @return WorkerCode 任务执行结果
	// @return time.Duration 任务执行时间间隔
	Worker(ctx context.Context, taskType OnceTaskType, taskId string, attachData any) *OnceWorkerResp
}

type extendData struct {
	Delay      time.Duration
	Data       any
	RetryCount int // 重试次数
}

// 初始化
func InitOnce(ctx context.Context, re redis.UniversalClient, keyPrefix string, call Callback, opts ...Option) (*Once, error) {
	op := newOptions(opts...)

	if re == nil {
		op.logger.Errorf(ctx, "redis client is nil")
		return nil, errors.New("redis client is nil")
	}

	ctx, cancel := context.WithCancel(ctx)

	u, _ := uuid.NewV7()

	wo := &Once{
		ctx:              ctx,
		cancel:           cancel,
		logger:           op.logger,
		zsetKey:          "timer:once_zsetkey" + keyPrefix,
		listKey:          "timer:once_listkey" + keyPrefix,
		executeInfoKey:   "timer:once_executeInfoKey" + keyPrefix,
		globalLockPrefix: "timer:once_globalLockPrefix" + keyPrefix,
		usePriority:      op.usePriority,
		redis:            re,
		worker:           call,
		keyPrefix:        keyPrefix,
		batchSize:        op.batchSize,
		stopChan:         make(chan struct{}),
		instanceId:       u.String(),
		keySeparator:     "[:]",
		timeout:          op.timeout,
		maxRetryCount:    op.maxRetryCount,
	}

	// 初始化优先级
	if wo.usePriority {
		pri, err := priority.InitPriority(
			ctx,
			re,
			keyPrefix,
			op.priorityVal,
			priority.WithLogger(wo.logger),
		)
		if err != nil {
			wo.logger.Errorf(ctx, "InitPriority err:%v", err)
			return nil, err
		}
		wo.priority = pri
	}

	// 初始化leader
	le, err := leader.InitLeader(
		ctx,
		re,
		wo.keyPrefix,
		leader.WithLogger(wo.logger),
		leader.WithPriority(wo.priority),
		leader.WithInstanceId(wo.instanceId),
	)
	if err != nil {
		wo.logger.Infof(ctx, "InitLeader err:%v", err)
		return nil, err
	}
	wo.leader = le

	// 初始化心跳
	heart, err := heartbeat.InitHeartBeat(
		ctx,
		re,
		wo.keyPrefix,
		heartbeat.WithInstanceId(wo.instanceId),
		heartbeat.WithLeader(wo.leader),
		heartbeat.WithLogger(wo.logger),
		heartbeat.WithPriority(wo.priority),
		heartbeat.WithSource("once"),
	)
	if err != nil {
		wo.logger.Errorf(ctx, "InitHeartBeat err:%v", err)
		return nil, err
	}
	wo.heartbeat = heart

	wo.startDaemon()

	return wo, nil
}

// Close 停止集群定时器
func (l *Once) Close() {
	close(l.stopChan)
	if l.usePriority && l.priority != nil {
		l.priority.Close()
	}
	if l.leader != nil {
		l.leader.Close()
	}
	l.heartbeat.Close()
	l.cancel()
	l.wg.Wait()
}

func (l *Once) startDaemon() {

	// 任务调度
	l.wg.Add(1)
	go l.scheduleTasks()

	// 任务执行
	l.wg.Add(1)
	go l.executeTasks()

	// 清理过期任务
	l.wg.Add(1)
	go l.cleanExecuteInfoLoop()

}

func (l *Once) cleanExecuteInfoLoop() {
	l.wg.Done()

	ticker := time.NewTicker(time.Minute * 5)
	defer ticker.Stop()

	for {
		select {
		case <-l.stopChan:
			return
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			if l.leader.IsLeader() {
				l.cleanExecuteInfo()
			}
		}
	}
}

// 清除过期任务
func (l *Once) cleanExecuteInfo() error {
	// 移除执行信息
	l.redis.ZRemRangeByScore(l.ctx, l.executeInfoKey, "0", strconv.FormatInt(time.Now().Add(-15*time.Minute).UnixMilli(), 10)).Err()
	return nil
}

// 任务调度 领导
func (l *Once) scheduleTasks() {
	defer l.wg.Done()

	timer := time.NewTicker(time.Millisecond * 200)
	defer timer.Stop()

	for {
		select {
		case <-l.stopChan:
			return
		case <-l.ctx.Done():
			return
		case <-timer.C:
			// 优先级
			if l.usePriority {
				if !l.priority.IsLatest(l.ctx) {
					continue
				}
			}
			// 领导
			if !l.leader.IsLeader() {
				continue
			}

			l.batchGetTasks()
		}
	}

}

// 任务执行
func (l *Once) executeTasks() {
	defer l.wg.Done()

	for {

		if l.usePriority {
			if !l.priority.IsLatest(l.ctx) {
				time.Sleep(time.Second * 5)
				continue
			}
		}

		select {
		case <-l.stopChan:
			return
		case <-l.ctx.Done():
			return
		default:

			keys, err := l.redis.BLPop(l.ctx, time.Second*10, l.listKey).Result()
			if err != nil {
				continue
			}

			go l.processTask(keys[1])

		}
	}

}

// 构建Redis key
func (l *Once) buildRedisKey(taskType OnceTaskType, taskId string) string {
	return fmt.Sprintf("%s%s%s", taskType, l.keySeparator, taskId)
}

// 解析Redis Key
func (l *Once) parseRedisKey(key string) (OnceTaskType, string, error) {
	parts := strings.Split(key, l.keySeparator)
	if len(parts) < 2 {
		return "", "", fmt.Errorf("invalid key format: %s", key)
	}
	return OnceTaskType(parts[0]), parts[1], nil
}

// 添加任务(覆盖)
// 重复插入就代表覆盖
// @param jobType string 任务类型
// @param uniTaskId string 任务唯一标识
// @param delayTime time.Duration 延迟时间
// @param attachData interface{} 附加数据
func (l *Once) Save(taskType OnceTaskType, taskId string, delayTime time.Duration, attachData interface{}) error {
	return l.save(taskType, taskId, delayTime, attachData, 0)
}

// 添加任务(覆盖)
// 重复插入就代表覆盖
func (w *Once) save(taskType OnceTaskType, taskId string, delayTime time.Duration, attachData interface{}, retryCount int) error {
	if delayTime <= 0 {
		return fmt.Errorf("delay time must be positive")
	}

	redisKey := w.buildRedisKey(taskType, taskId)
	executeTime := time.Now().Add(delayTime)

	ed := extendData{
		Delay:      delayTime,
		Data:       attachData,
		RetryCount: retryCount,
	}
	b, _ := json.Marshal(ed)

	// 使用事务确保原子性
	pipe := w.redis.TxPipeline()

	dataExpire := delayTime + time.Minute*30

	pipe.SetEX(w.ctx, w.keyPrefix+redisKey, b, dataExpire)
	pipe.ZAdd(w.ctx, w.zsetKey, &redis.Z{
		Score:  float64(executeTime.UnixMilli()),
		Member: redisKey,
	})
	_, err := pipe.Exec(w.ctx)
	if err != nil {
		w.logger.Errorf(w.ctx, "save task failed:%v", err)
		return err
	}

	return nil
}

// 添加任务(不覆盖)
func (l *Once) Create(taskType OnceTaskType, taskId string, delayTime time.Duration, attachData any) error {
	return l.create(taskType, taskId, delayTime, attachData, 0)
}

func (l *Once) create(taskType OnceTaskType, taskId string, delayTime time.Duration, attachData any, retryCount int) error {
	if delayTime <= 0 {
		return fmt.Errorf("delay time must be positive")
	}

	redisKey := l.buildRedisKey(taskType, taskId)

	score, err := l.redis.ZScore(l.ctx, l.zsetKey, redisKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return l.Save(taskType, taskId, delayTime, attachData)
		}
		l.logger.Errorf(l.ctx, "redis.ZScore err:%v", err)
		return err
	}
	if score > 0 {
		return fmt.Errorf("task already exists")
	}

	return l.save(taskType, taskId, delayTime, attachData, retryCount)
}

// 删除任务
func (w *Once) Delete(taskType OnceTaskType, taskId string) error {
	redisKey := w.buildRedisKey(taskType, taskId)

	pipe := w.redis.TxPipeline()
	pipe.Del(w.ctx, redisKey)
	pipe.ZRem(w.ctx, w.zsetKey, redisKey)

	_, err := pipe.Exec(w.ctx)
	if err != nil {
		w.logger.Errorf(w.ctx, "delete task failed:%v", err)
		return err
	}

	return nil
}

// 获取任务
func (l *Once) Get(taskType OnceTaskType, taskId string) {
	//
}

// 批量获取任务
func (l *Once) batchGetTasks() {
	script := `
	local tasks = redis.call('zrangebyscore', KEYS[1], 0, ARGV[1], 'LIMIT', 0, ARGV[2])
	if #tasks == 0 then return 0 end
	
	for i, task in ipairs(tasks) do
		redis.call('zrem', KEYS[1], task)
		redis.call('lpush', KEYS[2], task)
	end
	return #tasks
	`

	result, err := l.redis.Eval(
		l.ctx,
		script,
		[]string{l.zsetKey, l.listKey},
		time.Now().UnixMilli(),
		l.batchSize,
	).Result()
	if err != nil && err != redis.Nil {
		l.logger.Errorf(l.ctx, "batch get tasks failed: %s", err.Error())
		return
	}

	if count, ok := result.(int64); ok && count > 0 {
		l.logger.Infof(l.ctx, "moved %d tasks to ready queue", count)
	}
}

// 执行任务
func (l *Once) processTask(key string) {
	begin := time.Now()

	ctx, cancel := context.WithTimeout(l.ctx, l.timeout)
	defer cancel()

	u, _ := uuid.NewV7()

	ctx = context.WithValue(ctx, "trace_id", u.String())

	l.logger.Infof(ctx, "processTask start key:%s", key)

	taskType, taskId, err := l.parseRedisKey(key)
	if err != nil {
		l.logger.Errorf(ctx, "processTask parseRedisKey:%v key:%s", err, key)
		return
	}

	// 这里加一个全局锁
	lock, err := lockx.NewGlobalLock(ctx, l.redis, l.globalLockPrefix+key)
	if err != nil {
		l.logger.Errorf(ctx, "processTask timer:获取锁失败:%s", taskId)
		return
	}
	if b, err := lock.Lock(); !b {
		l.logger.Errorf(ctx, "processTask timer:获取锁失败:%s %+v", taskId, err)
		return
	}
	defer lock.Unlock()

	// 上报执行情况
	executeVal := fmt.Sprintf("%s|%s|%s|%s", key, l.instanceId, u.String(), begin.Format(time.RFC3339Nano))
	l.redis.ZAdd(ctx, l.executeInfoKey, &redis.Z{
		Score:  float64(begin.UnixMilli()),
		Member: executeVal,
	})

	defer func() {
		if err := recover(); err != nil {
			l.logger.Errorf(ctx, "processTask panic:%s stack:%s", err, string(debug.Stack()))
		}
	}()

	// 读取数据
	redisKey := l.keyPrefix + l.buildRedisKey(taskType, taskId)
	str, err := l.redis.Get(ctx, redisKey).Result()
	if err != nil {
		l.logger.Errorf(ctx, "processTask redis.Get key:%s err:%s", key, err)
		return
	}

	ed := extendData{}
	json.Unmarshal([]byte(str), &ed)

	resp := l.worker.Worker(ctx, taskType, taskId, ed.Data)
	l.logger.Infof(ctx, "processTask exec key:%s resp:%+v data:%s", key, resp, str)

	if resp == nil || !resp.Retry {
		// 完成 删除任务
		// 删除任务
		l.logger.Infof(ctx, "processTask delete key:%s", key)
		if err := l.Delete(taskType, taskId); err != nil {
			l.logger.Errorf(ctx, "processTask delete errprocessTask delete err:%v", err)
		}
		return
	}

	// 重新放入队列
	if err := l.handleRetry(ctx, taskType, taskId, &ed, resp); err != nil {
		l.logger.Errorf(ctx, "processTask handleRetry err:%v", err)
	}
}

func (l *Once) handleRetry(ctx context.Context, taskType OnceTaskType, taskId string,
	ed *extendData, resp *OnceWorkerResp) error {
	// 限制重试次数
	ed.RetryCount++
	if l.maxRetryCount > 0 && ed.RetryCount > l.maxRetryCount {
		l.logger.Infof(ctx, "handleRetry task exceeded retry limit: %s %s %d", taskType, taskId, l.maxRetryCount)
		return nil
	}

	// 更新延迟时间
	if resp.DelayTime > 0 {
		ed.Delay = resp.DelayTime
	}
	if resp.AttachData != nil {
		ed.Data = resp.AttachData
	}

	l.logger.Infof(ctx, "handleRetry retrying task: %s:%s, retry count: %d",
		taskType, taskId, ed.RetryCount)

	// 不覆盖的新建
	return l.create(taskType, taskId, ed.Delay, ed.Data, ed.RetryCount)
}
