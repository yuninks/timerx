package timerx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
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
	stopOnce   sync.Once     //
	instanceId string        // 实例ID
	queue      *taskQueue    // 就绪队列

	leader    *leader.Leader // 领导
	heartbeat *heartbeat.HeartBeat

	executeInfoKey string        // 执行情况的key 有序集合
	keySeparator   string        // 分割符
	timeout        time.Duration // 任务执行超时时间

	maxRunCount int           // 最大重试次数 0代表不限
	workerChan  chan struct{} // worker
	maxWorkers  int           // 最大worker数量
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
	TaskTimes []time.Time
	Data      any
	RunCount  int // 运行次数
	JobType   jobType
	Version   string `json:",omitempty"` // 每次保存都不同，用于识别执行期间任务是否被重新保存
}

type jobType string

const (
	jobTypeOnce = "once"
	jobTypeList = "list"
)

type createSource string

const (
	createSourceDefault = "default"
	createSourceRetry   = "retry"
)

// 初始化
func InitOnce(ctx context.Context, re redis.UniversalClient, keyPrefix string, call Callback, opts ...Option) (*Once, error) {
	op := newOptions(opts...)

	if re == nil {
		op.logger.Errorf(ctx, "redis client is nil")
		return nil, errors.New("redis client is nil")
	}

	if call == nil && !op.allowNilFunc {
		op.logger.Errorf(ctx, "worker callback is nil")
		return nil, errors.New("worker callback is nil")
	}

	// 清理 keyPrefix 中的 {} 字符，避免破坏 Redis Cluster hash tag
	keyPrefix = sanitizeKeyPrefix(keyPrefix)

	ctx, cancel := context.WithCancel(ctx)

	u, err := uuid.NewV7()
	if err != nil {
		op.logger.Errorf(ctx, "InitOnce uuid.NewV7 err:%v, using fallback", err)
		u = uuid.New()
	}

	wo := &Once{
		ctx:              ctx,
		cancel:           cancel,
		logger:           op.logger,
		zsetKey:          fmt.Sprintf("timer:{%s}:once_zset", keyPrefix),
		listKey:          fmt.Sprintf("timer:{%s}:once_list", keyPrefix),
		executeInfoKey:   fmt.Sprintf("timer:{%s}:once_execinfo", keyPrefix),
		globalLockPrefix: fmt.Sprintf("timer:{%s}:once_lock:", keyPrefix),
		usePriority:      false,
		redis:            re,
		worker:           call,
		keyPrefix:        keyPrefix,
		batchSize:        op.batchSize,
		stopChan:         make(chan struct{}),
		instanceId:       u.String(),
		keySeparator:     "[:]",
		timeout:          op.timeout,
		maxRunCount:      op.maxRunCount,
		workerChan:       make(chan struct{}, op.maxWorkers),
		maxWorkers:       op.maxWorkers,
	}

	wo.queue = newTaskQueue(
		re,
		wo.logger,
		wo.listKey,
		fmt.Sprintf("timer:{%s}:once_instances", keyPrefix),
		fmt.Sprintf("timer:{%s}:once_processing:", keyPrefix),
		wo.instanceId,
	)

	// 不需要回调
	if call == nil {
		return wo, nil
	}

	// 初始化优先级
	if op.priorityType != priorityTypeNone {

		wo.usePriority = true

		if op.priorityType == priorityTypeVersion {
			pVal, err := priority.PriorityByVersion(op.priorityVersion)
			if err != nil {
				wo.logger.Errorf(ctx, "PriorityByVersion version:%s err:%v", op.priorityVersion, err)
				return nil, err
			}
			op.priorityVal = pVal
		}

		pri, err := priority.InitPriority(
			ctx,
			re,
			keyPrefix,
			op.priorityVal,
			priority.WithLogger(wo.logger),
			priority.WithInstanceId(wo.instanceId),
			priority.WithSource("once"),
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
		leader.WithSource("once"),
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
	l.stopOnce.Do(l.close)
}

func (l *Once) close() {
	close(l.stopChan)
	if l.usePriority && l.priority != nil {
		l.priority.Close()
	}
	if l.leader != nil {
		l.leader.Close()
	}
	if l.heartbeat != nil {
		l.heartbeat.Close()
	}
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

	// 上报存活、回收失活实例的任务
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.queue.loop(l.ctx, l.stopChan, l.leader.IsLeader)
	}()

}

func (l *Once) cleanExecuteInfoLoop() {
	defer l.wg.Done()

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
func (l *Once) cleanExecuteInfo() {
	// 移除执行信息
	if err := l.redis.ZRemRangeByScore(l.ctx, l.executeInfoKey, "0", strconv.FormatInt(time.Now().Add(-15*time.Minute).UnixMilli(), 10)).Err(); err != nil {
		l.logger.Errorf(l.ctx, "cleanExecuteInfo ZRemRangeByScore err:%v", err)
	}
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
		select {
		case <-l.stopChan:
			return
		case <-l.ctx.Done():
			return
		default:
		}

		if l.usePriority && !l.priority.IsLatest(l.ctx) {
			sleepCtx(l.ctx, l.stopChan, time.Second*5)
			continue
		}

		if !l.queue.ready() {
			sleepCtx(l.ctx, l.stopChan, time.Millisecond*200)
			continue
		}

		// 限制并发数量：先占用worker再取任务，取出的任务一定有worker处理
		select {
		case <-l.stopChan:
			return
		case <-l.ctx.Done():
			return
		case l.workerChan <- struct{}{}:
		}
		if l.ctx.Err() != nil {
			<-l.workerChan
			return
		}

		key, err := l.queue.pop(l.ctx)
		if err != nil {
			<-l.workerChan
			if l.ctx.Err() != nil {
				return
			}
			if err == redis.Nil {
				l.queue.reconcile(l.ctx, true)
				continue
			}
			l.logger.Errorf(l.ctx, "Failed to pop task: %v", err)
			sleepCtx(l.ctx, l.stopChan, time.Second*5)
			continue
		}

		go l.processTask(key)
	}
}

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
func (l *Once) Save(ctx context.Context, taskType OnceTaskType, taskId string, delayTime time.Duration, attachData any) error {
	if delayTime < 0 {
		delayTime = 0
	}
	execTime := time.Now().Add(delayTime)
	return l.save(ctx, jobTypeOnce, taskType, taskId, []time.Time{execTime}, attachData, 0)
}

// 指定时间添加任务(覆盖)
func (l *Once) SaveByTime(ctx context.Context, taskType OnceTaskType, taskId string, executeTime time.Time, attachData any) error {
	return l.save(ctx, jobTypeOnce, taskType, taskId, []time.Time{executeTime}, attachData, 0)
}

func (l *Once) SaveByList(ctx context.Context, taskType OnceTaskType, taskId string, executeTimes []time.Time, attachData any, runCount int) error {
	return l.save(ctx, jobTypeList, taskType, taskId, executeTimes, attachData, runCount)
}

// 添加任务(覆盖)
// 重复插入就代表覆盖
func (w *Once) save(ctx context.Context, jobType jobType, taskType OnceTaskType, taskId string, taskTimes []time.Time, attachData any, runCount int) error {
	if len(taskTimes) == 0 {
		w.logger.Errorf(ctx, "delay time must be positive taskType:%v taskId:%v attachData:%v runCount:%v", taskType, taskId, attachData, runCount)
		return ErrExecuteTime
	}

	if jobType == jobTypeList && len(taskTimes) <= runCount {
		w.logger.Errorf(ctx, "delay time must be positive taskType:%v taskId:%v attachData:%v runCount:%v", taskType, taskId, attachData, runCount)
		return ErrRunCount
	}

	// 根据时间从小到大排序
	sort.Slice(taskTimes, func(i, j int) bool {
		return taskTimes[i].Before(taskTimes[j])
	})

	if taskTimes[0].IsZero() {
		w.logger.Errorf(ctx, "execute time is zero taskType:%v taskId:%v attachData:%v runCount:%v", taskType, taskId, attachData, runCount)
		return ErrExecuteTime
	}

	now := time.Now()

	// 单次任务的时间已到(含延迟为0)代表立即执行；时间列表全部过期才报错
	latestTime := taskTimes[len(taskTimes)-1]
	if jobType == jobTypeList && latestTime.Before(now) {
		w.logger.Errorf(ctx, "delay time must be positive taskType:%v taskId:%v attachData:%v runCount:%v", taskType, taskId, attachData, runCount)
		return ErrDelayTime
	}

	nextTime := taskTimes[0]
	if jobType == jobTypeList {
		nextTime = taskTimes[runCount]
	}

	redisKey := w.buildRedisKey(taskType, taskId)

	ed := extendData{
		TaskTimes: taskTimes,
		Data:      attachData,
		RunCount:  runCount,
		JobType:   jobType,
		Version:   uuid.NewString(),
	}
	b, err := json.Marshal(ed)
	if err != nil {
		w.logger.Errorf(ctx, "save json.Marshal err:%v taskType:%v taskId:%v", err, taskType, taskId)
		return err
	}

	// 使用事务确保原子性
	pipe := w.redis.TxPipeline()

	// 数据保留到最后一次执行时间之后
	expiresTime := latestTime
	if expiresTime.Before(now) {
		expiresTime = now
	}
	dataExpire := expiresTime.Add(time.Minute * 30).Sub(now)

	dataKey := fmt.Sprintf("timer:{%s}:once_data:%s", w.keyPrefix, redisKey)
	pipe.SetEx(ctx, dataKey, b, dataExpire)
	pipe.ZAdd(ctx, w.zsetKey, redis.Z{
		Score:  float64(nextTime.UnixMilli()),
		Member: redisKey,
	})
	_, err = pipe.Exec(ctx)
	if err != nil {
		w.logger.Errorf(ctx, "save task failed:%v taskType:%v taskId:%v attachData:%v retryCount:%v", err, taskType, taskId, attachData, runCount)
		return err
	}

	return nil
}

// 添加任务(不覆盖)
func (l *Once) Create(ctx context.Context, taskType OnceTaskType, taskId string, delayTime time.Duration, attachData any) error {
	if delayTime <= 0 {
		delayTime = 0
	}
	execTime := time.Now().Add(delayTime)
	return l.create(ctx, createSourceDefault, jobTypeOnce, taskType, taskId, []time.Time{execTime}, attachData, 0)
}

// 指定时间执行(不覆盖)
func (l *Once) CreateByTime(ctx context.Context, taskType OnceTaskType, taskId string, executeTime time.Time, attachData any) error {
	return l.create(ctx, createSourceDefault, jobTypeOnce, taskType, taskId, []time.Time{executeTime}, attachData, 0)
}

// 指定多个时间执行(不覆盖)
func (l *Once) CreateByList(ctx context.Context, taskType OnceTaskType, taskId string, taskTimes []time.Time, attachData any) error {
	return l.create(ctx, createSourceDefault, jobTypeList, taskType, taskId, taskTimes, attachData, 0)
}

func (l *Once) create(ctx context.Context, source createSource, jobType jobType, taskType OnceTaskType, taskId string, taskTimes []time.Time, attachData any, runCount int) error {
	if len(taskTimes) <= 0 {
		l.logger.Errorf(ctx, "delay time must be positive taskType:%v taskId:%v attachData:%v runCount:%v", taskType, taskId, attachData, runCount)
		return ErrExecuteTime
	}

	redisKey := l.buildRedisKey(taskType, taskId)

	if source != createSourceRetry {
		// 这里加一个全局锁 从Retry来的不需要 因为已经加锁了
		lock, err := lockx.NewGlobalLock(ctx, l.redis, l.globalLockPrefix+redisKey)
		if err != nil {
			l.logger.Errorf(ctx, "processTask timer:获取锁失败:%s", taskId)
			return err
		}
		// 锁被占用时 lockx 返回 redis.Nil：同ID任务正在创建或执行
		b, err := lock.Lock()
		if err != nil && !errors.Is(err, redis.Nil) {
			l.logger.Errorf(ctx, "create timer:获取锁失败:%s %+v", taskId, err)
			return err
		}
		if !b {
			l.logger.Errorf(ctx, "create task is creating or running taskType:%v taskId:%v", taskType, taskId)
			return ErrTaskExists
		}
		defer lock.Unlock()
	}

	_, err := l.redis.ZScore(ctx, l.zsetKey, redisKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return l.save(ctx, jobType, taskType, taskId, taskTimes, attachData, runCount)
		}
		l.logger.Errorf(ctx, "redis.ZScore err:%v", err)
		return err
	}
	l.logger.Errorf(l.ctx, "task exists taskType:%v taskId:%v attachData:%v runCount:%v", taskType, taskId, attachData, runCount)
	return ErrTaskExists
}

// 删除任务
func (w *Once) Delete(taskType OnceTaskType, taskId string) error {
	redisKey := w.buildRedisKey(taskType, taskId)
	dataKey := fmt.Sprintf("timer:{%s}:once_data:%s", w.keyPrefix, redisKey)

	pipe := w.redis.TxPipeline()
	pipe.Del(w.ctx, dataKey)
	pipe.ZRem(w.ctx, w.zsetKey, redisKey)

	_, err := pipe.Exec(w.ctx)
	if err != nil {
		w.logger.Errorf(w.ctx, "delete task failed:%v", err)
		return err
	}

	return nil
}

// 获取任务
func (l *Once) Get(taskType OnceTaskType, taskId string) (any, error) {
	redisKey := l.buildRedisKey(taskType, taskId)
	dataKey := fmt.Sprintf("timer:{%s}:once_data:%s", l.keyPrefix, redisKey)
	str, err := l.redis.Get(l.ctx, dataKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	var ed extendData
	if err := json.Unmarshal([]byte(str), &ed); err != nil {
		return nil, err
	}
	return ed.Data, nil
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

	defer func() {
		<-l.workerChan
	}()

	if l.runTask(key) {
		l.queue.ack(l.ctx, key)
		return
	}

	// 暂时无法处理：稍后放回就绪队列，由任意实例重新处理
	sleepCtx(l.ctx, l.stopChan, time.Second)
	l.queue.nack(l.ctx, key)
}

// 执行任务
// @return done bool 任务是否处理结束 false代表需要放回就绪队列
func (l *Once) runTask(key string) (done bool) {
	done = true

	begin := time.Now()

	ctx, cancel := context.WithTimeout(l.ctx, l.timeout)
	defer cancel()

	u, err := uuid.NewV7()
	if err != nil {
		l.logger.Errorf(ctx, "processTask uuid.NewV7 err:%v, using fallback", err)
		u = uuid.New()
	}

	ctx = context.WithValue(ctx, "trace_id", u.String())

	l.logger.Infof(ctx, "processTask start key:%s", key)

	taskType, taskId, err := l.parseRedisKey(key)
	if err != nil {
		l.logger.Errorf(ctx, "processTask parseRedisKey:%v key:%s", err, key)
		return true
	}

	// 这里加一个全局锁
	lock, err := lockx.NewGlobalLock(ctx, l.redis, l.globalLockPrefix+key)
	if err != nil {
		l.logger.Errorf(ctx, "processTask timer:获取锁失败:%s", taskId)
		return false
	}
	// 锁被占用(同ID任务正在执行或创建)或Redis异常，都稍后再处理
	if b, err := lock.Lock(); !b || err != nil {
		l.logger.Errorf(ctx, "processTask timer:获取锁失败:%s %+v", taskId, err)
		return false
	}
	defer lock.Unlock()

	// 上报执行情况
	executeVal := fmt.Sprintf("tid:%s|insId:%s|uuid:%s|time:%s", key, l.instanceId, u.String(), begin.Format(time.RFC3339Nano))
	if err := l.redis.ZAdd(ctx, l.executeInfoKey, redis.Z{
		Score:  float64(begin.UnixMilli()),
		Member: executeVal,
	}).Err(); err != nil {
		l.logger.Errorf(ctx, "processTask ZAdd executeInfo err:%v", err)
	}

	defer func() {
		if err := recover(); err != nil {
			l.logger.Errorf(ctx, "processTask panic:%s stack:%s", err, string(debug.Stack()))
		}
	}()

	// 读取数据
	dataKey := fmt.Sprintf("timer:{%s}:once_data:%s", l.keyPrefix, l.buildRedisKey(taskType, taskId))
	str, err := l.redis.Get(ctx, dataKey).Result()
	if err != nil {
		l.logger.Errorf(ctx, "processTask redis.Get key:%s err:%s", key, err)
		// 数据不存在代表任务已删除或已完成
		return errors.Is(err, redis.Nil)
	}

	ed := extendData{}
	if err := json.Unmarshal([]byte(str), &ed); err != nil {
		l.logger.Errorf(ctx, "processTask json.Unmarshal err:%v key:%s", err, key)
		return true
	}

	resp := l.worker.Worker(ctx, taskType, taskId, ed.Data)
	l.logger.Infof(ctx, "processTask exec key:%s resp:%+v data:%s", key, resp, str)

	// 回调结束后的收尾不受关闭影响
	finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(ctx), queueOpTimeout)
	defer finishCancel()

	if resp == nil || !resp.Retry {
		// 完成 删除任务
		l.logger.Infof(ctx, "processTask delete key:%s", key)
		if err := l.deleteIfUnchanged(finishCtx, key, str); err != nil {
			l.logger.Errorf(ctx, "processTask delete err:%v", err)
		}
		return true
	}

	// 重新放入队列
	if err := l.handleRetry(finishCtx, taskType, taskId, &ed, resp, str); err != nil {
		l.logger.Errorf(ctx, "processTask handleRetry err:%v", err)
	}
	return true
}

// 任务执行结束后删除任务
// 执行期间任务被重新保存(数据已变化)则保留新任务
func (l *Once) deleteIfUnchanged(ctx context.Context, redisKey string, rawData string) error {
	script := `
	if redis.call('GET', KEYS[1]) == ARGV[1] then
		redis.call('DEL', KEYS[1])
		redis.call('ZREM', KEYS[2], ARGV[2])
		return 1
	end
	return 0
	`
	dataKey := fmt.Sprintf("timer:{%s}:once_data:%s", l.keyPrefix, redisKey)
	return l.redis.Eval(ctx, script, []string{dataKey, l.zsetKey}, rawData, redisKey).Err()
}

func (l *Once) handleRetry(ctx context.Context, taskType OnceTaskType, taskId string,
	ed *extendData, resp *OnceWorkerResp, rawData string) error {
	// 限制重试次数
	ed.RunCount++
	if l.maxRunCount > 0 && ed.RunCount > l.maxRunCount {
		l.logger.Infof(ctx, "handleRetry task exceeded retry limit: %s %s %d", taskType, taskId, l.maxRunCount)
		return l.deleteIfUnchanged(ctx, l.buildRedisKey(taskType, taskId), rawData)
	}

	// 执行期间任务被重新保存：以新保存的为准，不再重试旧的
	dataKey := fmt.Sprintf("timer:{%s}:once_data:%s", l.keyPrefix, l.buildRedisKey(taskType, taskId))
	if cur, err := l.redis.Get(ctx, dataKey).Result(); err == nil && cur != rawData {
		l.logger.Infof(ctx, "handleRetry task was saved again during execution, skip retry: %s %s", taskType, taskId)
		return nil
	}

	// 更新延迟时间 延迟为0代表立即重试
	if ed.JobType == jobTypeOnce {
		delayTime := resp.DelayTime
		if delayTime < 0 {
			delayTime = 0
		}
		ed.TaskTimes = []time.Time{time.Now().Add(delayTime)}
	}

	if resp.AttachData != nil {
		ed.Data = resp.AttachData
	}

	l.logger.Infof(ctx, "handleRetry retrying task: %s:%s, retry count: %d",
		taskType, taskId, ed.RunCount)

	// 不覆盖的新建
	return l.create(ctx, createSourceRetry, ed.JobType, taskType, taskId, ed.TaskTimes, ed.Data, ed.RunCount)
}
