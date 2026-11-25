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

// 保存方式
type saveMode string

const (
	saveModeOverwrite saveMode = "overwrite" // 覆盖
	saveModeCreate    saveMode = "create"    // 任务不存在才保存
	saveModeUnchanged saveMode = "unchanged" // 任务数据未变化才保存
)

// 任务数据已变化（执行期间被重新保存或删除）
var errTaskChanged = errors.New("task changed")

// 待保存的任务
type saveArgs struct {
	redisKey string
	data     []byte
	nextTime time.Time
	expire   time.Duration
}

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
				l.queue.reconcile(l.ctx)
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
	args, err := w.prepareSave(ctx, jobType, taskType, taskId, taskTimes, attachData, runCount)
	if err != nil {
		return err
	}
	return w.execSave(ctx, saveModeOverwrite, "", args)
}

// 校验并生成待保存的数据（不访问Redis）
func (w *Once) prepareSave(ctx context.Context, jobType jobType, taskType OnceTaskType, taskId string, taskTimes []time.Time, attachData any, runCount int) (*saveArgs, error) {
	if len(taskTimes) == 0 {
		w.logger.Errorf(ctx, "delay time must be positive taskType:%v taskId:%v attachData:%v runCount:%v", taskType, taskId, attachData, runCount)
		return nil, ErrExecuteTime
	}

	if jobType == jobTypeList && len(taskTimes) <= runCount {
		w.logger.Errorf(ctx, "delay time must be positive taskType:%v taskId:%v attachData:%v runCount:%v", taskType, taskId, attachData, runCount)
		return nil, ErrRunCount
	}

	// 根据时间从小到大排序
	sort.Slice(taskTimes, func(i, j int) bool {
		return taskTimes[i].Before(taskTimes[j])
	})

	if taskTimes[0].IsZero() {
		w.logger.Errorf(ctx, "execute time is zero taskType:%v taskId:%v attachData:%v runCount:%v", taskType, taskId, attachData, runCount)
		return nil, ErrExecuteTime
	}

	now := time.Now()

	// 单次任务的时间已到(含延迟为0)代表立即执行；时间列表全部过期才报错
	latestTime := taskTimes[len(taskTimes)-1]
	if jobType == jobTypeList && latestTime.Before(now) {
		w.logger.Errorf(ctx, "delay time must be positive taskType:%v taskId:%v attachData:%v runCount:%v", taskType, taskId, attachData, runCount)
		return nil, ErrDelayTime
	}

	nextTime := taskTimes[0]
	if jobType == jobTypeList {
		nextTime = taskTimes[runCount]
	}

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
		return nil, err
	}

	// 数据保留到最后一次执行时间之后
	expiresTime := latestTime
	if expiresTime.Before(now) {
		expiresTime = now
	}

	return &saveArgs{
		redisKey: w.buildRedisKey(taskType, taskId),
		data:     b,
		nextTime: nextTime,
		expire:   expiresTime.Add(time.Minute * 30).Sub(now),
	}, nil
}

// 原子地写入任务数据和执行时间
// @param expectData string saveModeUnchanged 时任务当前应有的数据
func (w *Once) execSave(ctx context.Context, mode saveMode, expectData string, args *saveArgs) error {
	script := `
	if ARGV[1] == 'create' then
		if redis.call('EXISTS', KEYS[1]) == 1 or redis.call('ZSCORE', KEYS[2], ARGV[5]) then
			return 0
		end
	elseif ARGV[1] == 'unchanged' then
		if redis.call('GET', KEYS[1]) ~= ARGV[6] then
			return 0
		end
	end
	redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
	redis.call('ZADD', KEYS[2], ARGV[4], ARGV[5])
	return 1
	`
	dataKey := fmt.Sprintf("timer:{%s}:once_data:%s", w.keyPrefix, args.redisKey)
	saved, err := w.redis.Eval(
		ctx,
		script,
		[]string{dataKey, w.zsetKey},
		string(mode),
		args.data,
		args.expire.Milliseconds(),
		args.nextTime.UnixMilli(),
		args.redisKey,
		expectData,
	).Int64()
	if err != nil {
		w.logger.Errorf(ctx, "save task failed:%v key:%s mode:%s", err, args.redisKey, mode)
		return err
	}
	if saved == 0 {
		if mode == saveModeCreate {
			return ErrTaskExists
		}
		return errTaskChanged
	}

	return nil
}

// 添加任务(不覆盖)
func (l *Once) Create(ctx context.Context, taskType OnceTaskType, taskId string, delayTime time.Duration, attachData any) error {
	if delayTime <= 0 {
		delayTime = 0
	}
	execTime := time.Now().Add(delayTime)
	return l.create(ctx, jobTypeOnce, taskType, taskId, []time.Time{execTime}, attachData, 0)
}

// 指定时间执行(不覆盖)
func (l *Once) CreateByTime(ctx context.Context, taskType OnceTaskType, taskId string, executeTime time.Time, attachData any) error {
	return l.create(ctx, jobTypeOnce, taskType, taskId, []time.Time{executeTime}, attachData, 0)
}

// 指定多个时间执行(不覆盖)
func (l *Once) CreateByList(ctx context.Context, taskType OnceTaskType, taskId string, taskTimes []time.Time, attachData any) error {
	return l.create(ctx, jobTypeList, taskType, taskId, taskTimes, attachData, 0)
}

func (l *Once) create(ctx context.Context, jobType jobType, taskType OnceTaskType, taskId string, taskTimes []time.Time, attachData any, runCount int) error {
	args, err := l.prepareSave(ctx, jobType, taskType, taskId, taskTimes, attachData, runCount)
	if err != nil {
		return err
	}

	// 这里加一个全局锁
	lock, err := lockx.NewGlobalLock(ctx, l.redis, l.globalLockPrefix+args.redisKey)
	if err != nil {
		l.logger.Errorf(ctx, "create timer:获取锁失败:%s", taskId)
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

	// 任务数据存在即任务存在（包括已到期、正在等待执行的任务）
	err = l.execSave(ctx, saveModeCreate, "", args)
	if errors.Is(err, ErrTaskExists) {
		l.logger.Errorf(ctx, "task exists taskType:%v taskId:%v attachData:%v runCount:%v", taskType, taskId, attachData, runCount)
	}
	return err
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
	// 任务还在等待执行时间(取出后被重新保存)时不执行，到期后会再次进入就绪队列
	// 数据的有效期延长到覆盖本次执行，避免执行期间过期导致重试被丢弃
	loadScript := `
	if redis.call('ZSCORE', KEYS[2], ARGV[1]) then
		return false
	end
	local data = redis.call('GET', KEYS[1])
	if data then
		local ttl = redis.call('PTTL', KEYS[1])
		if ttl >= 0 and ttl < tonumber(ARGV[2]) then
			redis.call('PEXPIRE', KEYS[1], ARGV[2])
		end
	end
	return data
	`
	dataKey := fmt.Sprintf("timer:{%s}:once_data:%s", l.keyPrefix, key)
	keepAlive := l.timeout + time.Minute
	str, err := l.redis.Eval(ctx, loadScript, []string{dataKey, l.zsetKey}, key, keepAlive.Milliseconds()).Text()
	if err != nil {
		l.logger.Errorf(ctx, "processTask load data key:%s err:%s", key, err)
		// 数据不存在代表任务已删除或已完成
		return errors.Is(err, redis.Nil)
	}

	ed := extendData{}
	unmarshalErr := json.Unmarshal([]byte(str), &ed)

	var resp *OnceWorkerResp
	if unmarshalErr == nil {
		resp = l.callWorker(ctx, taskType, taskId, ed.Data)
		l.logger.Infof(ctx, "processTask exec key:%s resp:%+v data:%s", key, resp, str)
	}

	// 回调结束后的收尾不受关闭和回调耗时影响
	finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(ctx), queueOpTimeout)
	defer finishCancel()

	if unmarshalErr != nil {
		l.logger.Errorf(ctx, "processTask json.Unmarshal err:%v key:%s", unmarshalErr, key)
		if err := l.deleteIfUnchanged(finishCtx, key, str); err != nil {
			l.logger.Errorf(ctx, "processTask delete err:%v", err)
		}
		return true
	}

	if resp == nil || !resp.Retry {
		// 完成 删除任务
		l.logger.Infof(ctx, "processTask delete key:%s", key)
		if err := l.deleteIfUnchanged(finishCtx, key, str); err != nil {
			l.logger.Errorf(ctx, "processTask delete err:%v", err)
		}
		return true
	}

	// 重新放入队列
	return l.handleRetry(finishCtx, taskType, taskId, &ed, resp, str)
}

// 执行回调，panic 视为任务结束且不重试
func (l *Once) callWorker(ctx context.Context, taskType OnceTaskType, taskId string, data any) (resp *OnceWorkerResp) {
	defer func() {
		if err := recover(); err != nil {
			l.logger.Errorf(ctx, "processTask panic:%s stack:%s", err, string(debug.Stack()))
			resp = nil
		}
	}()

	return l.worker.Worker(ctx, taskType, taskId, data)
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

// 重试任务
// @return done bool 任务是否处理结束 false代表重试未能保存，需要放回就绪队列重新执行
func (l *Once) handleRetry(ctx context.Context, taskType OnceTaskType, taskId string,
	ed *extendData, resp *OnceWorkerResp, rawData string) (done bool) {
	redisKey := l.buildRedisKey(taskType, taskId)

	// 任务结束：删除任务
	finish := func() bool {
		if err := l.deleteIfUnchanged(ctx, redisKey, rawData); err != nil {
			l.logger.Errorf(ctx, "handleRetry delete err:%v", err)
		}
		return true
	}

	// 限制重试次数
	ed.RunCount++
	if l.maxRunCount > 0 && ed.RunCount > l.maxRunCount {
		l.logger.Infof(ctx, "handleRetry task exceeded retry limit: %s %s %d", taskType, taskId, l.maxRunCount)
		return finish()
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

	args, err := l.prepareSave(ctx, ed.JobType, taskType, taskId, ed.TaskTimes, ed.Data, ed.RunCount)
	if err != nil {
		// 无法再重试（如时间列表已执行完）
		l.logger.Errorf(ctx, "handleRetry can not retry: %s %s err:%v", taskType, taskId, err)
		return finish()
	}

	// 执行期间任务被重新保存或删除：以最新的为准，不再重试旧的
	err = l.execSave(ctx, saveModeUnchanged, rawData, args)
	if errors.Is(err, errTaskChanged) {
		l.logger.Infof(ctx, "handleRetry task changed during execution, skip retry: %s %s", taskType, taskId)
		return true
	}
	if err != nil {
		l.logger.Errorf(ctx, "handleRetry save err:%v", err)
		return false
	}

	return true
}
