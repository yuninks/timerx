package timerx

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yuninks/timerx/logger"
)

// taskQueue 就绪队列的可靠消费
//  1. 取任务时原子地把任务从就绪 list 移到当前实例的处理中 list（BLMOVE，要求 Redis >= 6.2）
//  2. 处理结束后 ack 移除；暂时无法处理时 nack 放回就绪 list
//  3. 实例定期上报存活，leader 把失活实例处理中的任务放回就绪 list（至少执行一次）
type taskQueue struct {
	redis  redis.UniversalClient
	logger logger.Logger

	listKey          string // 就绪队列
	instancesKey     string // 存活实例 有序集合(score 为 Redis 时间)
	processingPrefix string // 处理中队列前缀
	processingKey    string // 当前实例的处理中队列
	instanceId       string

	heartbeatInterval time.Duration // 存活上报间隔
	instanceTimeout   time.Duration // 超过该时间未上报视为失活

	registered atomic.Bool  // 是否已成功上报过存活
	inflight   atomic.Int64 // 当前实例执行中的任务数
	idleRounds int          // 连续空闲次数(仅取任务的协程访问)
}

const (
	queueOpTimeout    = 5 * time.Second
	queueRecoverBatch = 1000
	// go-redis 默认不会因 ctx 取消而中断阻塞中的命令，阻塞时间决定了关闭需要等待的时间
	queuePopTimeout = time.Second
	// 连续空闲多少次整理一次
	queueReconcileRounds = 10
)

func newTaskQueue(re redis.UniversalClient, log logger.Logger, listKey, instancesKey, processingPrefix, instanceId string) *taskQueue {
	return &taskQueue{
		redis:             re,
		logger:            log,
		listKey:           listKey,
		instancesKey:      instancesKey,
		processingPrefix:  processingPrefix,
		processingKey:     processingPrefix + instanceId,
		instanceId:        instanceId,
		heartbeatInterval: 5 * time.Second,
		instanceTimeout:   30 * time.Second,
	}
}

// 守护：上报存活，leader 负责回收失活实例的任务
func (q *taskQueue) loop(ctx context.Context, stopChan <-chan struct{}, isLeader func() bool) {
	q.heartbeat(ctx)

	ticker := time.NewTicker(q.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			q.heartbeat(ctx)
			if isLeader() {
				q.recoverDead(ctx)
			}
		}
	}
}

// 是否可以开始取任务（未上报存活前取任务，实例崩溃后任务无法被回收）
func (q *taskQueue) ready() bool {
	return q.registered.Load()
}

func (q *taskQueue) heartbeat(ctx context.Context) {
	script := `
	local t = redis.call('TIME')
	local now = t[1] * 1000 + math.floor(t[2] / 1000)
	redis.call('ZADD', KEYS[1], now, ARGV[1])
	return now
	`
	if err := q.redis.Eval(ctx, script, []string{q.instancesKey}, q.instanceId).Err(); err != nil {
		if ctx.Err() == nil {
			q.logger.Errorf(ctx, "taskQueue heartbeat err:%v", err)
		}
		return
	}
	q.registered.Store(true)
}

// 取出一个任务并记录到当前实例的处理中队列，没有任务时返回 redis.Nil
func (q *taskQueue) pop(ctx context.Context) (string, error) {
	member, err := q.redis.BLMove(ctx, q.listKey, q.processingKey, "RIGHT", "LEFT", queuePopTimeout).Result()
	if err != nil {
		return "", err
	}
	q.inflight.Add(1)
	return member, nil
}

// 空闲时整理：没有执行中的任务时，处理中队列里剩下的是 ack/nack 失败的遗留
// 只能在取任务的协程里调用
// @param requeue bool 遗留任务是否放回就绪队列
func (q *taskQueue) reconcile(ctx context.Context, requeue bool) {
	if q.inflight.Load() != 0 {
		q.idleRounds = 0
		return
	}
	q.idleRounds++
	if q.idleRounds < queueReconcileRounds {
		return
	}
	q.idleRounds = 0

	script := `
	local moved = 0
	while moved < tonumber(ARGV[2]) do
		local task = redis.call('RPOP', KEYS[1])
		if not task then
			break
		end
		if ARGV[1] == '1' then
			redis.call('RPUSH', KEYS[2], task)
		end
		moved = moved + 1
	end
	return moved
	`
	flag := "0"
	if requeue {
		flag = "1"
	}
	moved, err := q.redis.Eval(ctx, script, []string{q.processingKey, q.listKey}, flag, queueRecoverBatch).Int64()
	if err != nil {
		if ctx.Err() == nil {
			q.logger.Errorf(ctx, "taskQueue reconcile err:%v", err)
		}
		return
	}
	if moved > 0 {
		q.logger.Infof(ctx, "taskQueue reconcile %d leftover tasks requeue:%v", moved, requeue)
	}
}

// 任务处理结束
func (q *taskQueue) ack(ctx context.Context, member string) {
	defer q.inflight.Add(-1)

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), queueOpTimeout)
	defer cancel()

	if err := q.redis.LRem(ctx, q.processingKey, 1, member).Err(); err != nil {
		q.logger.Errorf(ctx, "taskQueue ack err:%v member:%s", err, member)
	}
}

// 任务暂时无法处理，放回就绪队列
func (q *taskQueue) nack(ctx context.Context, member string) {
	defer q.inflight.Add(-1)

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), queueOpTimeout)
	defer cancel()

	// 已被回收的不重复放回
	script := `
	if redis.call('LREM', KEYS[1], 1, ARGV[1]) > 0 then
		redis.call('LPUSH', KEYS[2], ARGV[1])
		return 1
	end
	return 0
	`
	if err := q.redis.Eval(ctx, script, []string{q.processingKey, q.listKey}, member).Err(); err != nil {
		q.logger.Errorf(ctx, "taskQueue nack err:%v member:%s", err, member)
	}
}

// 把失活实例处理中的任务放回就绪队列
func (q *taskQueue) recoverDead(ctx context.Context) {
	listScript := `
	local t = redis.call('TIME')
	local now = t[1] * 1000 + math.floor(t[2] / 1000)
	return redis.call('ZRANGEBYSCORE', KEYS[1], 0, now - tonumber(ARGV[1]), 'LIMIT', 0, 100)
	`
	dead, err := q.redis.Eval(ctx, listScript, []string{q.instancesKey}, q.instanceTimeout.Milliseconds()).StringSlice()
	if err != nil {
		if err != redis.Nil && ctx.Err() == nil {
			q.logger.Errorf(ctx, "taskQueue recoverDead list err:%v", err)
		}
		return
	}

	// 回收前再次确认失活，任务放到队尾优先消费，回收完才移除实例
	moveScript := `
	local t = redis.call('TIME')
	local now = t[1] * 1000 + math.floor(t[2] / 1000)
	local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
	if not score or tonumber(score) > now - tonumber(ARGV[2]) then
		return -1
	end
	local moved = 0
	while moved < tonumber(ARGV[3]) do
		local task = redis.call('RPOP', KEYS[2])
		if not task then
			break
		end
		redis.call('RPUSH', KEYS[3], task)
		moved = moved + 1
	end
	if redis.call('LLEN', KEYS[2]) == 0 then
		redis.call('ZREM', KEYS[1], ARGV[1])
	end
	return moved
	`
	for _, instanceId := range dead {
		moved, err := q.redis.Eval(
			ctx,
			moveScript,
			[]string{q.instancesKey, q.processingPrefix + instanceId, q.listKey},
			instanceId,
			q.instanceTimeout.Milliseconds(),
			queueRecoverBatch,
		).Int64()
		if err != nil {
			if ctx.Err() == nil {
				q.logger.Errorf(ctx, "taskQueue recoverDead move err:%v instanceId:%s", err, instanceId)
			}
			continue
		}
		if moved > 0 {
			q.logger.Infof(ctx, "taskQueue recovered %d tasks from dead instance:%s", moved, instanceId)
		}
	}
}

// 可被中断的等待，被中断返回 false
func sleepCtx(ctx context.Context, stopChan <-chan struct{}, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	case <-stopChan:
		return false
	}
}
