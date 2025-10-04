package leader

// 竞选Leader

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yuninks/lockx"
	"github.com/yuninks/timerx/logger"
	"github.com/yuninks/timerx/priority"
)

// 领导
// 作用：领导选举，是领导才执行，避免资源浪费
// 依赖：priority

type Leader struct {
	ctx    context.Context
	cancel context.CancelFunc

	isLeader         bool         // 是否是领导
	leaderLock       sync.RWMutex // 领导锁
	leaderUniLockKey string       // 领导唯一锁
	leaderKey        string       // 上报当前的Leader

	redis  redis.UniversalClient // redis
	logger logger.Logger

	priority *priority.Priority // 全局优先级
	wg       sync.WaitGroup

	instanceId string // 实例ID
}

// leader负责选举

func InitLeader(ctx context.Context, ref redis.UniversalClient, keyPrefix string, opts ...Option) (*Leader, error) {

	if ref == nil {
		return nil, errors.New("redis is nil")
	}

	op := newOptions(opts...)

	ctx, cancel := context.WithCancel(ctx)

	l := &Leader{
		ctx:              ctx,
		cancel:           cancel,
		redis:            ref,
		leaderUniLockKey: "timer:leader_lockKey" + op.source + keyPrefix,
		priority:         op.priority,
		instanceId:       op.instanceId,
		logger:           op.logger,
	}

	l.wg.Add(1)
	go l.leaderElection()

	l.logger.Infof(l.ctx, "InitLeader InstanceId %s lockKey:%s", l.instanceId, l.leaderUniLockKey)

	return l, nil
}

func (l *Leader) Close() {
	l.cancel()
	l.wg.Wait()
}

// 领导选举
// 领导作用：全局推选一个人计算执行时间&移入队列，避免每个都进行计算浪费资源
func (l *Leader) leaderElection() {
	defer l.wg.Done()

	// 先执行一次
	l.getLeaderLock()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			l.getLeaderLock()
		case <-l.ctx.Done():
			return
		}
	}
}

// 成为领导
func (l *Leader) getLeaderLock() error {

	// 非当前优先级不用抢leader
	if l.priority != nil && !l.priority.IsLatest(l.ctx) {
		return nil
	}

	ctx, cancel := context.WithCancel(l.ctx)
	defer cancel()

	// 尝试加锁
	lock, err := lockx.NewGlobalLock(ctx, l.redis, l.leaderUniLockKey)
	if err != nil {
		l.logger.Errorf(l.ctx, "getLeaderLock err:%+v", err)
		return err
	}
	if b, _ := lock.Lock(); !b {
		// 加锁失败 非Reader
		l.leaderLock.Lock()
		l.isLeader = false
		l.leaderLock.Unlock()
		return nil
	}
	defer lock.Unlock()

	// 加锁成功
	l.leaderLock.Lock()
	l.isLeader = true
	l.leaderLock.Unlock()

	// 上报当前的Leader实例
	l.redis.Set(l.ctx, l.leaderKey, l.instanceId, time.Hour*24)

	l.logger.Infof(l.ctx, "getLeaderLock Instance %s became leader", lock.GetValue())

	go func() {
		if l.priority == nil {
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
				if !l.priority.IsLatest(l.ctx) {
					cancel()
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// 等待超时退出
	<-lock.GetCtx().Done()

	// 已过期
	// l.leaderLock.Lock()
	// l.isLeader = false
	// l.leaderLock.Unlock()

	return nil

}

// isCurrentLeader 检查当前实例是否是leader
func (c *Leader) IsLeader() bool {
	c.leaderLock.RLock()
	defer c.leaderLock.RUnlock()
	return c.isLeader
}
