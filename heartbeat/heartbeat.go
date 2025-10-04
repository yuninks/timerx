package heartbeat

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/yuninks/timerx/leader"
	"github.com/yuninks/timerx/logger"
	"github.com/yuninks/timerx/priority"
)

// 心跳
// 作用：上报实例存活状态
// 依赖：leader priority

type HeartBeat struct {
	ctx    context.Context
	cancel context.CancelFunc

	redis  redis.UniversalClient // redis
	logger logger.Logger

	priority *priority.Priority // (允许nil)全局优先级
	leader   *leader.Leader     // (允许nil)领导

	heartbeatKey string // 心跳Key 有序集合
	instanceId   string // 实例ID

	wg sync.WaitGroup
}

func InitHeartBeat(ctx context.Context, ref redis.UniversalClient, keyPrefix string, opts ...Option) (*HeartBeat, error) {

	if ref == nil {
		return nil, errors.New("redis is nil")
	}

	op := newOptions(opts...)

	ctx, cancel := context.WithCancel(ctx)

	l := &HeartBeat{
		ctx:    ctx,
		cancel: cancel,

		heartbeatKey: "timer:heartbeat_key" + op.source + keyPrefix,

		priority:   op.priority,
		redis:      ref,
		logger:     op.logger,
		leader:     op.leader,
		instanceId: op.instanceId,
	}

	l.logger.Infof(l.ctx, "InitHeartBeat InstanceId %s lockKey:%s", l.instanceId, l.heartbeatKey)

	l.startDaemon()

	return l, nil
}

func (l *HeartBeat) Close() {
	l.cancel()
	l.cleanHeartbeat(true)
	l.wg.Wait()
}

func (l *HeartBeat) startDaemon() {

	l.wg.Add(1)
	go l.heartbeatLoop()

	l.wg.Add(1)
	go l.cleanHeartbeatLoop()

}

// 心跳上报
// 需要确定当前存活的实例&当前实例是否是领导
func (l *HeartBeat) heartbeatLoop() {
	defer l.wg.Done()

	// 先执行一次
	l.heartbeat()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			l.heartbeat()
		}
	}

}

// 单次心跳
func (l *HeartBeat) heartbeat() error {
	err := l.redis.ZAdd(l.ctx, l.heartbeatKey, &redis.Z{
		Score:  float64(time.Now().UnixMilli()),
		Member: l.instanceId,
	}).Err()

	if err != nil {
		l.logger.Errorf(l.ctx, "heartbeat redis.ZAdd err:%v", err)
		return err
	}

	return nil
}

// 心跳清理（leader可操作）
func (l *HeartBeat) cleanHeartbeatLoop() {
	defer l.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			if l.leader != nil {
				if !l.leader.IsLeader() {
					// l.logger.Infof(l.ctx, "cleanHeartbeatLoop not leader")
					continue
				}
			}
			l.cleanHeartbeat(false)
		}
	}

}

// 单次清理
func (l *HeartBeat) cleanHeartbeat(cleanSelf bool) error {

	// 仅移除自己
	if cleanSelf {
		return l.redis.ZRem(l.ctx, l.heartbeatKey, l.instanceId).Err()
	}

	// 移除心跳
	l.redis.ZRemRangeByScore(l.ctx, l.heartbeatKey, "0", strconv.FormatInt(time.Now().Add(-15*time.Second).UnixMilli(), 10)).Err()

	return nil
}
