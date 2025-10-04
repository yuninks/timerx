package priority

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yuninks/timerx/logger"
)

// 多版本场景判断当前是否最新版本

type Priority struct {
	ctx        context.Context       // 上下文
	cancel     context.CancelFunc    // 取消函数
	priority   int64                 // 优先级
	redis      redis.UniversalClient // redis
	redisKey   string                // redis key
	logger     logger.Logger         // 日志
	expireTime time.Duration         // 过期时间

	setInterval time.Duration // 尝试set的间隔
	getInterval time.Duration // 尝试get的间隔

	wg sync.WaitGroup

	isLatest  bool         // 是否是最新版本
	latestMux sync.RWMutex // 最新版本锁

	instanceId string // 实例ID
}

func InitPriority(ctx context.Context, re redis.UniversalClient, keyPrefix string, priority int64, opts ...Option) (*Priority, error) {

	if re == nil {
		return nil, errors.New("redis is nil")
	}

	conf := newOptions(opts...)

	ctx, cancel := context.WithCancel(ctx)

	pro := &Priority{
		ctx:         ctx,
		cancel:      cancel,
		priority:    priority,
		redis:       re,
		logger:      conf.logger,
		redisKey:    "timer:priority_" + conf.source + keyPrefix,
		expireTime:  conf.expireTime,
		setInterval: conf.updateInterval,
		getInterval: conf.getInterval,
		instanceId:  conf.instanceId,
	}

	pro.startDaemon()

	return pro, nil
}

func (p *Priority) Close() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
}

// 守护进程
func (l *Priority) startDaemon() {
	// 启动更新缓存
	l.wg.Add(1)
	go l.runUpdateLoop()

	l.wg.Add(1)
	go l.getLatestLoop()

}

func (p *Priority) runUpdateLoop() {
	defer p.wg.Done()

	// 立即尝试设置一次优先级
	if _, err := p.setPriority(); err != nil {
		p.logger.Errorf(p.ctx, "Initial priority set failed: %v", err)
	}

	ticker := time.NewTicker(p.setInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if _, err := p.setPriority(); err != nil {
				p.logger.Errorf(p.ctx, "Priority update failed: %v", err)
			}
		case <-p.ctx.Done():
			return
		}
	}
}

func (l *Priority) getLatestLoop() {

	defer l.wg.Done()

	if err := l.getLatest(); err != nil {
		l.logger.Errorf(l.ctx, "Priority update failed: %v", err)
	}

	ticker := time.NewTicker(l.getInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := l.getLatest(); err != nil {
				l.logger.Errorf(l.ctx, "Priority update failed: %v", err)
			}
		case <-l.ctx.Done():
			return
		}
	}

}

func (p *Priority) IsLatest(ctx context.Context) bool {
	p.latestMux.RLock()
	defer p.latestMux.RUnlock()

	return p.isLatest
}

func (p *Priority) setPriority() (string, error) {
	script := `
	-- KEYS[1] 是全局优先级的key
	local priorityKey = KEYS[1]
	-- ARGV[1] 是新的优先级
	local priority = ARGV[1]
	-- ARGV[2] 是过期时间
	local expireTime = ARGV[2]

	-- 校验参数完整性
	if not priorityKey or not priority or not expireTime then
		return redis.error_reply("Missing required arguments")
	end

	-- 尝试将字符串转换为数字
	local currentPriority = redis.call('get', priorityKey)
	local currentPriorityNum = tonumber(currentPriority)
	local newPriorityNum = tonumber(priority)

	if not currentPriority then
		-- 如果当前优先级不存在，则设置新优先级并设置TTL
		redis.call('set', priorityKey, priority, 'ex', expireTime)
		return { "SET" }
	elseif currentPriorityNum < newPriorityNum then
		-- 如果当前优先级小于新优先级，则更新优先级并更新TTL
		redis.call('set', priorityKey, priority, 'ex', expireTime)
		return { "RESET" }
	elseif currentPriorityNum == newPriorityNum then
		-- 优先级相同则更新TTL
		redis.call('expire', priorityKey, expireTime)
		return { "UPDATE" }
	else
		-- 如果当前优先级大于新优先级，则不更新
		return { "NOAUCH" }
	end
	`

	newPriorityStr := strconv.FormatInt(p.priority, 10)

	result, err := p.redis.Eval(p.ctx, script, []string{p.redisKey}, newPriorityStr, p.expireTime.Seconds()).Result()
	// p.logger.Infof(p.ctx, "Priority update result:%+v err:%+v", result, err)
	if err != nil {
		p.logger.Errorf(p.ctx, "Priority update err:%s", err.Error())
		return "", err
	}

	// 解析结果
	if resultMap, ok := result.([]interface{}); ok && len(resultMap) == 1 {
		resultStr := resultMap[0].(string)
		return resultStr, nil
	}

	return "", fmt.Errorf("script error: %v", result)
}

func (l *Priority) getLatest() error {
	// 查询Redis获取当前最高优先级
	currentPriority, err := l.getCurrentPriority()

	l.logger.Infof(l.ctx, "Priority getLatest currentPriority:%d l.priority:%d err:%+v", currentPriority, l.priority, err)

	if err != nil {
		l.logger.Errorf(l.ctx, "Priority getLatest getCurrentPriority err:%s", err.Error())
		return err
	}
	if currentPriority > l.priority {
		// 当前不是最新的
		l.latestMux.Lock()
		l.isLatest = false
		l.latestMux.Unlock()

		return nil
	}

	l.latestMux.Lock()
	l.isLatest = true
	l.latestMux.Unlock()

	return nil
}

func (p *Priority) getCurrentPriority() (int64, error) {
	result, err := p.redis.Get(p.ctx, p.redisKey).Result()
	if err != nil {
		if err == redis.Nil {
			// Key不存在，返回0作为默认值
			return 0, nil
		}
		return 0, err
	}

	priority, err := strconv.ParseInt(result, 10, 64)
	if err != nil {
		return 0, err
	}

	return priority, nil
}

// ForceRefresh 强制刷新优先级，用于紧急情况
func (p *Priority) ForceRefresh() error {

	_, err := p.setPriority()
	if err != nil {
		p.logger.Errorf(p.ctx, "Priority ForceRefresh setPriority err:%s", err.Error())
		return err
	}
	err = p.getLatest()
	if err != nil {
		p.logger.Errorf(p.ctx, "Priority ForceRefresh getLatest err:%s", err.Error())
		return err
	}

	return nil
}

// GetCurrentMaxPriority 获取当前系统中的最大优先级
func (p *Priority) GetCurrentMaxPriority(ctx context.Context) (int64, error) {
	return p.getCurrentPriority()
}
