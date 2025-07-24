package priority

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/yuninks/timerx/logger"
)

// 多版本场景判断当前是否最新版本

type Priority struct {
	ctx        context.Context
	priority   int // 优先级
	redis      redis.UniversalClient
	redisKey   string
	logger     logger.Logger
	expireTime time.Duration
}

func InitPriority(ctx context.Context, re redis.UniversalClient, keyPrefix string, priority int, opts ...Option) *Priority {
	conf := newOptions(opts...)

	pro := &Priority{
		ctx:        ctx,
		priority:   priority,
		redis:      re,
		logger:     conf.logger,
		redisKey:   "timer:priority_" + keyPrefix,
		expireTime: conf.expireTime,
	}

	// 更新间隔
	updateTnterval := time.NewTicker(conf.updateInterval)
	go func(ctx context.Context) {
		pro.setPriority()
	Loop:
		for {
			select {
			case <-updateTnterval.C:
				pro.setPriority()
			case <-ctx.Done():
				break Loop
			}
		}
	}(ctx)

	return pro
}

func (l *Priority) IsLatest(ctx context.Context) bool {
	// 加缓存
	str, err := l.redis.Get(l.ctx, l.redisKey).Result()

	if err != nil {
		if err == redis.Nil && l.priority == 0 {
			return true
		}
		l.logger.Errorf(l.ctx, "获取全局优先级失败:%s", err.Error())
		return false
	}

	strPriority, err := strconv.Atoi(str)
	if err != nil {
		l.logger.Errorf(l.ctx, "全局优先级转换失败:%s", err.Error())
		return false
	}
	if l.priority >= strPriority {
		return true
	}
	return false
}

func (l *Priority) setPriority() bool {
	// redis lua脚本
	// 如果redis的可以不存在则设置值，如果存在且redis内的值比当前大则不处理，如果存在redis内的值比当前小或等于则更新值且更新ttl
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
		return { "SET", expireTime }
	elseif currentPriorityNum < newPriorityNum then
		-- 如果当前优先级小于新优先级，则更新优先级并更新TTL
		redis.call('set', priorityKey, priority, 'ex', expireTime)
		return { "RESET", expireTime }
	elseif currentPriorityNum == newPriorityNum then
		-- 优先级相同则更新TTL
		redis.call('expire', priorityKey, expireTime)
		return { "UPDATE", expireTime }
	else
		-- 如果当前优先级大于新优先级，则不更新
		return { "NOAUCH", '0' }
	end
	`
	priority := fmt.Sprintf("%d", l.priority)

	res, err := l.redis.Eval(l.ctx, script, []string{l.redisKey}, priority, l.expireTime.Seconds()).Result()
	if err != nil {
		l.logger.Errorf(l.ctx, "设置全局优先级失败:%s", err.Error())
		return false
	}

	l.logger.Infof(l.ctx, "设置全局优先级返回值:%+v", res)

	// 处理返回值，包含操作结果和 TTL
	resultArray := res.([]interface{})
	if len(resultArray) < 2 {
		l.logger.Errorf(l.ctx, "设置全局优先级失败: 返回值格式不正确")
		return false
	}
	operationResult := resultArray[0].(string)
	ttl := resultArray[1].(string)

	if operationResult == "SET" || operationResult == "UPDATE" {
		l.logger.Infof(l.ctx, "设置全局优先级成功:%s", priority)
		return true
	}
	_ = ttl
	l.logger.Infof(l.ctx, "设置全局优先级未更新:%s", priority)
	return false
}
