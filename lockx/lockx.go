package lockx

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

// 全局锁
type globalLock struct {
	redis     *redis.Client
	ctx       context.Context
	uniqueKey string
	value     string
}

func NewGlobalLock(ctx context.Context, red *redis.Client, uniqueKey string) *globalLock {
	return &globalLock{
		redis:     red,
		ctx:       ctx,
		uniqueKey: uniqueKey,
		value:     fmt.Sprintf("%d", time.Now().UnixNano()),
	}
}

// 获取锁
func (g *globalLock) Lock() bool {

	script := `
	return redis.call('set',KEYS[1],ARGV[1],'EX',ARGV[2])
	`

	resp, err := g.redis.Eval(g.ctx, script, []string{g.uniqueKey}, g.value, 10).Result()
	if resp != "OK" {
		log.Println("globalLock Lock", resp, err)
	}
	return resp == "OK"
}

// 尝试获取锁
func (g *globalLock) Try(limitTimes int) bool {
	for i := 0; i < limitTimes; i++ {
		if g.Lock() {
			return true
		}
		time.Sleep(time.Millisecond * 100)
	}
	return false
}

// 删除锁
func (g *globalLock) Unlock() bool {

	script := `
	local token = redis.call('get',KEYS[1])
	if token == ARGV[1]
	then
		 redis.call('del',KEYS[1])
		 return 'OK'
	end
	return 'ERROR'
	`

	resp, err := g.redis.Eval(g.ctx, script, []string{g.uniqueKey}, g.value).Result()
	if resp != "OK" {
		log.Println("globalLock Unlock", resp, err)
	}
	return resp == "OK"
}

// 刷新锁
func (g *globalLock) Refresh() {
	go func() {
		ctx, cancel := context.WithTimeout(g.ctx, time.Second*30)
		defer cancel()

		t := time.NewTicker(time.Second)
		for {
			select {
			case <-t.C:
				g.refresh()
			case <-ctx.Done():
				t.Stop()
				return
			}
		}
	}()
}

func (g *globalLock) refresh() bool {
	script := `
	local token = redis.call('get',KEYS[1])
	if token == ARGV[1]
	then
		redis.call('set',KEYS[1],ARGV[1],'EX',ARGV[2])
		return 'OK'
	end
	return 'ERROR'
	`

	resp, err := g.redis.Eval(g.ctx, script, []string{g.uniqueKey}, g.value, 5).Result()
	if resp != "OK" {
		log.Println("globalLock refresh", resp, err)
	}
	return resp == "OK"
}
