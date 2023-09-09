package timer

// 作者：黄新云

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

// 定时器
// 原理：每毫秒的时间触发

// uuid -> timerStr
var timerMap = make(map[string]*timerStr)
var timerMapMux sync.Mutex

var timerCount int        // 当前定时数目
var onceLimit sync.Once   // 实现单例
var nextTime = time.Now() // 下一次执行的时间

type ContextValueKey string // 定义context 传递的Key类型

const (
	extendParamKey ContextValueKey = "extend_param"
)

// 定时器类
func InitSingle(ctx context.Context) {
	onceLimit.Do(func() {
		timer := time.NewTicker( time.Millisecond*200)
		go func(ctx context.Context) {
		Loop:
			for {
				select {
				case t := <-timer.C:
					if t.Before(nextTime) {
						// 当前时间小于下次发送时间：跳过
						continue
					}
					// 迭代定时器
					iteratorTimer(ctx, t)
					// fmt.Println("timer: 执行")
				case <-ctx.Done():
					// 跳出循环
					break Loop
				}
			}
			log.Println("timer: initend")
		}(ctx)
	})
}

// 间隔定时器
func AddTimer(space time.Duration, call callback, extend ExtendParams) (int, error) {
	timerMapMux.Lock()
	defer timerMapMux.Unlock()

	if space != space.Abs() {
		return 0, errors.New("space must be positive")
	}

	timerCount += 1

	nowTime := time.Now()

	t := timerStr{
		Callback:   call,
		BeginTime:  nowTime,
		NextTime:   nowTime, // nowTime.Add(space), // 添加任务的时候就执行一次
		SpaceTime:  space,
		CanRunning: make(chan struct{}, 1),
		UniqueKey:  "",
		Extend:     extend,
	}

	timerMap[fmt.Sprintf("%d", timerCount)] = &t

	if t.NextTime.Before(nextTime) {
		// 本条规则下次需要发送的时间小于系统下次发送时间：替换
		nextTime = t.NextTime
	}

	return timerCount, nil
}

// 添加需要定时的规则
func AddToTimer(space time.Duration, call callback) int {
	extend := ExtendParams{}
	count, _ := AddTimer(space, call, extend)
	return count
}

func DelToTimer(index string) {
	timerMapMux.Lock()
	defer timerMapMux.Unlock()
	delete(timerMap, index)
}

// 迭代定时器列表
func iteratorTimer(ctx context.Context, nowTime time.Time) {
	timerMapMux.Lock()
	defer timerMapMux.Unlock()

	// fmt.Println("nowTime:", nowTime.Format("2006-01-02 15:04:05.000"))

	// 默认5秒后（如果没有值就暂停进来5秒）
	newNextTime := nowTime.Add(time.Second * 5)

	index := 0
	for _, v := range timerMap {
		index++
		v := v
		// 判断执行的时机
		if v.NextTime.Before(nowTime) {
			// fmt.Println("NextTime", v.NextTime.Format("2006-01-02 15:04:05.000"))

			v.NextTime = v.NextTime.Add(v.SpaceTime)

			// 判断下次执行时间与当前时间
			if v.NextTime.Before(nowTime) {
				v.NextTime = nowTime.Add(v.SpaceTime)
			}

			if index == 1 {
				// 循环的第一个需要替换默认值
				newNextTime = v.NextTime
			}

			// 获取最小的
			if v.NextTime.Before(newNextTime) {
				// 本规则下次发送时间小于系统下次需要执行的时间：替换
				newNextTime = v.NextTime
			}

			// 处理中就跳过本次
			go func(ctx context.Context, v *timerStr) {
				select {
				case v.CanRunning <- struct{}{}:
					defer func() {
						// fmt.Printf("timer: 执行完成 %v %v \n", k, v.Tag)
						select {
						case <-v.CanRunning:
							return
						default:
							return
						}
					}()
					// fmt.Printf("timer: 准备执行 %v %v \n", k, v.Tag)
					timerAction(ctx, v.Callback, v.UniqueKey, v.Extend)
				default:
					// fmt.Printf("timer: 已在执行 %v %v \n", k, v.Tag)
					return
				}
			}(ctx, v)
		}
	}

	// 实际下次时间小于预期下次时间：替换
	if nextTime.Before(newNextTime) {
		// 判断一下避免异常
		if newNextTime.Before(nowTime) {
			// 比当前时间小
			nextTime = nowTime
		} else {
			nextTime = newNextTime
		}
	}

	// fmt.Println("timer: one finish")
}

// 定义各个回调函数
type callback func(ctx context.Context) bool

// 定时器操作类
// 这里不应painc
func timerAction(ctx context.Context, call callback, uniqueKey string, extend ExtendParams) bool {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("timer:定时器出错", err)
			log.Println("errStack", string(debug.Stack()))
		}
	}()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 附加数据
	ctx = context.WithValue(ctx, extendParamKey, extend)

	return call(ctx)
}

// 快捷方法
func GetExtendParams(ctx context.Context) (*ExtendParams, error) {
	val := ctx.Value(extendParamKey)
	params, ok := val.(ExtendParams)
	if !ok {
		return nil, errors.New("没找到参数")
	}
	return &params, nil
}
