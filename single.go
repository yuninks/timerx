package timerx

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
// 1. 这个定时器的作用范围是本机

// uuid -> timerStr
var timerMap = make(map[string]*timerStr)
var timerMapMux sync.Mutex

var timerCount int      // 当前定时数目
var onceLimit sync.Once // 实现单例

type Single struct{}

var sin *Single = nil

// 定时器类
func InitSingle(ctx context.Context) *Single {
	onceLimit.Do(func() {
		sin = &Single{}

		timer := time.NewTicker(time.Millisecond * 200)
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
					sin.iterator(ctx, t)
					// fmt.Println("timer: 执行")
				case <-ctx.Done():
					// 跳出循环
					break Loop
				}
			}
			log.Println("timer: initend")
		}(ctx)
	})

	return sin
}

// 间隔定时器
// @param space 间隔时间
// @param call 回调函数
// @param extend 附加参数
// @return int 定时器索引
// @return error 错误
func (s *Single) Add(space time.Duration, call callback, extend interface{}) (int, error) {
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
		ExtendData: extend,
	}

	timerMap[fmt.Sprintf("%d", timerCount)] = &t

	if t.NextTime.Before(nextTime) {
		// 本条规则下次需要发送的时间小于系统下次发送时间：替换
		nextTime = t.NextTime
	}

	return timerCount, nil
}

// 删除定时器
func (s *Single) Del(index string) {
	timerMapMux.Lock()
	defer timerMapMux.Unlock()
	delete(timerMap, index)
}

// 迭代定时器列表
func (s *Single) iterator(ctx context.Context, nowTime time.Time) {
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
					s.doTask(ctx, v.Callback, v.ExtendData)
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

// 定时器操作类
// 这里不应painc
func (s *Single) doTask(ctx context.Context, call callback, extend interface{}) error {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("timer:定时器出错", err)
			log.Println("errStack", string(debug.Stack()))
		}
	}()
	return call(ctx, extend)
}
