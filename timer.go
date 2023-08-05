package timer

// 作者：黄新云

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// 定时器
// 原理：每毫秒的时间触发

type singleTimer struct {
	Callback   callback // 需要回调的方法
	CanRunning chan (struct{})
	BeginTime  time.Time     // 初始化任务的时间
	NextTime   time.Time     // 下一次执行的时间
	SpaceTime  time.Duration // 间隔时间
	Params     []int
}

type clusterTimer struct {

}

var timerMap = make(map[int]*singleTimer)
var timerMapMux sync.Mutex
var timerCount int        // 当前定时数目
var onceLimit sync.Once   // 实现单例
var nextTime = time.Now() // 下一次执行的时间



// 定时器类
func InitSingle(ctx context.Context) {
	onceLimit.Do(func() {
		timer := time.NewTicker(1 * time.Millisecond)
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

func InitCluster(ctx context.Context) {}

// 添加需要定时的规则
func AddToTimer(space time.Duration, call callback) int {
	timerMapMux.Lock()
	defer timerMapMux.Unlock()

	timerCount += 1
	// 计算出首次开始时间和时间间隔，保存在map里面

	nowTime := time.Now()

	t := singleTimer{
		Callback:   call,
		BeginTime:  nowTime,
		NextTime:   nowTime.Add(space),
		SpaceTime:  space,
		CanRunning: make(chan struct{}, 1),
	}
	timerMap[timerCount] = &t

	if t.NextTime.Before(nextTime) {
		// 本条规则下次需要发送的时间小于系统下次发送时间：替换
		nextTime = t.NextTime
	}

	return timerCount
}

func DelToTimer(index int) {
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

	for k, v := range timerMap {
		v := v
		// 判断执行的时机
		if v.NextTime.Before(nowTime) {
			// fmt.Println("NextTime", v.NextTime.Format("2006-01-02 15:04:05.000"))

			// TODO:这个有问题:假如加上一个时间段还是比当前时间小，会导致连续多次执行
			v.NextTime = v.NextTime.Add(v.SpaceTime)

			if k == 0 {
				// 循环的第一个需要替换默认值
				newNextTime = v.NextTime
			}

			// 获取最小的
			if v.NextTime.Before(newNextTime) {
				// 本规则下次发送时间小于系统下次需要执行的时间：替换
				newNextTime = v.NextTime
			}

			// 处理中就跳过本次
			go func(ctx context.Context, v *singleTimer) {
				select {
				case v.CanRunning <- struct{}{}:
					// TODO: 需要考虑分布式锁
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
					timerAction(ctx, v.Callback)
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
type callback func(context.Context) bool

// 定时器操作类
// 这里不应painc
func timerAction(ctx context.Context, call callback) bool {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("timer:定时器出错", err)
		}
	}()
	return call(ctx)
}
