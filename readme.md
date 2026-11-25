# timerx

基于 Redis 的 Go 定时任务库。它把“到点执行”拆成三种互不干扰的模式：单机周期任务、集群周期任务、分布式一次性任务，用一套 API 覆盖从本地缓存刷新到集群日终对账、延迟关单、失败重试等常见场景。

核心目标只有一个：**在多副本部署下，让同一个任务按时、且只有一个实例执行；并在实例宕机、扩缩容时，通过“就绪队列 + 处理中列表 + 失活回收”保证任务至少被执行一次。**

- 模块路径：`github.com/yuninks/timerx`
- Go：`1.24+`
- Redis：`6.2+`（Cluster / Once 模式使用 `BLMOVE`），支持 standalone 与 Redis Cluster

---

## 目录

- [特性](#特性)
- [三种模式怎么选](#三种模式怎么选)
- [安装](#安装)
- [快速开始](#快速开始)
- [任务类型与调度语义](#任务类型与调度语义)
- [cron 表达式](#cron-表达式)
- [核心使用用例](#核心使用用例)
- [配置项](#配置项)
- [Redis 键布局](#redis-键布局)
- [执行语义与可靠性](#执行语义与可靠性)
- [限制与已知缺陷](#限制与已知缺陷)
- [常见问题](#常见问题)
- [开发与测试](#开发与测试)
- [目录结构](#目录结构)
- [待实现](#待实现)

## 特性

- **三种模式，按部署形态选型**：`Single`（进程内、无依赖）、`Cluster`（多副本、Redis 协调）、`Once`（分布式延迟/一次性任务）。
- **同一任务串行执行**：集群模式用“任务级全局锁”串行化同一 `taskId`（锁默认最长 1 小时）；上一次没跑完时跳过本次并记录日志。
- **Leader 调度**：只有 Leader 计算下次执行时间并把到期任务移入就绪队列，其余实例只负责消费，避免 N 个实例重复计算。
- **可靠消费**：就绪队列使用 `BLMOVE` 原子转移到“实例处理中列表”，处理完 `ack`，暂时不能处理 `nack`，实例失活后由 Leader 把其处理中任务放回队列（至少一次语义）。
- **一次性任务**：支持 `delayTime`、指定时间、批量时间点；回调可返回“重试 + 延迟 + 新附加数据”，并可通过 `WithMaxRetryCount` 限制重试次数。
- **写入原子**：任务数据与执行时间在同一个 Lua 脚本里写入，避免“数据写进去但没排期”的中间状态。
- **多种 cron 解析器**：默认兼容 Linux 5 段 crontab，可选 6 段秒级、`@every/@daily` 等描述符，或自定义 `cron.ParseOption`。
- **灰度/多版本**：按版本号或自定义优先级，只让最高优先级的实例参与调度与消费，适合新旧版本混部。
- **Redis Cluster 友好**：同一 `keyPrefix` 的所有 key 都带 `{keyPrefix}` hash tag，保证 Lua 多 key 操作落在同一个 slot。

## 三种模式怎么选

| 模式 | 入口 | 任务存储 | 典型场景 | 多实例行为 |
| --- | --- | --- | --- | --- |
| `Single` | `timerx.InitSingle(ctx, opts...)` | 进程内存 | 单机周期任务、本地缓存刷新、指标上报 | 每个实例各跑各的，互不协调 |
| `Cluster` | `timerx.InitCluster(ctx, rdb, keyPrefix, opts...)` | Redis | 部署多副本、业务上只需要跑一次的周期任务 | Leader 计算时间，任意实例执行一次 |
| `Once` | `timerx.InitOnce(ctx, rdb, keyPrefix, worker, opts...)` | Redis | 延迟关单、定时提醒、失败重试、一次性补偿 | 全局唯一执行，至少一次 |

一句话判断：**不需要 Redis 就用 `Single`；周期任务用 `Cluster`；“在某个时间点执行一次”用 `Once`。**

## 安装

```bash
go get github.com/yuninks/timerx
```

## 快速开始

### 1. Single：单机周期任务

> 不需要 Redis；任务只存在于当前进程，重启即丢失；调度精度约 `100ms`。

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yuninks/timerx"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	s := timerx.InitSingle(ctx,
		timerx.WithTimeout(30*time.Second), // 单次回调最长执行时间
		timerx.WithLocation(time.Local),    // 按此时区计算“每天/每周/每月”
	)
	defer s.Stop()

	// 每 10 秒刷新一次本地缓存
	index, err := s.EverySpace(ctx, "flush-local-cache", 10*time.Second, flushCache, nil)
	if err != nil {
		log.Fatal(err)
	}

	// 每天 03:15:00 执行
	if _, err := s.EveryDay(ctx, "daily-compact", 3, 15, 0, compact, nil); err != nil {
		log.Fatal(err)
	}

	<-ctx.Done()
	_ = index // 需要单独移除时：s.Del(index)
}

func flushCache(ctx context.Context, _ any) error { return nil }
func compact(ctx context.Context, _ any) error { return nil }
```

`Single` 的 `EveryXxx` 返回自增的 `index`（不是 `taskId`，`Cluster`/`Once` 的注册方法只返回 `error`）。`Del(index)` 移除单个任务，`DelByTaskId(taskId)` 移除所有同名任务；`TaskCount()` 查看当前任务数。

### 2. Cluster：多副本下只执行一次

> 所有实例注册同一批任务（`taskId` 相同）；Redis 负责选主、排期、去重；执行落在哪个实例由消费速度决定。

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yuninks/timerx"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()

	// keyPrefix 用于隔离不同业务/环境；不能为空，也不要带 {}
	clu, err := timerx.InitCluster(ctx, rdb, "order-center")
	if err != nil {
		log.Fatal(err)
	}
	defer clu.Stop()

	// 每天 02:00:00 做日终对账，全集群只会有一个实例执行
	err = clu.EveryDay(ctx, "daily-settle", 2, 0, 0,
		func(ctx context.Context, data any) error {
			return settle(ctx, data.(string))
		}, "2025-01-01")
	if err != nil {
		log.Fatal(err)
	}

	// 每 5 秒同步一次增量数据
	err = clu.EverySpace(ctx, "sync-increment", 5*time.Second, syncIncrement, nil)
	if err != nil {
		log.Fatal(err)
	}

	<-ctx.Done()
}

func settle(ctx context.Context, date string) error { return nil }
func syncIncrement(ctx context.Context, _ any) error { return nil }
```

注意事项：

- `taskId` 在同一个 `keyPrefix` 下全局唯一，重复注册返回 `ErrTaskIdExists`。
- 任务注册表在进程内存中，**不能**在运行时删除；要下线任务请从代码中移除并重启全部实例。
- 每个实例都要注册相同的任务；新版本删除的旧任务，可能仍留在 Redis 有序集合里等待执行（见[限制](#限制与已知缺陷)）。

### 3. Once：延迟任务 / 一次性任务

> 任务保存在 Redis；回调实现 `timerx.Callback`；支持失败重试、延迟重试、修改附加数据。

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yuninks/timerx"
)

const orderTimeout timerx.OnceTaskType = "order-timeout"

type orderWorker struct{}

// Worker 是任务执行入口；返回值决定是否重试
func (orderWorker) Worker(ctx context.Context, taskType timerx.OnceTaskType, taskId string, attachData any) *timerx.OnceWorkerResp {
	if err := closeOrder(ctx, taskId); err != nil {
		// 30 秒后重试；attachData 不改成 nil 就会原样保留
		return &timerx.OnceWorkerResp{Retry: true, DelayTime: 30 * time.Second}
	}
	return &timerx.OnceWorkerResp{} // Retry=false：任务结束，删除
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()

	once, err := timerx.InitOnce(ctx, rdb, "order-center", orderWorker{},
		timerx.WithMaxRetryCount(5), // 0 = 不限次数（默认）
		timerx.WithTimeout(30*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer once.Close()

	// 30 分钟后执行一次；重复提交同一 taskId 会覆盖旧任务
	if err := once.Save(ctx, orderTimeout, "ORDER-10086", 30*time.Minute, map[string]any{"orderId": "ORDER-10086"}); err != nil {
		log.Fatal(err)
	}

	// 不存在才创建，已存在返回 ErrTaskExists
	_ = once.CreateByTime(ctx, orderTimeout, "ORDER-10087", time.Now().Add(time.Hour), nil)

	// 查询/删除
	data, err := once.Get(orderTimeout, "ORDER-10086")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("attachData=%v", data)
	// _ = once.Delete(orderTimeout, "ORDER-10086")

	// 保持进程存活，等待任务到点执行
	<-ctx.Done()
}

func closeOrder(ctx context.Context, orderId string) error { return nil }
```

`OnceWorkerResp` 字段：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `Retry` | `bool` | `false` 表示任务完成并删除；`true` 表示按 `DelayTime` 重新排队。列表任务中 `true` 同时表示“进入下一个时间点” |
| `DelayTime` | `time.Duration` | 重试延迟；仅 `Retry=true` 时生效，`<=0` 表示立即重试；列表任务忽略该字段 |
| `AttachData` | `any` | 非 `nil` 时替换附加数据，供下次执行使用 |

## 任务类型与调度语义

| 方法 | 等价表达 | 说明 |
| --- | --- | --- |
| `EveryMonth(ctx, id, day, hour, minute, second, cb, data)` | 每月 `day` 号 `hour:minute:second` | `day` 超过当月天数时回退到**当月最后一天** |
| `EveryWeek(ctx, id, week, hour, minute, second, cb, data)` | 每周 `week` 的 `hour:minute:second` | `time.Sunday=0`，与 Go 一致 |
| `EveryDay(ctx, id, hour, minute, second, cb, data)` | 每天 `hour:minute:second` | 当天已过则排到明天 |
| `EveryHour(ctx, id, minute, second, cb, data)` | 每小时 `minute:second` | 当前小时已过则排到下一小时 |
| `EveryMinute(ctx, id, second, cb, data)` | 每分钟 `second` 秒 | 当前分钟已过则排到下一分钟 |
| `EverySpace(ctx, id, interval, cb, data)` | 固定间隔 | 以 `2025-01-01 00:00:00` 为基准对齐，保证多实例算出同一个时间点 |
| `Cron(ctx, id, expr, cb, data, opts...)` | cron 表达式 | 解析器由 `Option` 决定，默认 5 段 |

`EverySpace` 要求 `interval > 0`；月/周/日/时/分参数超范围时分别返回 `ErrMonthDay`、`ErrWeekday`、`ErrHour`、`ErrMinute`、`ErrSecond`。

`JobData` 中的 `NextTime` 由内部维护；对外只需要传参。`EverySpace` 的基准时间固定，因此**不要**期望它从“第一次调用”开始计时，它按绝对时间点对齐。

## cron 表达式

默认解析器与 Linux crontab 兼容：`分 时 日 月 周`（5 段，无秒）。用 `Option` 切换：

| Option | 段数/格式 | 示例 |
| --- | --- | --- |
| 默认 | 5 段：`分 时 日 月 周` | `*/5 * * * *` 每 5 分钟 |
| `WithCronParserSecond()` | 6 段：`秒 分 时 日 月 周`，同时支持描述符 | `*/5 * * * * ?` 每 5 秒 |
| `WithCronParserLinux()` | 5 段，显式声明 | `0 3 * * *` 每天 03:00 |
| `WithCronParserDescriptor()` | 仅描述符 | `@every 5s`、`@daily` |
| `WithCronParserOption(opt)` | 自定义 `cron.ParseOption` | `cron.Second \| cron.Minute \| cron.Hour \| cron.Dom \| cron.Month \| cron.Dow` |
| `WithCronParser(parser)` | 直接传入 `cron.Parser` | 复用已有解析器 |

`Cron` 还支持按任务覆盖解析器：

```go
clu.Cron(ctx, "every-5s", "*/5 * * * * ?", cb, nil, timerx.WithCronParserSecond())
s.Cron(ctx, "every-5s", "*/5 * * * * ?", cb, nil, timerx.WithCronParserSecond())
```

6 段表达式里日/周占位通常写 `?`（robfig 会当作 `*` 处理）；5 段解析器遇到 6 段会直接报 `ErrCronExpression`，这是最常见的接入错误。

## 核心使用用例

### 用例 1：单机周期任务（无 Redis）

```go
s := timerx.InitSingle(ctx)
defer s.Stop()
s.EverySpace(ctx, "flush-local-cache", 10*time.Second, flushCache, nil)
s.EveryMinute(ctx, "report-metrics", 30, reportMetrics, nil) // 每分钟第 30 秒
```

适用：本地缓存预热/刷新、进程内表同步、指标上报。不适用：多副本只能执行一次的任务（用 `Cluster`）。

### 用例 2：集群日终对账（每天一次，全集群一份）

```go
clu, _ := timerx.InitCluster(ctx, rdb, "finance")
defer clu.Stop()
clu.EveryDay(ctx, "daily-settle", 2, 0, 0, func(ctx context.Context, _ any) error {
	return settleDaily(ctx)
}, nil)
```

执行落在哪一个实例取决于谁先从 Redis 就绪队列取到任务；其他实例不会重复执行。同一 `taskId` 上一次未结束时，本次会被跳过（日志：`任务正在执行中，跳过本次`）。

### 用例 3：秒级/复杂 cron

```go
clu.Cron(ctx, "sync-inventory", "*/5 * * * * ?", syncInventory, nil, timerx.WithCronParserSecond())
clu.Cron(ctx, "daily-report", "0 30 8 * * *", sendReport, nil, timerx.WithCronParserSecond())
clu.Cron(ctx, "every-90s", "@every 90s", heartbeatReport, nil, timerx.WithCronParserSecond())
```

调度侧最小粒度约 `200ms`（`Cluster` 每 200ms 扫描一次），因此 `*/1 * * * * ?` 的实际执行间隔会有百毫秒级抖动。

### 用例 4：订单超时关闭（延迟任务 + 重试）

```go
once.Save(ctx, "order-timeout", orderId, 30*time.Minute, payload)
```

回调里如果下游超时，返回 `Retry: true, DelayTime: time.Minute`，任务会在 1 分钟后重新入队；配合 `WithMaxRetryCount(5)` 限制总尝试次数。注意：**回调必须幂等**，语义是至少一次。

### 用例 5：多个时间点的提醒/推送

```go
times := []time.Time{
	time.Now().Add(1 * time.Hour),
	time.Now().Add(24 * time.Hour),
	time.Now().Add(72 * time.Hour),
}
once.SaveByList(ctx, "remind", "user-1", times, msg, 0) // 覆盖写入，runCount 从 0 开始
once.CreateByList(ctx, "remind", "user-2", times, msg)  // 不存在才创建
```

`runCount` 表示“已经执行到第几个时间点”，由内部维护；业务侧新建任务传 `0`。时间列表会被排序，最后一个时间点之后 30 分钟数据自动过期。

> ⚠️ 列表任务的时间点推进依赖回调返回 `Retry: true`：每次成功执行后返回 `Retry: true` 才会把游标移到下一个时间点（`DelayTime` 对列表任务无效）；任何一次返回 `Retry: false` 都会删除整条任务，**剩余时间点不再执行**。最后一个时间点执行后返回 `Retry: true` 会因“没有下一个时间点”而自动结束。详见[限制](#限制与已知缺陷)第 21 条。

### 用例 6：灰度发布（新旧版本混部）

```go
// v2.3.0 的实例优先级更高，只有它们参与调度和消费
clu, _ := timerx.InitCluster(ctx, rdb, "order-center", timerx.WithPriorityByVersion("v2.3.0"))
```

版本号会换算成整数优先级：`major*10^12 + minor*10^9 + patch*10^6 + build*10^3 + rev`，每段 `0-999`、最多 5 段。也可以用 `WithPriority(100)` 直接指定。

生效范围：Leader 选举、周期任务调度、周期任务消费、`Once` 消费。低优先级实例会自动让位；Redis 中的优先级 key 有 8 秒 TTL，全部实例下线后优先级会重置。注意：Redis 异常时优先级判断沿用上一次结果（fail-open），切换窗口内可能出现新旧版本同时生效，仍应保证回调幂等。

### 用例 7：生产者/消费者分离

```go
// 只负责投递任务，不消费
producer, err := timerx.InitOnce(ctx, rdb, "order-center", nil, timerx.WithAllowNilFunc(true))
producer.Save(ctx, "order-timeout", orderId, 30*time.Minute, payload)
```

`worker == nil` 且未开启 `WithAllowNilFunc(true)` 时 `InitOnce` 返回错误。生产者实例不会启动调度循环。

### 用例 8：优雅停机

```go
clu, _ := timerx.InitCluster(ctx, rdb, "order-center")
defer clu.Stop() // 可重复调用；关闭后不再取新任务
```

`Stop()` / `Close()` 会关闭守护协程、停止上报存活，并停止取新任务。**不会等待正在执行的回调**：它们会收到被取消的 `ctx`，可以继续跑完；本实例处理中的任务要等心跳超时（默认 30 秒）后才由 Leader 回收。

## 配置项

`InitSingle` / `InitCluster` / `InitOnce` 共享同一组 `Option`，部分选项只对特定模式生效。

| Option | 默认值 | 生效模式 | 说明 |
| --- | --- | --- | --- |
| `WithLogger(logger.Logger)` | 标准库 `log` | 全部 | 只需实现 `Infof` / `Errorf` |
| `WithLocation(*time.Location)` | `time.Local` | 全部 | 计算“每天/每周/每月”时使用的时区 |
| `WithTimeout(d)` | `1h` | 全部 | 单次回调的超时；Once 还会延长任务数据的 TTL 以覆盖执行期 |
| `WithCronParser(p)` | Linux 5 段 | 全部 | 自定义 `cron.Parser` |
| `WithCronParserSecond()` | - | 全部 | 6 段秒级 + 描述符 |
| `WithCronParserLinux()` | - | 全部 | 5 段 crontab |
| `WithCronParserDescriptor()` | - | 全部 | 仅 `@every` 等描述符 |
| `WithCronParserOption(opt)` | - | 全部 | 任意 `cron.ParseOption` |
| `WithBatchSize(n)` | `100` | Cluster / Once | 每次从 zset 移入就绪队列的任务数，小于 1 时按 1 |
| `WithMaxWorkers(n)` | `100` | Cluster / Once | 单实例最大并发回调数，`<=0` 时按 100 |
| `WithMaxRetryCount(n)` | `0`（不限） | Once | 回调重试次数上限 |
| `WithPriority(p)` | 不启用 | Cluster / Once | 直接指定优先级，越大越新 |
| `WithPriorityByVersion(v)` | 不启用 | Cluster / Once | 用版本号换算优先级，如 `v1.2.3` |
| `WithAllowNilFunc(bool)` | `false` | Once | 允许 `worker == nil`，用于只投递不消费的实例 |

`Single` 忽略 `WithBatchSize` / `WithMaxWorkers` / `WithMaxRetryCount` / `WithPriority`：每个到期任务都会起一个 goroutine，同名 `taskId` 不做唯一性校验。

## Redis 键布局

`<prefix>` 是初始化时传入并做过 `{}` 清洗的 `keyPrefix`；同一前缀下所有 key 共享 `{<prefix>}` hash tag，保证 Lua 多 key 操作在 Redis Cluster 中同 slot。

| 模式 | Key | 类型 | 用途 |
| --- | --- | --- | --- |
| Cluster | `timer:{<prefix>}:cluster_zset` | ZSET | 待执行任务，score 为下次执行时间（毫秒） |
| Cluster | `timer:{<prefix>}:cluster_list` | LIST | 就绪队列 |
| Cluster | `timer:{<prefix>}:cluster_instances` | ZSET | 存活实例（score 为 Redis 时间） |
| Cluster | `timer:{<prefix>}:cluster_processing:<instanceId>` | LIST | 当前实例处理中的任务 |
| Cluster | `timer:{<prefix>}:cluster_lock_task_<taskId>` | 锁 | 任务级全局锁，防并发 |
| Cluster | `timer:{<prefix>}:cluster_calc_lock:<taskId>:<ms>` | 短锁 | 下次执行时间写入去重（60s） |
| Cluster | `timer:{<prefix>}:cluster_execinfo` | ZSET | 执行流水，Leader 每 5 分钟清理 15 分钟前的数据 |
| Once | `timer:{<prefix>}:once_zset` | ZSET | 待执行任务 |
| Once | `timer:{<prefix>}:once_list` | LIST | 就绪队列 |
| Once | `timer:{<prefix>}:once_data:<taskType>[:]<taskId>` | STRING | 任务数据（JSON），最后一次执行时间 + 30 分钟后过期 |
| Once | `timer:{<prefix>}:once_instances` | ZSET | 存活实例 |
| Once | `timer:{<prefix>}:once_processing:<instanceId>` | LIST | 处理中任务 |
| Once | `timer:{<prefix>}:once_lock:<taskType>[:]<taskId>` | 锁 | 任务级全局锁 |
| 公共 | `timer:{<prefix>}:leader_lock_<cluster\|once>` | 锁 | Leader 选举 |
| 公共 | `timer:{<prefix>}:leader_<cluster\|once>` | STRING | 当前 Leader 实例（观测用） |
| 公共 | `timer:{<prefix>}:heartbeat_<cluster\|once>` | ZSET | 心跳上报（观测/清理用） |
| 公共 | `timer:{<prefix>}:priority_<cluster\|once>` | STRING | 当前全局最高优先级，TTL 8s |

观测建议：`cluster_execinfo` / `once_execinfo` 能看到“谁在什么时候执行了什么”，`leader_*` 能看到当前 Leader；这两个 key 适合接入监控。

## 执行语义与可靠性

- **Leader 选举**：`leader` 子包通过 Redis 分布式锁选主，每秒校验锁归属；Redis 持续不可用超过 4 秒即主动退位，避免双主长期存在。
- **调度**：Leader 每 `200ms` 遍历本进程注册的任务，计算下次执行时间写入 zset，再把到期任务原子地移入就绪队列；多实例重复计算由 Lua 的“已存在且更早”判断与短锁抑制。
- **消费**：所有实例用 `BLMOVE` 从就绪队列取任务到自己的处理中列表，因此任务在“取出但未 ack”期间不会丢失。
- **失败与重试**：`Cluster` 回调返回 `error` 只记录日志，不重试（下一次周期照常）；`Once` 由回调返回值决定是否重试。
- **至少一次**：实例崩溃后，Leader 会把其处理中列表里的任务放回就绪队列（心跳 5s、失活判定 30s），因此回调**可能被执行多次**，必须幂等。
- **防重入**：同一 `taskId` 通过全局锁串行；`Cluster` 在锁被占用时跳过本次，`Once` 在锁被占用时 `nack` 稍后重试。
- **超时**：回调拿到的 `ctx` 带有 `WithTimeout` 的超时；`Once` 会把任务数据 TTL 延长到 `timeout + 1min`，防止执行期间数据过期。

## 限制与已知缺陷

1. **月末语义**：`EveryMonth(day=31)` 在 2 月会回退到 28/29 号执行，而不是跳过；如果业务要求“只在有 31 号的月份执行”，需要自己判断。
2. **夏令时**：`time.Date` 对不存在的墙钟时间会做归一化且 Go 不保证结果。实测纽约时区 `EveryDay(2, 30, 0)` 在 2024-03-10（春季跳变）被排到 01:30，即**提前 1 小时执行**；秋季重复的 01:30 只执行一次。对 DST 敏感的任务请统一使用 UTC 时区，或在业务侧校验实际执行时间。
3. **时钟依赖**：`Cluster` 的下次执行时间由各实例本地时钟计算，生产环境请保证 NTP 同步；`Once` 的存活判定使用 Redis `TIME`，不受本地时钟影响。
4. **滚动发布丢任务**：任务注册表在内存，新旧版本混部时，旧版本注册的任务可能被新版本实例从 zset 取出后因“本地未注册”而丢弃。请配合优先级灰度，或按“先下线旧任务、再发布新版本”的节奏操作。
5. **至少一次，不是恰好一次**：网络分区、实例失活回收、Redis 主从切换都可能导致重复执行；回调必须幂等。
6. **调度开销与任务数量成正比**：Leader 每 200ms 对本进程所有任务各发一次 Lua，任务数大时 Redis 压力明显；建议单实例任务保持在百级，或按业务拆分 `keyPrefix`。
7. **单 slot 热点**：为了 Lua 原子性，同一 `keyPrefix` 的所有 key 在 Redis Cluster 中位于同一个 slot；任务量极大时应拆分成多个 `keyPrefix`。
8. **Once 的数据会过期**：任务数据 TTL = 最后执行时间 + 30 分钟。如果队列积压超过这个时间，任务数据先过期，取出后会因“数据不存在”被当作已完成而静默丢弃。
9. **Once 的分隔符**：内部用 `[:]` 拼接 `taskType` 与 `taskId`，二者不要包含 `[:]`，否则 `Worker` 收到的 `taskId` 会被截断。
10. **附加数据必须可 JSON 序列化**：`attachData` 会 `json.Marshal`，`func`、`chan`、未导出字段、`time.Time` 之外的自定义类型需确认往返无损。
11. **默认 cron 是 5 段**：秒级表达式必须显式使用 `WithCronParserSecond()`；`@every` 需要带 `Descriptor` 的解析器。
12. **`JobTypeEverySecond` 未实现**：常量存在，但 `GetNextTime` 不支持该类型，请用 6 段 cron 表达“每秒”。
13. **`Single` 无持久化、无跨实例协调**：重启即丢任务；同名 `taskId` 可重复注册（按 index 管理），`DelByTaskId` 会一次删除所有同名任务。
14. **`Single` 的调度精度约 100ms**：`EverySpace(1*time.Millisecond)` 不会按毫秒执行，实际约 100ms 一次。
15. **`Cluster` 无法运行时删除任务**：没有 `Del` 类 API，只能改代码并重启。
16. **默认无限重试**：`Once` 的 `WithMaxRetryCount` 默认为 `0`（不限），如果回调总是返回 `Retry: true, DelayTime: 0`，任务会立即无限重试，请务必设置上限或退避时间。
17. **日志能力有限**：默认 logger 只输出到标准库 `log`，忽略 `ctx`；`trace_id` 目前不会出现在日志中，生产建议注入自己的 `logger.Logger`。
18. **`heartbeat` 子包只写不读**：它维护的心跳集合没有消费方，仅用于外部观测；真正的失活回收由 `queue` 子包的 `instances` 集合完成，两者不要混淆。
19. **代码风格遗留**：`context.WithValue` 使用了字符串 key（`trace_id` 实际也不会进入日志）；`Cluster` 的 `cache`、`maxWorkers` 字段赋值后从未读取；`JobData.Month` 字段未被任何调度分支使用；构造函数里还保留着多处被注释的 `CreateTime` 代码。
20. **主从切换**：Redis 主从切换期间可能出现锁丢失、任务重复；`Leader` 有 1 秒校验与 4 秒退位机制，但无法保证强一致，切主窗口内请依赖回调幂等。
21. **`SaveByList` 的推进语义不直观**：列表任务只有在回调返回 `Retry: true` 时才把游标移到下一个时间点；返回 `Retry: false` 会删除整条任务，剩余时间点全部丢失。且 `WithMaxRetryCount` 也会按 `RunCount` 截断列表。这是当前实现最容易被误用的地方（一旦改成“成功也自动推进”会改变现有行为，需要显式设计）。
22. **空 `keyPrefix` 在 Redis Cluster 下不可用**：`keyPrefix=""` 会生成 `timer:{}:...` 的空哈希标签，等价于没有标签，多 key Lua 会报 `CROSSSLOT`。Standalone 模式不受影响；请始终传入非空前缀。
23. **底层锁最长 1 小时**：`lockx` 默认 `lockTimeout=60min`，超时后不再续期（`Expiry` 默认 5s）。因此超过 1 小时的回调可能被其他实例并发执行，Leader 也会每小时被动退位一次。长任务请显式评估 `WithTimeout` 与 `lockx` 配置。
24. **`Create` 可能误报“已存在”**：`Create` 在获取任务锁失败时直接返回 `ErrTaskExists`，这代表“同 ID 任务正在创建/执行”，并不严格代表任务数据已存在。

## 常见问题

**Q：任务没有执行，从哪里查？**

1. 是否注册成功（返回值、日志 `addJob taskId`）；2. `leader_<mode>` 是否指向某个存活实例；3. Redis 版本是否 `>= 6.2`；4. `cron` 段数与解析器是否匹配；5. `keyPrefix` 是否与实例一致；6. `cluster_zset` 中是否有该任务。

**Q：为什么同一任务执行了两次？**

“至少一次”是设计语义：实例崩溃/失活回收、Redis 切换、回调超时后重试都会导致重复。请保证回调幂等，并在业务侧用唯一键去重。

**Q：如何保证任务不被并发执行？**

同一 `taskId` 有全局锁。`Cluster` 在上一次未结束时**跳过**本次；`Once` 会 `nack` 后重试。跨 `taskId` 的互斥需要业务自己做。

**Q：能接入 Redis Cluster 吗？**

可以，传 `redis.UniversalClient`（`*redis.ClusterClient`）即可。所有 key 已带 hash tag，Lua 脚本不会跨 slot。参考 `example/cluster_redis`。

**Q：任务量很大怎么办？**

拆分 `keyPrefix` 到多个 slot；降低任务数量（用 `Once` 承担一次性任务）；用 `WithBatchSize` / `WithMaxWorkers` 控制单次吞吐；避免超高频（100ms 级）周期任务。

**Q：关闭后任务会丢吗？**

`Once` 的任务在 Redis 中，不丢；`Cluster` 的下次执行时间在 zset 中，重启后重新计算；`Single` 的任务只在内存，重启即丢失。

## 开发与测试

```bash
go build ./...
go vet ./...
go test ./... -count=1
```

- 根包与 `priority`、`leader` 的部分用例依赖本地 Redis（`localhost:6379`，密码 `123456`，与示例一致）；不可用时 `regression_test.go`、`leader` 的用例会 `Skip`，而 `priority` 的用例会失败。
- `go test -race` 需要本机 CGO 工具链（Windows 下需 gcc），CI 建议在 Linux 上跑。
- 单元测试覆盖：`GetNextTime` 全分支、Option 默认值、key 哈希标签、`Single` 基础能力、`Once` 键格式、leader 退位；集成测试覆盖在 `regression_test.go`。
- `cluster_test.go` / `once_test.go` 的用例在 Redis 不可用时不会失败，但多数只断言“注册返回 error 为 nil”，不验证实际执行，不能作为调度正确性的回归依据。

## 目录结构

```text
.
├── single.go        # Single：进程内定时器（100ms 轮询）
├── cluster.go       # Cluster：Redis 集群定时器（leader 调度 + 队列消费）
├── once.go          # Once：分布式一次性/延迟任务
├── queue.go         # 就绪队列：BLMOVE、ack/nack、失活回收
├── next_time.go     # 下次执行时间计算（月/周/日/时/分/间隔/cron）
├── option.go        # 通用 Option 与 cron 解析器
├── types.go         # JobType / JobData
├── keys.go          # keyPrefix 清洗（hash tag 安全）
├── error.go         # 错误定义
├── leader/          # Leader 选举（锁 + 校验）
├── heartbeat/       # 实例心跳上报
├── priority/        # 优先级/版本灰度
├── logger/          # 日志接口与默认实现
├── example/         # single / cluster / cluster_redis / once 示例
└── cmd/             # 手工调试入口
```

## 待实现

- [ ] 允许执行完重置任务倒计时（自定义下一次执行时间）
- [ ] `Cluster` 支持运行时注销任务
- [ ] `Single` 支持持久化与跨实例协调
- [ ] 更完善的 metrics（执行耗时、成功/失败、队列积压）

