# 定时任务 scheduler

`scheduler` 是一个嵌入业务进程的分布式定时任务组件。任务持久化在 PostgreSQL，近期窗口内的
到期触发由 Redis 承担：PostgreSQL 是唯一真相源，Redis 只保存窗口内任务的索引和易失任务
的内容，可以丢失并按对账重建。

- 至少执行一次，回调必须幂等；不保证任务之间的顺序。
- 精度约为 `PollInterval`（默认 100ms）加一次 PostgreSQL 批量读取。
- Redis 不可用时登记照常成功，触发暂停，恢复后由对账补齐。
- 依赖 `*sqlx.DB`（驱动 `pgx`，PostgreSQL 14+）与 `redis.UniversalClient`
  （Redis 6.2+，单机与 Cluster 均可），不新增第三方依赖。

与 `pkg/compensator` 的取舍：compensator 只依赖 PostgreSQL，适合与业务事务绑定、量不大的
补偿和延迟任务；`scheduler` 多依赖一个 Redis，换取更高的到期吞吐和更小的 PostgreSQL 压力。

## 快速开始

```go
type OrderTimeout struct {
    OrderID int64 `json:"order_id"`
}

s, err := scheduler.New(ctx, db, rdb, scheduler.Config{
    TableSuffix: "orders", // 必填，决定表名和 Redis key
}, map[string]scheduler.Handler{
    "order.timeout": func(ctx context.Context, task *scheduler.Task) error {
        var p OrderTimeout
        if err := task.Decode(&p); err != nil {
            return err
        }
        // 回调须幂等：以 task.ID 作为下游幂等键。
        return closeOrder(ctx, task.ID, p)
    },
})
if err != nil {
    return err
}
defer s.Close()

task, err := scheduler.NewTask("order.timeout", OrderTimeout{OrderID: 42})
if err != nil {
    return err
}
task.RunAt = time.Now().Add(30 * time.Minute)

// 独立登记：近期任务提交后立即写入 Redis，远期任务等进入窗口后由 loader 搬入。
id, err := s.Add(ctx, task)

// 与业务同事务：任务与业务数据一起提交或回滚。
err = s.Transaction(ctx, func(tx *scheduler.Tx) error {
    if _, err := tx.SQLTx().ExecContext(ctx, "UPDATE orders SET status = 'timeout' WHERE id = $1", 42); err != nil {
        return err
    }
    _, err := tx.Add(ctx, task)
    return err
})

// 接入调用方已有的事务：始终以 pending 写入，由 loader 在 LoadInterval 内入队。
id, err = s.AddTx(ctx, sqlTx, task)
```

`Payload` 是 sonic 编码的 JSON 字符串：`NewTask` 用 `sonic.MarshalString` 编码，
`Task.Decode` 用 `sonic.UnmarshalString` 解码。库中以 `TEXT` 原样保存，不做 JSON 校验或规范化。

### 注意：10 秒内的 Add 任务默认不持久化

`VolatileBelow` 默认为 10 秒。**经 `Add` 登记且延迟小于该值的任务默认只写 Redis，完全不进
PostgreSQL，Redis 丢数据时这类任务会丢失。** 不能丢的短延迟任务必须：

- 设置 `task.Durable = true`，或
- 经 `Transaction` / `AddTx` 登记（事务中的任务始终是持久任务）。

`VolatileBelow` 设为负数可关闭阈值，此时只有显式 `task.Volatile = true` 的任务才是易失任务。

```go
ping, _ := scheduler.NewTask("room.heartbeat", Heartbeat{RoomID: 7})
ping.RunAt = time.Now().Add(3 * time.Second)
ping.Volatile = true      // 只进 Redis，可丢
id, err := s.Add(ctx, ping) // Redis 写入失败时直接返回错误，没有兜底
```

易失任务的限制：

- 只能经 `Add` 登记；`Transaction` / `AddTx` 中登记 `Volatile = true` 返回 `ErrInvalidArgument`。
- 延迟不得超过 `Window`，payload 不得超过 `VolatileMaxPayload`（默认 4KB）。显式 `Volatile = true` 的任务不满足时返回 `ErrInvalidArgument`；只是被 `VolatileBelow` 阈值自动判定的任务不满足时回落为持久任务。
- `Get`、`Retry` 不支持易失任务，返回 `ErrNotFound`。
- 失败且次数耗尽时直接丢弃并上报指标与日志，不留记录。
- 部署时 Redis 的淘汰策略必须是 `noeviction`，否则持久任务的索引也会受影响。

## 任务状态

| 值 | 名称 | 含义 |
| --- | --- | --- |
| 0 | pending | 只在 PostgreSQL，尚未进入 Redis |
| 1 | queued | 已进入（或应当进入）Redis，包含执行中 |
| 2 | succeeded | 成功 |
| 3 | failed | 尝试次数耗尽 |
| 4 | cancelled | 已取消 |

PostgreSQL 中没有 `running` 状态，执行中的事实由 Redis `processing` 集合表达。`queued` 是
乐观状态：登记、loader 先改 PostgreSQL 再写 Redis，两步之间崩溃时任务仍是 `queued`，
由 reconciler 补回。

回调收到的 `Task.Attempts` 是已失败的尝试次数，首次执行时为 0。任务 ID 是 UUIDv7，
可使用 `Get` 观察状态，或作为下游幂等键。

## 登记路径

| 入口 | 近期任务（`run_at < now + Window`） | 远期任务 |
| --- | --- | --- |
| `Add` / `Transaction` | 以 `queued` 写入 PostgreSQL，提交后立即入队 Redis | 以 `pending` 写入 |
| `AddTx` | 以 `pending` 写入，由 loader 在 `LoadInterval` 内入队 | 同左 |

- 入队失败不回滚、不报错，由 reconciler 在 `run_at + ReconcileGrace` 后补回。
- `AddTx` 不知道调用方何时提交，所以短延迟任务最多增加一个 `LoadInterval` 的触发延迟。
- 零值或过去的 `RunAt` 按立即执行处理。
- 登记时未注册的任务类型返回 `ErrNoHandler`。

## API

```go
s, err := scheduler.New(ctx, db, rdb, config, handlers)
defer s.Close()

id, err := s.Add(ctx, task)
err = s.Transaction(ctx, func(tx *scheduler.Tx) error { ... })
id, err = s.AddTx(ctx, sqlTx, task)
record, err := s.Get(ctx, id)
err = s.Cancel(ctx, id)   // pending / queued -> cancelled；不存在 ErrNotFound，终态 ErrFinished
err = s.Retry(ctx, id)    // failed -> pending，attempts 清零，由 loader 重新入队
err = s.Register("order.refund", handleRefund) // 运行期注册
stats, err := s.Stats(ctx)
```

`Close` 停止各循环，等待执行中的回调至多 `ShutdownTimeout`，等待期间不取消回调的 context。
超时后才取消，被中断的任务立即放回且不计尝试次数；忽略取消的回调靠租约回收。
可重复调用，不会关闭传入的连接池和 Redis 客户端。Close 时已弹出但尚未开始执行的任务会立即
`requeue` 放回。

## 配置

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `TableSuffix` | 必填 | `[a-z0-9_]{1,40}`，决定表名和 Redis key |
| `Schema` | `current_schema()` | 必须已存在 |
| `KeyPrefix` | `sched` | Redis key 前缀 |
| `Buckets` | 8 | Redis 分桶数，**上线后不可直接修改** |
| `Window` | 5m | 近期窗口，决定 Redis 中的任务数量 |
| `LoadInterval` | 1s | loader 扫描间隔 |
| `LoadBatch` | 1000 | loader 单批行数 |
| `PollInterval` | 100ms | 到期扫描间隔，决定触发精度 |
| `WorkerCount` | 16 | 每个节点同时执行的回调数 |
| `HandlerTimeout` | 30s | 单次回调超时 |
| `LeaseDuration` | 60s | 执行租约，必须大于 `HandlerTimeout` |
| `MaxAttempts` | 10 | 最大尝试次数，含首次 |
| `RetryBackoffMin` / `Max` | 1s / 5m | 重试退避范围，`Max <= Window` |
| `ReclaimInterval` | 5s | 租约回收间隔 |
| `ReconcileInterval` | 5s | 对账间隔 |
| `ReconcileGrace` | 10s | 任务逾期多久后才参与对账 |
| `VolatileBelow` | 10s | 经 `Add` 登记且延迟小于该值的任务自动成为易失任务；负数关闭 |
| `VolatileMaxPayload` | 4KB | 易失任务 payload 上限 |
| `Retention` | 24h | `succeeded` / `cancelled` 保留时长 |
| `FailedRetention` | 0（不删除） | `failed` 保留时长 |
| `CleanupInterval` | 1m | 清理间隔 |
| `ShutdownTimeout` | 30s | `Close` 等待回调的上限 |

存储参数（fillfactor、autovacuum 阈值）沿用 compensator；已存在的表不会被覆盖。

## Redis 布局

```text
ready       = {KeyPrefix}:{<suffix>:<bucket>}:ready        ZSET, score = run_at 的 Unix 毫秒
processing  = {KeyPrefix}:{<suffix>:<bucket>}:processing   ZSET, score = 租约到期的 Unix 毫秒
data        = {KeyPrefix}:{<suffix>:<bucket>}:data         HASH, 只存易失任务：id -> 内容
member      = 任务 ID 的 16 字节二进制
bucket      = fnv1a32(id) % Buckets
```

花括号是 Redis Cluster 的 hash tag，保证同一个桶的三个 key 落在同一个 slot。`Buckets` 上线后
修改需要先停止登记并排空 Redis。

## 故障与一致性

| 故障 | 结果 | 恢复 |
| --- | --- | --- |
| Redis 不可用 | 登记成功，触发暂停 | Redis 恢复后 reconciler 补齐，逾期任务立即执行 |
| Redis 丢失部分或全部数据 | 持久任务延迟；易失任务丢失 | 持久任务在 `run_at + ReconcileGrace` 后补回 |
| PostgreSQL 不可用 | 登记失败；已弹出的任务放回 Redis | PostgreSQL 恢复后继续 |
| loader 在写 Redis 前崩溃 | 该批任务延迟 | reconciler 补回 |
| worker 在执行中崩溃 | 任务停在 processing | 租约到期后 reclaimer 回收，计一次尝试 |
| 回调成功但写回 PostgreSQL 失败 | 任务重复执行一次 | 依赖回调幂等 |
| 任务被取消或完成后 Redis 仍有成员 | 不会执行 | 弹出时 PostgreSQL 状态校验拦截 |

会产生重复执行的情况：回调成功后写回失败、回调实际耗时超过租约、Redis 丢失 processing
数据后对账重新入队。三者都属于至少一次语义。

## 指标

实现 `Metrics` 接口并赋给 `Config.Metrics`，即可接入 Prometheus / OpenTelemetry：

- 计数：登记、入队、弹出、成功、失败、重试、取消、租约回收、对账补回、失效成员丢弃、
  未注册类型放回。
- 直方图：触发延迟（开始执行时间减 `run_at`）、回调耗时（见 `ExecEvent`）。
- 仪表：`Stats` 返回 Redis `ready` 中已到期数量、`processing` 数量、PostgreSQL 中
  `pending` / `queued` 数量与最老逾期时长。

`Stats` 的 PostgreSQL 部分与积压量成正比，多节点部署时建议只由一处低频采集。

## 运维要求

- PostgreSQL 14+，驱动 `pgx`；组件启动时自动建表并校验列与索引。
- Redis 6.2+，单机或 Cluster；淘汰策略必须为 `noeviction`。
- 所有节点必须注册相同的回调集合。弹出到未注册类型时延迟 1 秒放回并上报指标，不计尝试次数。
- 关闭进程前调用 `Close`；租约回收是异常退出的最后保障，不应作为常规关闭路径。
- 应用节点、PostgreSQL、Redis 的时钟必须用 NTP 同步。持久任务的 `RunAt` 是调用方给出的绝对
  时间，是否到期由 Redis 时钟判断，三者的时钟差会直接变成触发误差，Redis 时钟较快时任务会
  提前触发。易失任务按「Redis 当前时间加延迟」计算到期时间，不受应用节点时钟影响。
- 触发延迟约为 `PollInterval` 加每个桶一次 Redis 往返，再加持久任务的一次批量读取。Redis 与
  应用不在同一内网时，往返耗时会明显拉大这个值。
- 数据库连接池要容纳结果写回的并发：每个执行中的持久任务结束时占用一个连接，连接数明显小于
  `WorkerCount` 时结果写回会排队，表现为触发延迟变大。

## 测试

单元测试不依赖外部服务。集成测试的环境变量与 `pkg/compensator` 一致：

```bash
TEST_POSTGRES_DSN='postgres://user:pass@127.0.0.1:5432/kc?sslmode=disable' \
KC_REDIS_TEST_DSN='redis://127.0.0.1:6379/0' \
go test ./pkg/scheduler -count=1
```

- `KC_REDIS_TEST_DSN` 带 `addr` 参数时按 Cluster DSN 解析（如
  `redis://:pass@10.0.0.1:6379/0?addr=10.0.0.1:6380&addr=10.0.0.1:6381`），整套端到端用例都跑在
  Cluster 上；否则按单机解析。
- `KC_REDIS_CLUSTER_TEST_DSN` 用于额外的 Cluster 脚本测试；未设置时跳过。
- `TEST_SCHEDULER_LOG` 非空时把组件日志输出到 stderr。
- 集成测试使用随机表后缀与 Redis key 前缀，结束时删除表并清理 key。

## 容量与压测

每个任务的开销：

| 路径 | PG 写 | PG 读 | Redis |
| --- | --- | --- | --- |
| 近期任务（`Add` / `Transaction`） | 2（登记、结果） | 1（批量） | 入队、弹出、确认 |
| 远期任务或 `AddTx` | 3（登记、入队标记、结果） | 1（批量） | 入队、弹出、确认 |
| 易失任务 | 0 | 0 | 入队、弹出、确认 |

Redis 内存只与窗口内的任务数有关，与任务总量无关；易失任务另外占用 payload 空间。持久任务的
瓶颈通常在 PostgreSQL 的登记与结果写入；易失任务的瓶颈在 Redis 和回调本身。

压测入口：

```bash
TEST_POSTGRES_DSN=... KC_REDIS_TEST_DSN=... \
go test ./pkg/scheduler -run '^$' -bench 'BenchmarkAdd' -benchtime 10s
```

压测报告须记录硬件、PostgreSQL / Redis 版本、存量规模、payload 大小、节点数和负载曲线。
README 中的吞吐和延迟数字应在压测后按此格式补充，而不是估算。
