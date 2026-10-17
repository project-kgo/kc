# 本地任务补偿 compensator

`compensator` 让业务在自己的 PostgreSQL 事务里登记任务：首次就地执行回调，成功直接落为成功状态，不再进入轮询；失败或进程崩溃时由后台按指数退避在多个节点间补偿，直到成功或耗尽重试次数。只依赖业务现有的 `*sqlx.DB`（驱动 `pgx`，PostgreSQL 14+），不依赖 Redis 或服务发现。

## 快速开始

```go
type OrderPaid struct {
    OrderID int64 `json:"order_id"`
}

comp, err := compensator.New(ctx, db, compensator.Config{
    TableSuffix: "orders", // 必填，决定表名
}, map[string]compensator.Handler{
    "order.paid": func(ctx context.Context, task *compensator.Task) error {
        var event OrderPaid
        if err := task.Decode(&event); err != nil {
            return err
        }
        // 回调须幂等：以 task.ID 作为下游幂等键。
        return notifyWarehouse(ctx, task.ID, event)
    },
})
if err != nil {
    return err
}
defer comp.Close()

task, err := compensator.NewTask("order.paid", OrderPaid{OrderID: 42})
if err != nil {
    return err
}
results, err := comp.Transaction(ctx, func(tx *compensator.Tx) error {
    if _, err := tx.SQLTx().ExecContext(ctx, "UPDATE orders SET status = 'paid' WHERE id = $1", 42); err != nil {
        return err
    }
    _, err := tx.Add(ctx, task)
    return err
})
if err != nil {
    return err // 业务失败或提交失败，没有任何任务生效
}
for _, r := range results {
    if r.Err != nil {
        // 业务已提交，任务已登记为待重试，后台会补偿；不要因此重做业务。
        logger.Warn("首次执行失败，等待补偿", "task_id", r.ID, "error", r.Err)
    }
}
```

`Payload` 是 sonic 编码的 JSON 字符串：`NewTask` 用 `sonic.MarshalString` 编码，`Task.Decode` 用 `sonic.UnmarshalString` 解码。已编码的字符串可直接赋值 `Task{Type: "...", Payload: s}`。库中以 `TEXT` 原样保存，不做 JSON 校验或规范化。

## 两种回调时机

| 模式 | 流程 | 适用 |
|---|---|---|
| `ModeInTx`（默认） | 业务函数返回后、提交前执行回调，再用**一条多行 INSERT** 写入：成功的直接为 `succeeded`，失败的为 `pending` 等待补偿 | 回调成功即可结束，热路径只有 1 条 SQL |
| `ModeAfterCommit` | 事务内写入 `running` 且由本进程持有租约的任务，提交后立即回调，再按令牌落库结果 | 回调必须看到已提交的业务数据，或不希望在事务内等待外部调用 |

`ModeInTx` 下回调发生在提交之前：如果随后 INSERT 或提交失败，回调已经产生的外部副作用无法撤销，因此回调必须幂等，并能容忍业务最终未提交。回调期间事务保持打开，回调应尽量快（受 `HandlerTimeout` 约束）。多个任务的首次回调并发执行。

`ModeAfterCommit` 下进程在提交后、回调前崩溃时，任务的租约（`LeaseDuration`）到期后由后台接管。

### 接入已有事务

```go
tx, _ := db.BeginTxx(ctx, nil)
// ... 业务写入
result, err := comp.AddTx(ctx, tx, task) // ModeInTx：立即回调并写入
if err != nil {
    return err
}
if err := tx.Commit(); err != nil {
    return err
}
// ModeAfterCommit：提交成功后调用 Dispatch 立即执行；遗漏时租约到期由后台补偿。
results, err := comp.Dispatch(ctx, result.ID)
```

`Dispatch` 也可用于立即执行已到期的待重试任务；正被其它执行者处理、未到期或已终态的任务不会重复执行，返回其当前状态。

## 配置

| 字段 | 默认值 | 说明 |
|---|---|---|
| `TableSuffix` | 必填 | `[a-z0-9_]{1,40}` |
| `Schema` | `current_schema()` | 必须已存在 |
| `Mode` | `ModeInTx` | 见上节 |
| `WorkerCount` | 8 | 后台补偿 worker 数（不限制前台首次回调） |
| `ScanInterval` | 1s | 空闲扫描间隔；领取满批后立即继续 |
| `BatchSize` | 2×WorkerCount | 单次领取上限，实际不超过空闲 worker 数 |
| `MaxRetries` | 10 | 不含首次；`-1` 表示不重试 |
| `RetryBackoffMin` / `RetryBackoffMax` | 1s / 5min | 指数退避，叠加 50%～100% 随机抖动 |
| `HandlerTimeout` | 30s | 回调 context 超时；回调忽略超时返回 nil 也按失败处理 |
| `LeaseDuration` | HandlerTimeout+10s | running 任务被其它节点接管前的期限，必须大于 `HandlerTimeout` |
| `CleanupAfter` | 5min | 成功记录保留时长 |
| `CleanupInterval` / `CleanupBatchSize` | 30s / 1000 | 清理间隔与每批删除行数（每轮最多 100 批） |
| `FailedRetention` | 0 | >0 时清理失败超过该时长的记录；默认保留，便于排查和 `Retry` |
| `HeartbeatInterval` / `NodeTTL` | 3s / 15s | 节点与分片租约，`NodeTTL ≥ 3×HeartbeatInterval` |
| `OperationTimeout` | 5s | 后台单次数据库操作超时 |
| `DisableWorker` | false | 只登记和首次执行，不参与分片与补偿（如只写的批处理进程） |
| `Logger` | `slog.Default()` | 不记录载荷 |

最多执行 `1 + MaxRetries` 次。进程在领取后崩溃同样消耗一次预算，防止毒任务导致无限崩溃循环。耗尽后状态为 `failed`，可调用 `Retry(ctx, id)` 重置预算（ID 不变）。

## 监控

`Config.Metrics` 接收运行事件，组件本身不依赖具体监控库：

```go
type Metrics interface {
    Executed(ExecEvent)                // 每次回调执行后：类型、来源、第几次、落库状态、耗时、错误
    Cleaned(status Status, rows int64) // 每批清理后
    ShardsOwned(owned int)             // 每次分片协调后本节点持有的分片数
}
```

`ExecEvent.Source` 区分 `first_run`（事务内首次）、`after_commit`、`dispatch`、`background`（补偿）。方法在执行路径上同步调用，实现必须并发安全且不阻塞（如 Prometheus counter/histogram）。

积压用 `Stats(ctx)` 低频采集（如每 30 秒），返回全表 pending/running/failed 数量和最早到期时间，适合做 gauge 并配置告警：`failed > 0`、`now - OldestDue` 持续增大（补偿跟不上）。多节点部署时只需一处采集。

## 表结构

后缀 `orders` 创建两张表：

- `compensator_orders`：任务主表，主键为应用生成的 UUIDv7（回调前就有稳定 ID，时间有序利于 B-tree 追加）。
  - `compensator_orders_due_idx (shard_id, next_run_at) WHERE status < 2`：只包含待执行/执行中的任务，首次即成功的任务不进入，扫描始终很小。
  - `compensator_orders_done_idx (shard_id, status, completed_at) WHERE status >= 2`：清理使用。
- `compensator_orders_lease`：协调表，64 行分片（`kind=0`）加每个节点一行心跳（`kind=1`）。

多个节点并发启动时，建表在 `pg_advisory_xact_lock` 下串行执行，并校验列类型、索引和分片行。组件不做自动迁移，也不会覆盖已有表的存储参数。

## 多节点

任务插入时随机落入 64 个逻辑分片之一，之后不再搬迁。每个节点每个心跳周期：续约自己的心跳行、统计存活节点数 n，持有分片超过 `ceil(64/n)` 的主动释放，不足的用 `FOR UPDATE SKIP LOCKED` 随机抢占过期分片。各节点只扫描、清理自己持有的分片，互不竞争；节点失联后其分片在 `NodeTTL` 后被其它节点接管，`Close` 时主动释放以便立即接管。超过 64 个节点时部分节点没有分片（仍可登记和首次执行）。

分片归属只用于划分负载，正确性不依赖它：领取使用 `FOR UPDATE SKIP LOCKED` 并写入随机令牌，结果落库必须匹配令牌，交接期间旧执行者的结果不会覆盖新状态。领取是一条语句：对持有的分片逐个走 `due_idx` 取到期任务，只领取空闲 worker 数量的任务，避免任务在本地排队导致租约过期。

所有时间判断使用数据库时钟，节点时钟漂移不影响租约。

组件提供**至少一次**执行，不保证顺序：回调成功后在结果落库前崩溃，任务会再次执行。

## 空间回收与运维

成功记录在 `CleanupAfter` 后由持有对应分片的节点按批 `DELETE`。任务表创建时带有以下参数，使删除产生的死元组被及时回收，堆页与索引页的空闲空间被后续写入复用，稳定负载下表大小保持平稳：

```sql
WITH (
  fillfactor = 90,
  autovacuum_vacuum_scale_factor = 0,          -- 按绝对死元组数触发，不随表变大而迟钝
  autovacuum_vacuum_threshold = 2000,
  autovacuum_vacuum_insert_scale_factor = 0,   -- 写多删多的负载也及时 vacuum
  autovacuum_vacuum_insert_threshold = 10000,
  autovacuum_analyze_scale_factor = 0.02,
  autovacuum_vacuum_cost_delay = 1,            -- 降低节流，跟上删除速度
  toast.autovacuum_vacuum_scale_factor = 0,
  toast.autovacuum_vacuum_threshold = 2000
)
```

`vacuum_truncate` 保持默认开启，表尾部的空页会被 vacuum 截断归还操作系统；中间的空闲空间留给后续写入复用。需要立刻缩小文件时使用 `pg_repack`，不要在业务高峰执行 `VACUUM FULL`（全表排他锁）。

```sql
-- 表与索引大小
SELECT pg_size_pretty(pg_relation_size('compensator_orders')) AS heap,
       pg_size_pretty(pg_indexes_size('compensator_orders')) AS indexes,
       pg_size_pretty(pg_total_relation_size('compensator_orders')) AS total;

-- 死元组与 autovacuum 情况
SELECT n_live_tup, n_dead_tup, last_autovacuum, autovacuum_count
FROM pg_stat_user_tables WHERE relname = 'compensator_orders';

-- 各状态积压
SELECT status, count(*) FROM compensator_orders GROUP BY status;

-- 长事务会阻止死元组回收
SELECT pid, state, xact_start, backend_xmin, left(query, 80)
FROM pg_stat_activity WHERE xact_start IS NOT NULL ORDER BY xact_start;

-- 按实际吞吐调整参数，组件重启不会覆盖
ALTER TABLE compensator_orders SET (autovacuum_vacuum_threshold = 5000);
```

## 测试

```sh
go test -race ./pkg/compensator

# DSN 指向独立测试库，测试会创建并删除带随机后缀的表。
TEST_POSTGRES_DSN='postgres://user:pass@127.0.0.1:5432/test?sslmode=disable' \
  go test -race ./pkg/compensator -run Integration -count=1
TEST_POSTGRES_DSN='...' go test ./pkg/compensator -run '^$' -bench PostgreSQL -benchmem
```

设置 `TEST_COMPENSATOR_LOG=1` 可输出集成测试中组件的日志。集成测试覆盖：并发建表与表参数、结构不兼容时拒绝启动、两种模式的首次执行、业务回滚、外部事务与 `Dispatch`、崩溃后租约接管、旧令牌无法覆盖、重试耗尽与 `Retry`、按保留期清理、三节点分片均衡与节点下线接管（每个任务恰好成功一次）、索引使用以及删除后空间复用。
