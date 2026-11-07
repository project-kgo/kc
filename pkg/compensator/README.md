# 本地任务补偿 compensator

`compensator` 让业务在自己的 PostgreSQL 事务里登记任务，任务与业务数据一起提交或回滚。提交后由所有节点的后台 worker 共同领取执行，失败时按指数退避重试，直到成功或耗尽重试次数。只依赖业务现有的 `*sqlx.DB`（驱动 `pgx`，PostgreSQL 14+），不依赖 Redis 或服务发现。

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
err = comp.Transaction(ctx, func(tx *compensator.Tx) error {
    if _, err := tx.SQLTx().ExecContext(ctx, "UPDATE orders SET status = 'paid' WHERE id = $1", 42); err != nil {
        return err
    }
    _, err := tx.Add(ctx, task) // 返回任务 ID，可用 comp.Get 查询执行状态
    return err
})
if err != nil {
    return err // 业务失败、登记失败或提交失败，没有任何任务生效
}
```

`Payload` 是 sonic 编码的 JSON 字符串：`NewTask` 用 `sonic.MarshalString` 编码，`Task.Decode` 用 `sonic.UnmarshalString` 解码。已编码的字符串可直接赋值 `Task{Type: "...", Payload: s}`。库中以 `TEXT` 原样保存，不做 JSON 校验或规范化。

### 指定执行时间

登记时用 `Task.RunAt` 指定首次执行时间。零值表示提交后立即可执行，过去的时间同样按立即可执行处理：

```go
task, err := compensator.NewTask("order.remind", OrderPaid{OrderID: 42})
if err != nil {
    return err
}
task.RunAt = time.Now().Add(24 * time.Hour)

err = comp.Transaction(ctx, func(tx *compensator.Tx) error {
    _, err := tx.Add(ctx, task)
    return err
})
```

- 只影响首次执行；失败后的重试仍按 `RetryBackoffMin` / `RetryBackoffMax` 退避，`Retry` 也仍然立即重新排期。
- 是否到期由数据库时钟判断（`next_run_at <= now()`），但 `RunAt` 是调用方给出的绝对时间：用本机 `time.Now()` 计算时，节点时钟偏差会直接反映到实际触发时刻。
- 实际触发时间不会早于 `RunAt`，延迟最多一个 `ScanInterval` 加一次领取往返，不保证毫秒级准时。到期后由任一节点领取，与普通任务没有区别。
- 远期任务会一直保留在待执行索引（`status < 2`）中；领取仍按 `next_run_at` 顺序只取到期行，不影响领取效率，只占用少量索引空间。
- 回调收到的 `Task` 中 `RunAt` 为零值；`Get` 返回的 `Record.NextRunAt` 才是库中的实际时间。

### 指定重试总时长

`Task.MaxRetryDuration` 按任务限制从首次执行开始的重试总时长，零值或不设置表示不限制。它是任务自身的属性，与组件级的 `MaxRetries` 并存，先到者结束任务：

```go
task, err := compensator.NewTask("order.remind", OrderPaid{OrderID: 42})
if err != nil {
    return err
}
task.MaxRetryDuration = 30 * time.Minute

err = comp.Transaction(ctx, func(tx *compensator.Tx) error {
    _, err := tx.Add(ctx, task)
    return err
})
```

- 从该任务**首次被领取执行**开始计时；排队、积压和 `RunAt` 等待不计入
- 到期后不再发起新执行：失败后的退避不会排到期限之后，到期时任务在下一次领取中直接置为 `failed`，`last_error` 为 `retry duration exceeded`
- 只影响该任务，不同任务的预算互不共享；正在执行中的回调不会被中断，它成功则仍记为成功
- `Retry(ctx, id)` 会重置该任务的执行次数和计时起点，`MaxRetryDuration` 的设置本身保留
- 时长按毫秒保存，非零值至少 1ms；负数或亚毫秒值在登记时返回 `ErrInvalidArgument`

### 单独注册回调

`New` 的 `handlers` 可以为空，回调也可以在组件运行期间注册，适合各业务模块各自注册回调的场景：

```go
comp, err := compensator.New(ctx, db, compensator.Config{TableSuffix: "orders"}, nil)
// ...
if err := comp.Register("order.paid", handleOrderPaid); err != nil {
    return err
}
// 或原子地注册一组：任一类型无效或已注册时全部不生效。
err = comp.RegisterAll(map[string]compensator.Handler{"order.refund": handleRefund})
```

注册后本节点立即开始领取该类型的任务，也可以登记该类型的新任务；未注册的类型 `Add` / `AddTx` 返回 `ErrNoHandler`。同一类型只能注册一次，重复注册、类型无效或回调为 nil 返回 `ErrInvalidArgument`，关闭后返回 `ErrClosed`。不支持注销。

## 执行流程

1. **登记**：`Transaction` / `AddTx` 在业务事务内把任务写为 `pending`，随业务一起提交或回滚。设置 `Task.RunAt` 可指定首次可领取时间，零值或过去时间表示提交后立即可领取。回调不会在事务内执行。
2. **领取**：各节点的扫描循环用一条短语句领取到期任务：`FOR UPDATE SKIP LOCKED` 选出最早到期的行，改为 `running`，写入随机令牌，并把 `next_run_at` 设为租约到期时间（`LeaseDuration`）。语句提交后行锁即释放，回调在事务外执行。只领取空闲 worker 数量、且本节点注册了回调的类型的任务，领取后立即开始执行，不在本地排队。
3. **落库**：回调结束后按令牌写回结果：成功为 `succeeded`；失败且有剩余次数为 `pending` 并按退避推迟 `next_run_at`；次数耗尽为 `failed`。令牌不匹配（租约已被接管）时旧结果不会覆盖新状态。

`Transaction` 提交后会唤醒本节点的扫描：本节点有空闲 worker 时任务立即被领取，延迟只多一次领取的往返；否则由任一节点在 `ScanInterval` 内领取。`AddTx` 由调用方提交，不唤醒扫描。

`running` 的任务在租约到期前不会被其它节点领取。领取语句不随 `Close` 取消，避免语句已在服务端提交、结果却被丢弃，导致任务空等租约到期并白耗一次执行次数。回调受 `HandlerTimeout` 约束且 `LeaseDuration > HandlerTimeout`，正常执行总会在租约到期前落库；只有执行者崩溃或与数据库失联时，任务才会在租约到期后被其它节点接管。`Close` 会取消执行中的回调，并立即把它们归还为 `pending`，不消耗执行次数。

### 接入已有事务

```go
tx, _ := db.BeginTxx(ctx, nil)
// ... 业务写入
id, err := comp.AddTx(ctx, tx, task)
if err != nil {
    return err
}
if err := tx.Commit(); err != nil {
    return err
}
// 提交后任务由后台在 ScanInterval 内领取执行；回滚则任务不存在。
```

## 配置

| 字段 | 默认值 | 说明 |
|---|---|---|
| `TableSuffix` | 必填 | `[a-z0-9_]{1,40}` |
| `Schema` | `current_schema()` | 必须已存在 |
| `WorkerCount` | 8 | 每个节点同时执行回调的 worker 数，决定执行并发上限（`WorkerCount × 节点数`） |
| `ScanInterval` | 1s | 空闲扫描间隔；领取满批后立即继续 |
| `BatchSize` | 2×WorkerCount | 单次领取上限，实际不超过空闲 worker 数 |
| `MaxRetries` | 10 | 不含首次；`-1` 表示不重试 |
| `RetryBackoffMin` / `RetryBackoffMax` | 1s / 5min | 指数退避，叠加 50%～100% 随机抖动 |
| `HandlerTimeout` | 30s | 回调 context 超时；回调忽略超时返回 nil 也按失败处理 |
| `LeaseDuration` | HandlerTimeout+10s | running 任务被其它节点接管前的期限，必须大于 `HandlerTimeout`；也是执行者崩溃后任务重新可领取的等待时间 |
| `CleanupAfter` | 5min | 成功记录保留时长 |
| `CleanupInterval` / `CleanupBatchSize` | 30s / 1000 | 清理间隔与每批删除行数（每轮最多 100 批） |
| `FailedRetention` | 0 | >0 时清理失败超过该时长的记录；默认保留，便于排查和 `Retry` |
| `OperationTimeout` | 5s | 后台单次数据库操作超时 |
| `DisableWorker` | false | 只登记任务，不执行和清理（如只写的批处理进程） |
| `Logger` | `slog.Default()` | 不记录载荷 |

最多执行 `1 + MaxRetries` 次。进程在领取后崩溃同样消耗一次预算，防止毒任务导致无限崩溃循环。耗尽后状态为 `failed`，可调用 `Retry(ctx, id)` 重置预算（ID 不变）。

`MaxRetries` 是组件级配置；单个任务还可以用 `Task.MaxRetryDuration` 限制自己的重试总时长，见[指定重试总时长](#指定重试总时长)。

## 监控

`Config.Metrics` 接收运行事件，组件本身不依赖具体监控库：

```go
type Metrics interface {
    Executed(ExecEvent)                // 每次回调执行后：类型、第几次、落库状态、耗时、错误
    Cleaned(status Status, rows int64) // 每批清理后
}
```

方法在执行路径上同步调用，实现必须并发安全且不阻塞（如 Prometheus counter/histogram）。

积压用 `Stats(ctx)` 低频采集（如每 30 秒），返回全表 pending/running/failed 数量和最早到期时间，适合做 gauge 并配置告警：`failed > 0`、`now - OldestDue` 持续增大（补偿跟不上）。多节点部署时只需一处采集。

## 表结构

后缀 `orders` 创建任务表 `compensator_orders`，主键为应用生成的 UUIDv7（登记时就有稳定 ID，时间有序利于 B-tree 追加）：

- `next_run_at`：下次可领取时间；`first_run_at` 在首次领取时写入，是该任务重试时长的计时起点，未执行过时为 `NULL`。
- `max_retry_duration_ms`：该任务的重试总时长（毫秒），0 表示不限制；与组件配置无关。
- `compensator_orders_due_idx (next_run_at) WHERE status < 2`：只包含待执行/执行中的任务，执行完成即移出，领取扫描始终很小。
- `compensator_orders_done_idx (status, completed_at) WHERE status >= 2`：清理使用。

多个节点并发启动时，建表在 `pg_advisory_xact_lock` 下串行执行，并校验列类型和索引。组件不做自动迁移，也不会覆盖已有表的存储参数。

### 从旧版本升级

带按任务重试时长的版本要求表中有 `first_run_at` 和 `max_retry_duration_ms` 两列，旧表必须在部署新版本**之前**完成迁移，否则组件启动时会因列缺失返回 `ErrInvalidConfig`。下面的语句只做元数据变更（可空列、常量默认值），PostgreSQL 不重写表，属于在线 DDL：

```sql
BEGIN;
SET LOCAL lock_timeout = '3s';
ALTER TABLE "public"."compensator_orders"
  ADD COLUMN IF NOT EXISTS first_run_at timestamptz,
  ADD COLUMN IF NOT EXISTS max_retry_duration_ms bigint NOT NULL DEFAULT 0;
COMMIT;
```

表名规则是 `compensator_<TableSuffix>`，可用下面的查询为所有任务表生成同样的语句（按需替换 schema）：

```sql
SELECT format(
  'ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS first_run_at timestamptz, ADD COLUMN IF NOT EXISTS max_retry_duration_ms bigint NOT NULL DEFAULT 0;',
  schemaname, tablename)
FROM pg_tables
WHERE schemaname = 'public' AND tablename LIKE 'compensator\_%';
```

`ALTER TABLE` 需要短暂的 `ACCESS EXCLUSIVE` 锁；`lock_timeout` 让它在拿不到锁时快速失败，失败后重试即可，避免阻塞业务。旧版本代码不读写这两列，所以先执行 DDL、再滚动发布是兼容的；滚动期间旧节点不会写 `first_run_at`，这些任务会在新节点首次领取时开始计时，旧节点登记的任务 `max_retry_duration_ms` 取默认值 0（不限制）。升级前已经存在的 pending/running 行 `first_run_at` 为 `NULL`，同样会在下一次领取时开始计时；新创建的表默认就包含这两列。

## 多节点

所有节点从同一张表领取任务，不需要分片、节点注册或选主。并发领取由 `FOR UPDATE SKIP LOCKED` 保证同一行只被一个节点拿到，各节点互不阻塞；节点上下线不需要任何协调，新节点下一次扫描就开始消费。清理同样由所有节点并发执行，候选行加 `FOR UPDATE SKIP LOCKED`，彼此删除不同的行。

重复执行的防护：

| 情形 | 防护 |
|---|---|
| 多个节点同时领取 | 行锁 + 随机令牌，同一时刻只有一个持有者 |
| 执行中租约过期 | 只按空闲 worker 数领取、不在本地排队；`LeaseDuration > HandlerTimeout`，超时取消回调，回调超时后才返回成功也按失败处理 |
| 旧执行者晚到的结果 | 落库校验令牌，不匹配则丢弃 |
| 滚动发布新增任务类型 | 节点只领取自己注册了回调的类型，旧节点不会领取新类型后因缺少回调而失败 |
| 回调成功后、结果落库前崩溃 | 无法避免，任务会再次执行 |

所有时间判断使用数据库时钟，节点时钟漂移不影响租约；登记时指定的 `RunAt` 是调用方给出的绝对时间，见[指定执行时间](#指定执行时间)。

组件提供**至少一次**执行，不保证顺序：回调必须幂等，建议以 `task.ID` 作为下游幂等键。

## 空间回收与运维

成功记录在 `CleanupAfter` 后由各节点按批 `DELETE`。任务表创建时带有以下参数，使删除产生的死元组被及时回收，堆页与索引页的空闲空间被后续写入复用，稳定负载下表大小保持平稳：

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

### 参数评估

现有参数更适合“表里只有未完成任务、稳态高频更新/删除”的场景：`fillfactor=90` 给每次领取/落库的 UPDATE 留出页面空间（这些 UPDATE 会改 `next_run_at`/`status`，无法走 HOT，但低阈值 vacuum 把膨胀限制在数千个元组内）；`due_idx`/`done_idx` 是部分索引，扫描和清理只碰相关行。稳态下不需要调整。

使用 `RunAt` 调度大量远期任务后，表里可能长期存在远超“近期到期量”的行，有两个规模效应：

- `due_idx` 和 `Stats` 的代价随未完成任务数增长（远期任务也计入 `pending`）；领取本身仍走索引顺序，只取到期行。
- 绝对阈值 2000 / 10000 让 vacuum 触发频率与表大小无关。表很大而写入速率中等时，每次全表扫描的 vacuum 相对 2000 个死元组来说过于频繁；批量写入远期任务时也会反复触发 insert vacuum。

如果任务表会长期保留百万级未完成任务，先把触发阈值改成“绝对 + 比例”的混合形式，再按实际观测微调：

```sql
ALTER TABLE compensator_orders SET (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_threshold = 5000,
  autovacuum_vacuum_insert_scale_factor = 0.01,
  autovacuum_vacuum_insert_threshold = 50000
);
```

- 表小：仍在约 5000 个死元组触发，保持原有及时性。
- 表大：触发点随行数增长（如 1000 万行约 10 万死元组），避免 vacuum 持续空转。
- `autovacuum_vacuum_cost_delay` 保持 1ms；若 vacuum 仍追不上删除速度，再提高 `autovacuum_vacuum_cost_limit`（如 1000）并观察 I/O。
- `fillfactor` 只在 UPDATE 导致的堆膨胀明显时才降到 85；它只对新页生效，旧表需要 `pg_repack` 才有效果。

如果远期任务量级远大于近期到期量（例如百万级、调度跨度数月），说明纯 PostgreSQL 轮询不是最优形态，应评估独立的延迟调度层，而不是继续调 autovacuum。

## 测试

```sh
go test -race ./pkg/compensator

# DSN 指向独立测试库，测试会创建并删除带随机后缀的表。
TEST_POSTGRES_DSN='postgres://user:pass@127.0.0.1:5432/test?sslmode=disable' \
  go test -race ./pkg/compensator -run Integration -count=1
TEST_POSTGRES_DSN='...' go test ./pkg/compensator -run '^$' -bench PostgreSQL -benchmem
```

设置 `TEST_COMPENSATOR_LOG=1` 可输出集成测试中组件的日志。集成测试覆盖：并发建表与表参数、结构不兼容时拒绝启动、回调能看到已提交的业务数据、业务回滚、外部事务、指定执行时间（到期前不领取、过去时间立即执行）、慢回调下不因本地排队丢失租约、只领取已注册的类型、崩溃后租约接管、旧令牌无法覆盖、重试耗尽与 `Retry`、按任务设置的重试总时长（互不影响）与 `Retry` 重置、多节点并发清理、清理跳过重试中的任务、三节点并发消费与节点下线（每个任务恰好成功一次）、索引使用以及删除后空间复用。

`BenchmarkPostgreSQLEndToEnd` 报告从登记到回调完成的 p50/p99 延迟、调用方 `Transaction` 耗时和吞吐，建议 `-benchtime=2000x`；`BenchmarkPostgreSQLBackground` 建议 `-benchtime=1x`。
