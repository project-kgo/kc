# 本地 Outbox

`outbox` 将业务数据和消息写入同一 SQL 事务，提交后立即尝试回调，失败后通过数据库补偿。支持 PostgreSQL 14+（`pgx`）和 MySQL 8.0+（InnoDB、`mysql`），使用业务现有的 `*sqlx.DB`，不依赖 Redis 或服务发现。

## 消息与 JSON

`Message.Payload` 是 **JSON 字符串**。对象统一使用 `sonic.MarshalString` 编码，回调使用 `sonic.UnmarshalString` 解码；辅助方法 `NewMessage` 和 `Message.Decode` 已封装这两个调用。

```go
type OrderCreated struct {
    OrderID int64  `json:"order_id"`
    Name    string `json:"name"`
}

message, err := outbox.NewMessage("order.created", OrderCreated{
    OrderID: 9223372036854775807,
    Name:    "中文订单",
})
if err != nil {
    return err
}
```

PostgreSQL 使用 `JSONB NOT NULL`，MySQL 使用 `JSON NOT NULL`。已编码的 JSON 可以直接赋值给 `Message{Type: "order.created", Payload: jsonString}`；组件不会把这个文档再次编码为 JSON 字符串。非法文档由数据库拒绝，业务必须检查入队错误。JSON `null` 是有效载荷，空字符串不是。

首次处理和重试都会从数据库读取消息。`jsonb` 会规范化 JSON 格式，业务不能依赖空格、键顺序或原始文本签名。直接解码到业务结构体可以保留 `int64` 精度，避免先转为 `map[string]any`。

## 初始化与独立发布

```go
box, err := outbox.New(appCtx, db, outbox.Config{
    TableSuffix: "orders",
    WorkerCount: 8,
    ScanInterval: time.Second,
}, map[string]outbox.Handler{
    "order.created": func(ctx context.Context, message *outbox.Message) error {
        var event OrderCreated
        if err := message.Decode(&event); err != nil {
            return err
        }
        // 业务按消息 ID 幂等执行；外部请求也应传递这个稳定 ID。
        return notifyOrder(ctx, message.ID, event)
    },
})
if err != nil {
    return err
}
defer box.Close()

result, err := box.Publish(ctx, message)
if err != nil {
    // 持久化失败，或提交结果不确定。
    return err
}
if result.AttemptError != nil {
    // 消息已经提交，后台负责补偿，不要因此重新执行业务事务。
    logger.Warn("首次投递失败", "id", result.ID, "error", result.AttemptError)
}
```

`appCtx` 是组件生命周期上下文，不能使用初始化后立即取消的短请求上下文。`NewFromResource` 另外接收已在 `resource` 中注册的 SQL 资源名。`Close` 不会关闭传入的连接池。

初始化复制 handler 映射；之后修改原 map 不影响组件。处理同一后缀的节点应注册相同的消息类型，并保持重试和租约配置一致。

## 业务事务

推荐由组件托管事务：

```go
results, err := box.Transaction(ctx, func(tx *outbox.Tx) error {
    // DAO 也可通过 tx.SQLTx() 复用此事务。
    _, err := tx.SQLTx().ExecContext(ctx, db.Rebind(
        "INSERT INTO orders (id, name) VALUES (?, ?)"), order.ID, order.Name)
    if err != nil {
        return err
    }
    _, err = tx.Enqueue(ctx, message)
    return err
})
if err != nil {
    return err
}
for _, result := range results {
    if result.AttemptError != nil {
        logger.Warn("已提交，等待补偿", "id", result.ID, "error", result.AttemptError)
    }
}
```

业务返回错误或 panic 时回滚；panic 原样向上传播。任何一次 `Tx.Enqueue` 失败都会阻止托管事务提交，即使业务忽略了该入队错误。`Tx` 只在回调期间有效，业务不得手动提交或回滚 `SQLTx()`。

已有事务也可以接入：

```go
var messageID int64
err := sqlxhelper.Transaction(ctx, db, func(tx *sqlx.Tx) error {
    if err := saveOrder(ctx, tx, order); err != nil {
        return err
    }
    var err error
    messageID, err = box.EnqueueTx(ctx, tx, message)
    return err
})
if err != nil {
    return err
}
// 必须在提交成功后触发。遗漏这一步时，后台仍会补偿。
results, err := box.Dispatch(ctx, messageID)
```

外部事务必须属于组件使用的同一数据库，组件只能检查驱动类型，无法验证连接池指向是否一致。业务必须处理 `EnqueueTx` 错误。`Dispatch` 不会重新处理成功记录、未到重试时间的记录或已被其他 worker 领取的记录。

`Publish` 和 `Transaction` 的 `error` 只报告提交前错误与提交错误；提交后的取消、领取失败、回调失败和结果落库失败由每条 `Result.AttemptError` 返回。`Dispatch` 的参数/生命周期错误通过方法的 `error` 返回，各消息的处理或查询错误通过对应 `Result` 返回。查询中的状态是观察时的快照，另一个节点可能随后继续推进状态。

## 配置与运行

| 配置 | 默认值 | 含义 |
|---|---|---|
| `SchemaName` | 当前 schema / 数据库 | 必须预先存在 |
| `TableSuffix` | 空 | 只能包含 ASCII 字母、数字、下划线，最长 49 字符 |
| `DisableBackground` | `false` | 关闭常驻 goroutine，改用 `ScanOnce` 驱动 |
| `WorkerCount` | 4 | 单实例全部回调的并发上限，含前台调用 |
| `ScanInterval` | 1 秒 | 空闲扫描间隔；处理完成会主动唤醒 |
| `BatchSize` | 100 | 单轮处理上限，领取不超过空闲 worker 数 |
| `HandlerTimeout` | 30 秒 | 回调 context 超时，要求业务响应取消 |
| `TaskLease` | 60 秒 | 单条消息被故障接管前的处理期限 |
| `HeartbeatInterval` | 5 秒 | 节点注册、续租、重新均衡间隔 |
| `ShardLease` | 30 秒 | 节点及逻辑分片租约 |
| `MaxRetries` | `nil` → 20 | 不含首次投递；`&0` 表示不重试 |
| `RetryMin` / `RetryMax` | 1 秒 / 5 分钟 | 指数退避基数和上限，叠加 50%～100% 随机抖动 |
| `Retention` | 24 小时 | 成功记录从完成时间起的保留期限 |
| `CleanupInterval` | 1 分钟 | 清理检查间隔 |
| `CleanupBatchSize` | 1,000 | 每个分片每次最多删除的成功记录数 |
| `OperationTimeout` | 5 秒 | 单次调度、结果落库等数据库操作超时 |
| `CloseTimeout` | 35 秒 | 每次 `Close` 等待退出的时限 |
| `Postgres` | 见下一节 | 仅创建新消息表时采用 |
| `Logger` | `slog.Default()` | 结构化错误日志，不记录完整载荷 |

除 `MaxRetries` 和完整的 `PostgresOptions` 外，数量和时间为 0 表示采用默认值；负值无效。`TaskLease` 必须长于 `HandlerTimeout`，并至少留出一个 `OperationTimeout` 的结果写入时间；`ShardLease` 至少为三个心跳间隔。数据库时间字段使用 Unix 毫秒，配置时长不得小于 1 毫秒。

后台模式启动 `WorkerCount` 个回调 worker、一个扫描循环和一个协调循环。前台批量处理、手动扫描可创建临时 goroutine，但与后台共享同一个并发上限。关闭后台时不会注册空闲节点；`ScanOnce` 会注册/续租，执行一轮补偿并按需清理。需要持续处理时应周期调用，或启用后台模式。

`Close` 取消回调，等待已有操作完成结果落库，再释放节点租约；重复调用安全。业务不响应取消时可能返回关闭超时，组件仍会继续等待退出。不要在同一组件自己的回调里同步等待 `Close`。

默认最多执行 **首次 + 20 次重试，共 21 次**。进程在领取后崩溃也会消耗一次投递预算，防止毒消息造成无限崩溃循环。耗尽后保留 `StatusFailed`，调用 `Retry(ctx, id)` 重置预算，消息 ID 不变。失败、待处理和处理中记录均不自动删除。

## 多节点与索引

后缀 `orders` 创建三张组件私有表：

- `outbox_orders`：统一的消息定义，随机、均匀分配到固定 64 个逻辑分片。
- `outbox_nodes_orders`：活跃节点租约。
- `outbox_shards_orders`：分片所有权与代际令牌。

空后缀使用 `outbox`、`outbox_nodes`、`outbox_shards`。不同后缀完全独立。并发初始化通过数据库中的短期 DDL 锁串行化，启动时校验字段、主键、必要索引以及 MySQL InnoDB 引擎，不会自动迁移不兼容的表。

活跃节点在 Go 中按 ID 排序，轮转分配分片。节点数不超过 64 时，稳定状态下分片数量相差不超过 1；超过 64 个消费实例时部分实例没有分片。扩缩容期间允许暂时分配不均；旧节点主动释放不再负责的分片，失联时等待租约过期后接管。消息不需要搬迁。

领取和清理在短事务中验证分片所有权，使用 `FOR UPDATE SKIP LOCKED`。回调在事务外执行，消息结果更新须匹配任务令牌且租约仍有效，旧执行者不能覆盖新执行者的状态。前台按 ID 直接领取，不受分片归属限制，但遵守同样的消息租约。

扫描先用 `EXISTS` 和索引探测有到期任务的分片，避免空轮询对 64 个分片逐个开启事务，也避免扫描整个到期积压。PostgreSQL 使用按状态过滤的部分 B-tree 索引，MySQL 使用对应联合索引；不在 JSON 载荷上建立 GIN 索引。每次领取只读取有空闲执行容量的消息，避免消息在本地队列中等待到租约过期。

组件提供**至少一次**处理，不保证顺序。外部操作成功后进程崩溃或确认失败，仍可能重试。业务须用消息 ID 实现幂等，回调中发出的外部请求也应继承 context 超时。`Publish` 重复调用会创建新消息，并不是生产端去重接口。

## PostgreSQL 空间复用与运维

消息表是普通表，逻辑分片不是物理分区。新表默认参数：

```sql
WITH (
  fillfactor = 90,
  autovacuum_enabled = true,
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_threshold = 1000,
  autovacuum_analyze_scale_factor = 0.02,
  autovacuum_analyze_threshold = 1000,
  vacuum_truncate = false,
  toast.autovacuum_enabled = true,
  toast.autovacuum_vacuum_scale_factor = 0.01,
  toast.autovacuum_vacuum_threshold = 1000,
  toast.vacuum_truncate = false
)
```

需要自定义时从 `outbox.DefaultPostgresOptions()` 获取完整默认值再修改，传给 `Config.Postgres`。现有表的参数不会在重启时被覆盖。

清理仅分批删除超过保留期限的成功记录。普通 autovacuum 回收死元组后，后续插入可复用 heap 和索引中的空闲空间；`vacuum_truncate=false` 保留尾部空页供后续写入使用，避免截断文件所需的排他锁。组件不执行 `VACUUM FULL`，不以文件立即缩小为目标。数据库必须全局启用 autovacuum，并提供足够的 vacuum worker 和 I/O 预算。

```sql
-- 检查消息表和 TOAST 表参数。
SELECT c.oid::regclass, c.reloptions, t.reloptions AS toast_options
FROM pg_class c LEFT JOIN pg_class t ON t.oid = c.reltoastrelid
WHERE c.oid = 'public.outbox_orders'::regclass;

-- 观察死元组、自动清理时间和磁盘占用。
SELECT relname, n_live_tup, n_dead_tup, last_autovacuum, last_autoanalyze
FROM pg_stat_user_tables WHERE relid = 'public.outbox_orders'::regclass;
SELECT pg_size_pretty(pg_relation_size('public.outbox_orders')) AS heap,
       pg_size_pretty(pg_total_relation_size('public.outbox_orders')) AS total;

-- 已有表按实际吞吐调整参数；不会被组件启动逻辑重置。
ALTER TABLE public.outbox_orders SET (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_threshold = 1000,
  toast.autovacuum_vacuum_scale_factor = 0.01,
  toast.autovacuum_vacuum_threshold = 1000,
  vacuum_truncate = false,
  toast.vacuum_truncate = false
);

-- 长事务会阻止死元组回收，需要结合业务排查。
SELECT pid, state, xact_start, backend_xmin, wait_event_type, wait_event
FROM pg_stat_activity WHERE xact_start IS NOT NULL ORDER BY xact_start;
```

## 测试与基准

```sh
go test -race ./pkg/outbox ./pkg/sqlxhelper

# DSN 指向独立测试库，测试会创建并删除带唯一后缀的表。
TEST_POSTGRES_DSN='postgres://user:pass@127.0.0.1:5432/test?sslmode=disable' \
  go test -race ./pkg/outbox -run TestPostgreSQLIntegration -count=1
TEST_MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/test?parseTime=true' \
  go test -race ./pkg/outbox -run TestMySQLIntegration -count=1

go test ./pkg/outbox -run '^$' -bench BenchmarkMessageJSON -benchmem
# 设置对应 DSN 后运行单节点/四节点数据库吞吐基准。
go test ./pkg/outbox -run '^$' -bench BenchmarkSQLThroughput -benchtime=2s -benchmem
```

未设置对应 DSN 时明确跳过数据库测试。集成测试覆盖业务提交边界、JSON 类型、重试预算、任务令牌、分片交接、并发初始化、后台与前台竞争、清理边界；PostgreSQL 还检查表/TOAST 参数、索引计划和普通 vacuum 后的空间复用。
