package compensator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

func openTestDB(t testing.TB) *sqlx.DB {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("跳过 PostgreSQL 集成测试：未设置 TEST_POSTGRES_DSN")
	}
	db, err := sqlx.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(32)
	// database/sql 默认只保留 2 个空闲连接，并发压测时会反复建连；与上限保持一致。
	db.SetMaxIdleConns(32)
	t.Cleanup(func() { db.Close() })
	return db
}

// fastConfig 使用较短的时间参数，使租约、重试和清理在测试中快速发生；
// 同时为远程测试库的网络延迟留出余量。
func fastConfig(suffix string) Config {
	var logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("TEST_COMPENSATOR_LOG") != "" {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return Config{
		TableSuffix: suffix, WorkerCount: 4, ScanInterval: 50 * time.Millisecond,
		HandlerTimeout: 2 * time.Second, LeaseDuration: 4 * time.Second,
		RetryBackoffMin: 20 * time.Millisecond, RetryBackoffMax: 50 * time.Millisecond,
		OperationTimeout: 15 * time.Second, CleanupInterval: time.Hour, Logger: logger,
	}
}

func uniqueSuffix(t testing.TB, db *sqlx.DB) string {
	t.Helper()
	suffix := "t" + strings.ReplaceAll(uuid.NewString()[:13], "-", "")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS compensator_%s", suffix))
	})
	return suffix
}

func mustNew(t testing.TB, db *sqlx.DB, cfg Config, handlers map[string]Handler) *Compensator {
	t.Helper()
	c, err := New(context.Background(), db, cfg, handlers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// addTasks 在一个事务中登记 n 个相同任务并返回它们的 ID。
func addTasks(t testing.TB, c *Compensator, task Task, n int) []uuid.UUID {
	t.Helper()
	ctx := context.Background()
	ids := make([]uuid.UUID, 0, n)
	if err := c.Transaction(ctx, func(tx *Tx) error {
		for range n {
			id, err := tx.Add(ctx, task)
			if err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return ids
}

// executionCounter 统计每个任务被回调执行的次数，用于验证恰好一次。
type executionCounter struct{ m sync.Map }

func (e *executionCounter) inc(id uuid.UUID) {
	counter, _ := e.m.LoadOrStore(id, new(atomic.Int64))
	counter.(*atomic.Int64).Add(1)
}

func (e *executionCounter) assertOnce(t *testing.T, ids []uuid.UUID) {
	t.Helper()
	for _, id := range ids {
		counter, ok := e.m.Load(id)
		if n := int64(0); !ok || counter.(*atomic.Int64).Load() != 1 {
			if ok {
				n = counter.(*atomic.Int64).Load()
			}
			t.Errorf("task %v executed %d times, want 1", id, n)
		}
	}
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	waitFor(t, 90*time.Second, cond)
}

func statusOf(t *testing.T, c *Compensator, id uuid.UUID) Status {
	t.Helper()
	r, err := c.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return r.Status
}

func countRows(t testing.TB, db *sqlx.DB, suffix, where string) int {
	t.Helper()
	var n int
	if err := db.Get(&n, fmt.Sprintf("SELECT count(*) FROM compensator_%s WHERE %s", suffix, where)); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPostgreSQLIntegrationSchema(t *testing.T) {
	db := openTestDB(t)
	suffix := uniqueSuffix(t, db)
	cfg := fastConfig(suffix)
	cfg.DisableWorker = true
	handlers := map[string]Handler{"t": func(context.Context, *Task) error { return nil }}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Go(func() {
			c, err := New(context.Background(), db, cfg, handlers)
			if err == nil {
				err = c.Close()
			}
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent initialization: %v", err)
		}
	}
	var options string
	if err := db.Get(&options, "SELECT array_to_string(reloptions, ',') FROM pg_class WHERE relname = $1", "compensator_"+suffix); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"autovacuum_vacuum_scale_factor=0", "autovacuum_vacuum_threshold=2000", "fillfactor=90"} {
		if !strings.Contains(options, want) {
			t.Fatalf("reloptions %q missing %q", options, want)
		}
	}
	// 已有表结构不兼容时拒绝启动。
	if _, err := db.Exec(fmt.Sprintf("ALTER TABLE compensator_%s ALTER COLUMN payload TYPE varchar(10)", suffix)); err != nil {
		t.Fatal(err)
	}
	if _, err := New(context.Background(), db, cfg, handlers); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("incompatible column should be rejected: %v", err)
	}
}

func TestPostgreSQLIntegrationTransaction(t *testing.T) {
	db := openTestDB(t)
	suffix := uniqueSuffix(t, db)
	ctx := context.Background()
	if _, err := db.Exec(fmt.Sprintf("CREATE TABLE biz_%s (id uuid PRIMARY KEY)", suffix)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS biz_%s", suffix)) })
	var executions executionCounter
	var failOnce sync.Map
	c := mustNew(t, db, fastConfig(suffix), map[string]Handler{
		// 回调在提交之后执行，必须能从其它连接看到业务数据。
		"biz": func(ctx context.Context, task *Task) error {
			var id uuid.UUID
			if err := task.Decode(&id); err != nil {
				return err
			}
			var n int
			if err := db.GetContext(ctx, &n, fmt.Sprintf("SELECT count(*) FROM biz_%s WHERE id = $1", suffix), id); err != nil {
				return err
			}
			if n != 1 {
				return errors.New("business row not visible")
			}
			executions.inc(task.ID)
			return nil
		},
		"flaky": func(_ context.Context, task *Task) error {
			if _, loaded := failOnce.LoadOrStore(task.ID, true); !loaded {
				return errors.New("first attempt fails")
			}
			executions.inc(task.ID)
			return nil
		},
	})

	bizID := uuid.New()
	bizTask, _ := NewTask("biz", bizID)
	flakyTask, _ := NewTask("flaky", map[string]int64{"n": 9223372036854775807})
	var bizTaskID, flakyID uuid.UUID
	if err := c.Transaction(ctx, func(tx *Tx) error {
		if _, err := tx.SQLTx().ExecContext(ctx, fmt.Sprintf("INSERT INTO biz_%s VALUES ($1)", suffix), bizID); err != nil {
			return err
		}
		var err error
		if bizTaskID, err = tx.Add(ctx, bizTask); err != nil {
			return err
		}
		flakyID, err = tx.Add(ctx, flakyTask)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		return statusOf(t, c, bizTaskID) == StatusSucceeded && statusOf(t, c, flakyID) == StatusSucceeded
	})
	record, err := c.Get(ctx, bizTaskID)
	if err != nil || record.Attempts != 1 || record.CompletedAt.IsZero() {
		t.Fatalf("succeeded record: %+v, %v", record, err)
	}
	if r, _ := c.Get(ctx, flakyID); r.Attempts != 2 || r.Payload != `{"n":9223372036854775807}` {
		t.Fatalf("flaky record: %+v", r)
	}

	// 业务失败或登记失败时整个事务回滚，不留下任务。
	var rolledBack uuid.UUID
	if err = c.Transaction(ctx, func(tx *Tx) error {
		rolledBack, _ = tx.Add(ctx, flakyTask)
		return errors.New("business failed")
	}); err == nil || err.Error() != "business failed" {
		t.Fatalf("business error should be returned: %v", err)
	}
	if _, err = c.Get(ctx, rolledBack); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rolled back task should not exist: %v", err)
	}
	if err = c.Transaction(ctx, func(tx *Tx) error {
		_, _ = tx.Add(ctx, Task{Type: "unknown"})
		return nil // 忽略 Add 错误也不能提交
	}); !errors.Is(err, ErrNoHandler) {
		t.Fatalf("unregistered type should abort the transaction: %v", err)
	}

	// 外部事务：提交后由后台执行；回滚则不留下任务。
	bizID2 := uuid.New()
	bizTask2, _ := NewTask("biz", bizID2)
	tx := db.MustBeginTx(ctx, nil)
	tx.MustExecContext(ctx, fmt.Sprintf("INSERT INTO biz_%s VALUES ($1)", suffix), bizID2)
	externalID, err := c.AddTx(ctx, tx, bizTask2)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return statusOf(t, c, externalID) == StatusSucceeded })
	tx = db.MustBeginTx(ctx, nil)
	abortedID, err := c.AddTx(ctx, tx, bizTask2)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Get(ctx, abortedID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rolled back AddTx task should not exist: %v", err)
	}
	executions.assertOnce(t, []uuid.UUID{bizTaskID, flakyID, externalID})
}

// 回调总耗时远超租约时，每个任务仍只执行一次：worker 只按空闲容量领取，
// 领取后立即执行，任务不会在本地排队到租约过期。
func TestPostgreSQLIntegrationSlowHandlersKeepLease(t *testing.T) {
	db := openTestDB(t)
	suffix := uniqueSuffix(t, db)
	const n = 12
	var executions executionCounter
	cfg := fastConfig(suffix)
	cfg.WorkerCount = 1
	cfg.MaxRetries = -1
	// 12 个 200ms 的回调串行执行约 2.4 秒，远超 1.5 秒租约。
	cfg.HandlerTimeout = time.Second
	cfg.LeaseDuration = 1500 * time.Millisecond
	handler := func(_ context.Context, task *Task) error {
		executions.inc(task.ID)
		time.Sleep(200 * time.Millisecond)
		return nil
	}
	// 两个节点同时消费，验证跨节点也不会重复领取。
	c := mustNew(t, db, cfg, map[string]Handler{"t": handler})
	mustNew(t, db, cfg, map[string]Handler{"t": handler})
	task, _ := NewTask("t", 1)
	ids := addTasks(t, c, task, n)
	eventually(t, func() bool { return countRows(t, db, suffix, "status = 2") == n })
	if failed := countRows(t, db, suffix, "status = 3"); failed != 0 {
		t.Fatalf("%d tasks failed", failed)
	}
	executions.assertOnce(t, ids)
}

// 节点只领取自己注册了回调的类型：滚动发布新增类型时，旧节点不会领取后因缺少回调而失败。
func TestPostgreSQLIntegrationClaimsOnlyRegisteredTypes(t *testing.T) {
	db := openTestDB(t)
	suffix := uniqueSuffix(t, db)
	ctx := context.Background()
	cfg := fastConfig(suffix)
	cfg.MaxRetries = -1
	var executions executionCounter
	handler := func(_ context.Context, task *Task) error { executions.inc(task.ID); return nil }
	// 旧节点只认识 old 类型；新版本的生产者登记 new 类型，自身不消费。
	mustNew(t, db, cfg, map[string]Handler{"old": handler})
	producerCfg := cfg
	producerCfg.DisableWorker = true
	producer := mustNew(t, db, producerCfg, map[string]Handler{"old": handler, "new": handler})
	oldTask, _ := NewTask("old", 1)
	newTask, _ := NewTask("new", 1)
	oldID := addTasks(t, producer, oldTask, 1)[0]
	newID := addTasks(t, producer, newTask, 1)[0]
	eventually(t, func() bool { return statusOf(t, producer, oldID) == StatusSucceeded })
	// 旧节点又扫描了多轮，new 类型的任务应保持原样。
	time.Sleep(10 * cfg.ScanInterval)
	if r, err := producer.Get(ctx, newID); err != nil || r.Status != StatusPending || r.Attempts != 0 {
		t.Fatalf("old node must not claim an unregistered type: %+v %v", r, err)
	}
	// 新节点上线后执行。
	mustNew(t, db, cfg, map[string]Handler{"old": handler, "new": handler})
	eventually(t, func() bool { return statusOf(t, producer, newID) == StatusSucceeded })
	if r, _ := producer.Get(ctx, newID); r.Attempts != 1 {
		t.Fatalf("new type attempts = %d", r.Attempts)
	}
	executions.assertOnce(t, []uuid.UUID{oldID, newID})
}

func TestPostgreSQLIntegrationRetryExhausted(t *testing.T) {
	db := openTestDB(t)
	suffix := uniqueSuffix(t, db)
	ctx := context.Background()
	var healthy atomic.Bool
	var attempts atomic.Int64
	cfg := fastConfig(suffix)
	cfg.MaxRetries = 2
	c := mustNew(t, db, cfg, map[string]Handler{"t": func(context.Context, *Task) error {
		attempts.Add(1)
		if healthy.Load() {
			return nil
		}
		return errors.New("downstream unavailable")
	}})
	task, _ := NewTask("t", 1)
	id := addTasks(t, c, task, 1)[0]
	eventually(t, func() bool { return statusOf(t, c, id) == StatusFailed })
	if r, _ := c.Get(ctx, id); r.Attempts != 3 || r.LastError != "downstream unavailable" || attempts.Load() != 3 {
		t.Fatalf("exhausted record: %+v, calls %d", r, attempts.Load())
	}
	if err := c.Retry(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retry missing: %v", err)
	}
	healthy.Store(true)
	if err := c.Retry(ctx, id); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return statusOf(t, c, id) == StatusSucceeded })
	if err := c.Retry(ctx, id); !errors.Is(err, ErrNotFailed) {
		t.Fatalf("retry succeeded task: %v", err)
	}

	// MaxRetries=-1：首次失败直接进入 failed。
	cfg2 := fastConfig(uniqueSuffix(t, db))
	cfg2.MaxRetries = -1
	c2 := mustNew(t, db, cfg2, map[string]Handler{"t": func(context.Context, *Task) error { return errors.New("no") }})
	id = addTasks(t, c2, task, 1)[0]
	eventually(t, func() bool { return statusOf(t, c2, id) == StatusFailed })
	if r, _ := c2.Get(ctx, id); r.Attempts != 1 {
		t.Fatalf("no-retry attempts = %d", r.Attempts)
	}
}

func TestPostgreSQLIntegrationLeaseTakeover(t *testing.T) {
	db := openTestDB(t)
	suffix := uniqueSuffix(t, db)
	ctx := context.Background()
	var calls atomic.Int64
	c := mustNew(t, db, fastConfig(suffix), map[string]Handler{"t": func(context.Context, *Task) error { calls.Add(1); return nil }})
	task, _ := NewTask("t", 1)
	id := addTasks(t, c, task, 1)[0]
	eventually(t, func() bool { return statusOf(t, c, id) == StatusSucceeded })
	// 模拟执行者领取后崩溃：任务停留在 running，租约到期后被重新执行。
	db.MustExec(fmt.Sprintf("UPDATE compensator_%s SET status = 1, lease_token = 7, completed_at = NULL, next_run_at = now() + interval '1 second' WHERE id = $1", suffix), id)
	eventually(t, func() bool { return statusOf(t, c, id) == StatusSucceeded })
	// 旧执行者的结果不能覆盖新状态。
	if err := c.store.finish(ctx, claimed{Task: Task{ID: id}, token: 7}, StatusPending, "late", time.Second); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale token finish: %v", err)
	}
	// 已耗尽预算却停留在 running 的任务被直接标记为 failed。
	db.MustExec(fmt.Sprintf("UPDATE compensator_%s SET status = 1, attempts = 11, lease_token = 7, next_run_at = now() WHERE id = $1", suffix), id)
	eventually(t, func() bool { return statusOf(t, c, id) == StatusFailed })
	if calls.Load() != 2 {
		t.Fatalf("calls = %d", calls.Load())
	}
}

func TestPostgreSQLIntegrationCleanup(t *testing.T) {
	db := openTestDB(t)
	suffix := uniqueSuffix(t, db)
	ctx := context.Background()
	cfg := fastConfig(suffix)
	cfg.CleanupAfter = 10 * time.Second
	cfg.CleanupInterval = 200 * time.Millisecond
	cfg.CleanupBatchSize = 7
	cfg.FailedRetention = 10 * time.Second
	cfg.MaxRetries = -1
	handlers := map[string]Handler{
		"ok":   func(context.Context, *Task) error { return nil },
		"fail": func(context.Context, *Task) error { return errors.New("x") },
	}
	// 两个节点同时清理同一张表。
	c := mustNew(t, db, cfg, handlers)
	mustNew(t, db, cfg, handlers)
	ok, _ := NewTask("ok", 1)
	fail, _ := NewTask("fail", 1)
	if err := c.Transaction(ctx, func(tx *Tx) error {
		for range 50 {
			if _, err := tx.Add(ctx, ok); err != nil {
				return err
			}
		}
		_, err := tx.Add(ctx, fail)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return countRows(t, db, suffix, "status >= 2") == 51 })
	// 未到保留期前不删除（保留期留足余量，避免慢速网络下的时序抖动）。
	if n := countRows(t, db, suffix, "true"); n != 51 {
		t.Fatalf("rows deleted too early: %d", n)
	}
	eventually(t, func() bool { return countRows(t, db, suffix, "true") == 0 })

	// 并发清理各自删除不同的行，合计恰好等于全部过期记录。用独立的表，
	// 避免上面两个节点的后台清理参与计数。
	suffix = uniqueSuffix(t, db)
	cfg = fastConfig(suffix)
	cfg.DisableWorker = true
	c = mustNew(t, db, cfg, handlers)
	db.MustExec(fmt.Sprintf(`INSERT INTO compensator_%s (id, task_type, payload, status, attempts, next_run_at, created_at, updated_at, completed_at)
SELECT gen_random_uuid(), 'ok', '1', 2, 1, now(), now(), now(), now() - interval '1 hour' FROM generate_series(1, 2000)`, suffix))
	var deleted atomic.Int64
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				n, err := c.store.cleanup(ctx, StatusSucceeded, time.Minute, 100)
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
				deleted.Add(n)
			}
		})
	}
	wg.Wait()
	if remaining := countRows(t, db, suffix, "true"); deleted.Load() != 2000 || remaining != 0 {
		t.Fatalf("concurrent cleanup deleted %d, %d remaining", deleted.Load(), remaining)
	}
}

// Retry 已把过期失败任务改回 pending 但尚未提交时，清理须跳过该行，
// 不能在其提交后按旧快照把刚重置的任务删除。
func TestPostgreSQLIntegrationCleanupSkipsRetried(t *testing.T) {
	db := openTestDB(t)
	suffix := uniqueSuffix(t, db)
	ctx := context.Background()
	cfg := fastConfig(suffix)
	cfg.DisableWorker = true
	c := mustNew(t, db, cfg, map[string]Handler{"t": func(context.Context, *Task) error { return nil }})
	id := uuid.Must(uuid.NewV7())
	db.MustExec(fmt.Sprintf(`INSERT INTO compensator_%s (id, task_type, payload, status, attempts, next_run_at, created_at, updated_at, completed_at)
VALUES ($1, 't', '1', 3, 1, now(), now(), now(), now() - interval '1 hour')`, suffix), id)

	// 与 Retry 相同的更新，保持事务未提交以持有行锁。
	retryTx := db.MustBeginTx(ctx, nil)
	defer retryTx.Rollback()
	retryTx.MustExecContext(ctx, fmt.Sprintf(`UPDATE compensator_%s SET status = 0, attempts = 0, lease_token = 0, last_error = '',
  next_run_at = now(), completed_at = NULL, updated_at = now() WHERE id = $1 AND status = 3`, suffix), id)
	// 清理跳过被锁定的行，不等待 Retry 提交。
	cleanupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	deleted, err := c.store.cleanup(cleanupCtx, StatusFailed, time.Minute, 100)
	cancel()
	if err != nil || deleted != 0 {
		t.Fatalf("cleanup deleted %d rows: %v", deleted, err)
	}
	if err = retryTx.Commit(); err != nil {
		t.Fatal(err)
	}
	if deleted, err = c.store.cleanup(ctx, StatusFailed, time.Minute, 100); err != nil || deleted != 0 {
		t.Fatalf("cleanup after retry deleted %d rows: %v", deleted, err)
	}
	if r, err := c.Get(ctx, id); err != nil || r.Status != StatusPending {
		t.Fatalf("retried task: %+v %v", r, err)
	}
}

func TestPostgreSQLIntegrationMultiNode(t *testing.T) {
	db := openTestDB(t)
	suffix := uniqueSuffix(t, db)
	var executions executionCounter
	var firstFail sync.Map
	handler := func(_ context.Context, task *Task) error {
		if _, loaded := firstFail.LoadOrStore(task.ID, true); !loaded {
			return errors.New("first attempt fails")
		}
		executions.inc(task.ID)
		return nil
	}
	nodes := make([]*Compensator, 3)
	for i := range nodes {
		nodes[i] = mustNew(t, db, fastConfig(suffix), map[string]Handler{"t": handler})
	}
	const total = 300
	var mu sync.Mutex
	ids := make([]uuid.UUID, 0, total)
	var wg sync.WaitGroup
	for n := range nodes {
		wg.Go(func() {
			task, _ := NewTask("t", n)
			for range total / len(nodes) / 10 {
				batch := addTasks(t, nodes[n], task, 10)
				mu.Lock()
				ids = append(ids, batch...)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	// 关闭一个节点，其余节点继续消费，任务仍全部完成。
	if err := nodes[0].Close(); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return countRows(t, db, suffix, "status = 2") == total })
	executions.assertOnce(t, ids)
}

func TestPostgreSQLIntegrationIndexesAndSpaceReuse(t *testing.T) {
	db := openTestDB(t)
	suffix := uniqueSuffix(t, db)
	ctx := context.Background()
	cfg := fastConfig(suffix)
	cfg.DisableWorker = true
	c := mustNew(t, db, cfg, map[string]Handler{"t": func(context.Context, *Task) error { return nil }})
	table := "compensator_" + suffix

	explain := func(query string, args ...any) string {
		tx := db.MustBegin()
		defer tx.Rollback()
		tx.MustExec("SET LOCAL enable_seqscan = off")
		var lines []string
		if err := tx.Select(&lines, "EXPLAIN "+query, args...); err != nil {
			t.Fatal(err)
		}
		return strings.Join(lines, "\n")
	}
	if plan := explain(fmt.Sprintf("SELECT id FROM %s WHERE status < 2 AND next_run_at <= now() ORDER BY next_run_at LIMIT 10", table)); !strings.Contains(plan, table+"_due_idx") {
		t.Fatalf("claim should use due index:\n%s", plan)
	}
	if plan := explain(fmt.Sprintf("SELECT id FROM %s WHERE status = 2 AND completed_at < now() LIMIT 10", table)); !strings.Contains(plan, table+"_done_idx") {
		t.Fatalf("cleanup should use done index:\n%s", plan)
	}

	// 写入路径已在其它用例覆盖；这里在服务端生成成功记录，只验证删除后空间复用，
	// 避免测试耗时受网络带宽影响。
	fill := func() {
		db.MustExec(fmt.Sprintf(`INSERT INTO %s (id, task_type, payload, status, attempts, next_run_at, created_at, updated_at, completed_at)
SELECT gen_random_uuid(), 't', repeat('x', 200), 2, 1, now(), now(), now(), now()
FROM generate_series(1, 50000) g`, table))
	}
	size := func() int64 {
		var n int64
		if err := db.Get(&n, "SELECT pg_total_relation_size($1::regclass)", table); err != nil {
			t.Fatal(err)
		}
		return n
	}
	purge := func() {
		for {
			n, err := c.store.cleanup(ctx, StatusSucceeded, 0, 5000)
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				break
			}
		}
		db.MustExec("VACUUM " + table)
	}
	fill()
	before := size()
	purge()
	fill()
	after := size()
	// 删除并 vacuum 后，同量写入复用已释放的空间，峰值大小基本不增长。
	if after > before+before/5 {
		t.Fatalf("table grew from %d to %d bytes; freed space was not reused", before, after)
	}
	purge()
	// 全部删除后 vacuum 可截断尾部空页，堆文件归还操作系统。
	var heap int64
	if err := db.Get(&heap, "SELECT pg_relation_size($1::regclass)", table); err != nil {
		t.Fatal(err)
	}
	if heap > before/10 {
		t.Fatalf("heap still %d bytes after deleting all rows", heap)
	}
}

// BenchmarkPostgreSQLTransaction 测量业务事务内登记任务的吞吐。baseline 是同样事务里
// 只做一条普通 INSERT，用于对比组件的额外开销。
func BenchmarkPostgreSQLTransaction(b *testing.B) {
	for _, batch := range []int{1, 10} {
		b.Run(fmt.Sprintf("tasks=%d", batch), func(b *testing.B) {
			db := openTestDB(b)
			cfg := fastConfig(uniqueSuffix(b, db))
			cfg.DisableWorker = true
			c := mustNew(b, db, cfg, map[string]Handler{"t": func(context.Context, *Task) error { return nil }})
			task, _ := NewTask("t", map[string]int{"order_id": 1})
			ctx := context.Background()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := c.Transaction(ctx, func(tx *Tx) error {
						for range batch {
							if _, err := tx.Add(ctx, task); err != nil {
								return err
							}
						}
						return nil
					}); err != nil {
						b.Error(err)
						return
					}
				}
			})
			b.ReportMetric(float64(b.N*batch)/b.Elapsed().Seconds(), "tasks/s")
		})
	}
	b.Run("baseline/plain-insert", func(b *testing.B) {
		db := openTestDB(b)
		table := "bench_" + strings.ReplaceAll(uuid.NewString()[:13], "-", "")
		db.MustExec(fmt.Sprintf("CREATE TABLE %s (id uuid PRIMARY KEY, payload text NOT NULL)", table))
		b.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS " + table) })
		ctx := context.Background()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				tx, err := db.BeginTxx(ctx, nil)
				if err == nil {
					_, err = tx.ExecContext(ctx, "INSERT INTO "+table+" VALUES ($1, $2)", uuid.Must(uuid.NewV7()), `{"order_id":1}`)
				}
				if err == nil {
					err = tx.Commit()
				}
				if err != nil {
					b.Error(err)
					return
				}
			}
		})
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tasks/s")
	})
}

// BenchmarkPostgreSQLBackground 测量后台消费吞吐：预先在服务端生成一批到期的待执行任务，
// 计时直到全部被节点领取、执行并落库为成功。每次迭代处理 20000 个任务，建议 -benchtime=1x。
func BenchmarkPostgreSQLBackground(b *testing.B) {
	const total = 20000
	for _, nodes := range []int{1, 3} {
		for _, workers := range []int{8, 32} {
			b.Run(fmt.Sprintf("nodes=%d/workers=%d", nodes, workers), func(b *testing.B) {
				db := openTestDB(b)
				var done atomic.Int64
				handler := func(context.Context, *Task) error { done.Add(1); return nil }
				var elapsed time.Duration
				for range b.N {
					suffix := uniqueSuffix(b, db)
					cfg := fastConfig(suffix)
					cfg.WorkerCount = workers
					instances := make([]*Compensator, nodes)
					for i := range instances {
						instances[i] = mustNew(b, db, cfg, map[string]Handler{"t": handler})
					}
					done.Store(0)
					db.MustExec(fmt.Sprintf(`INSERT INTO compensator_%s (id, task_type, payload, status, attempts, next_run_at, created_at, updated_at)
SELECT gen_random_uuid(), 't', '{"order_id":1}', 0, 0, now(), now(), now() FROM generate_series(1, %d)`, suffix, total))
					start := time.Now()
					for _, c := range instances {
						c.notify()
					}
					for countRows(b, db, suffix, "status = 2") < total {
						time.Sleep(20 * time.Millisecond)
					}
					elapsed += time.Since(start)
					for _, c := range instances {
						c.Close()
					}
					if done.Load() < total {
						b.Fatalf("handler ran %d times for %d tasks", done.Load(), total)
					}
				}
				b.ReportMetric(float64(total*b.N)/elapsed.Seconds(), "tasks/s")
			})
		}
	}
}

// BenchmarkPostgreSQLEndToEnd 测量单节点上从业务回调返回到任务回调完成的端到端延迟和吞吐，
// 以及调用方 Transaction 的耗时。8 个生产者并发提交单任务事务，共 b.N 个任务，
// 建议 -benchtime=2000x。
func BenchmarkPostgreSQLEndToEnd(b *testing.B) {
	for _, work := range []time.Duration{0, 20 * time.Millisecond} {
		for _, workers := range []int{8, 32} {
			b.Run(fmt.Sprintf("handler=%v/workers=%d", work, workers), func(b *testing.B) {
				db := openTestDB(b)
				cfg := fastConfig(uniqueSuffix(b, db))
				cfg.WorkerCount = workers
				cfg.ScanInterval = time.Second
				var e2e endToEnd
				e2e.init(b.N)
				c := mustNew(b, db, cfg, map[string]Handler{"t": func(_ context.Context, task *Task) error {
					time.Sleep(work)
					e2e.finished(task.ID)
					return nil
				}})
				task, _ := NewTask("t", map[string]int{"order_id": 1})
				ctx := context.Background()
				b.ResetTimer()
				e2e.produce(b, 8, func() error {
					return c.Transaction(ctx, func(tx *Tx) error {
						id, err := tx.Add(ctx, task)
						e2e.started(id)
						return err
					})
				})
				e2e.report(b)
			})
		}
	}
}

// endToEnd 记录每个任务从登记到回调完成的延迟和调用方耗时。
type endToEnd struct {
	starts    sync.Map
	mu        sync.Mutex
	latencies []time.Duration
	calls     []time.Duration
	want      int
	done      chan struct{}
	begin     time.Time
}

func (e *endToEnd) init(n int) {
	e.want, e.done = n, make(chan struct{})
	e.latencies, e.calls = make([]time.Duration, 0, n), make([]time.Duration, 0, n)
}

func (e *endToEnd) started(id uuid.UUID) { e.starts.Store(id, time.Now()) }

func (e *endToEnd) finished(id uuid.UUID) {
	started, ok := e.starts.LoadAndDelete(id)
	if !ok {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.latencies = append(e.latencies, time.Since(started.(time.Time)))
	if len(e.latencies) == e.want {
		close(e.done)
	}
}

// produce 以 producers 个并发生产者共提交 want 次 submit，并等待全部回调完成。
func (e *endToEnd) produce(b *testing.B, producers int, submit func() error) {
	e.begin = time.Now()
	var next atomic.Int64
	var wg sync.WaitGroup
	for range producers {
		wg.Go(func() {
			for next.Add(1) <= int64(e.want) {
				started := time.Now()
				if err := submit(); err != nil {
					b.Error(err)
					return
				}
				e.mu.Lock()
				e.calls = append(e.calls, time.Since(started))
				e.mu.Unlock()
			}
		})
	}
	wg.Wait()
	select {
	case <-e.done:
	case <-time.After(5 * time.Minute):
		b.Fatalf("only %d of %d tasks completed", len(e.latencies), e.want)
	}
}

func (e *endToEnd) report(b *testing.B) {
	elapsed := time.Since(e.begin)
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	b.ReportMetric(ms(percentile(e.latencies, 0.50)), "e2e_p50_ms")
	b.ReportMetric(ms(percentile(e.latencies, 0.99)), "e2e_p99_ms")
	b.ReportMetric(ms(percentile(e.calls, 0.50)), "call_p50_ms")
	b.ReportMetric(ms(percentile(e.calls, 0.99)), "call_p99_ms")
	b.ReportMetric(float64(e.want)/elapsed.Seconds(), "tasks/s")
}

func percentile(values []time.Duration, p float64) time.Duration {
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	return sorted[min(len(sorted)-1, int(float64(len(sorted))*p))]
}
