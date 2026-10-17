package compensator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
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
		HeartbeatInterval: time.Second, NodeTTL: 10 * time.Second, OperationTimeout: 15 * time.Second,
		CleanupInterval: time.Hour, Logger: logger,
	}
}

func uniqueSuffix(t testing.TB, db *sqlx.DB) string {
	t.Helper()
	suffix := "t" + strings.ReplaceAll(uuid.NewString()[:13], "-", "")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS compensator_%s, compensator_%s_lease", suffix, suffix))
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
	var shards int
	if err := db.Get(&shards, fmt.Sprintf("SELECT count(*) FROM compensator_%s_lease WHERE kind = 0", suffix)); err != nil || shards != shardCount {
		t.Fatalf("shard rows = %d, %v", shards, err)
	}
	// 已有表结构不兼容时拒绝启动。
	if _, err := db.Exec(fmt.Sprintf("ALTER TABLE compensator_%s ALTER COLUMN payload TYPE varchar(10)", suffix)); err != nil {
		t.Fatal(err)
	}
	if _, err := New(context.Background(), db, cfg, handlers); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("incompatible column should be rejected: %v", err)
	}
}

func TestPostgreSQLIntegrationInTx(t *testing.T) {
	db := openTestDB(t)
	suffix := uniqueSuffix(t, db)
	var failOnce sync.Map
	c := mustNew(t, db, fastConfig(suffix), map[string]Handler{
		"ok": func(context.Context, *Task) error { return nil },
		"flaky": func(_ context.Context, task *Task) error {
			if _, loaded := failOnce.LoadOrStore(task.ID, true); !loaded {
				return errors.New("first attempt fails")
			}
			return nil
		},
	})
	ctx := context.Background()
	okTask, _ := NewTask("ok", map[string]int64{"n": 9223372036854775807})
	flakyTask, _ := NewTask("flaky", "x")
	var okID, flakyID uuid.UUID
	results, err := c.Transaction(ctx, func(tx *Tx) error {
		var err error
		if okID, err = tx.Add(ctx, okTask); err != nil {
			return err
		}
		flakyID, err = tx.Add(ctx, flakyTask)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Status != StatusSucceeded || results[0].Err != nil || results[1].Status != StatusPending || results[1].Err == nil {
		t.Fatalf("unexpected first-run results: %+v", results)
	}
	record, err := c.Get(ctx, okID)
	if err != nil || record.Status != StatusSucceeded || record.Attempts != 1 || record.CompletedAt.IsZero() || record.Payload != `{"n":9223372036854775807}` {
		t.Fatalf("succeeded record: %+v, %v", record, err)
	}
	eventually(t, func() bool { return statusOf(t, c, flakyID) == StatusSucceeded })
	if r, _ := c.Get(ctx, flakyID); r.Attempts != 2 {
		t.Fatalf("flaky task attempts = %d", r.Attempts)
	}

	// 业务失败或登记失败时整个事务回滚，不留下任务。
	var rolledBack uuid.UUID
	if _, err = c.Transaction(ctx, func(tx *Tx) error {
		rolledBack, _ = tx.Add(ctx, okTask)
		return errors.New("business failed")
	}); err == nil || err.Error() != "business failed" {
		t.Fatalf("business error should be returned: %v", err)
	}
	if _, err = c.Get(ctx, rolledBack); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rolled back task should not exist: %v", err)
	}
	if _, err = c.Transaction(ctx, func(tx *Tx) error {
		_, _ = tx.Add(ctx, Task{Type: "unknown"})
		return nil // 忽略 Add 错误也不能提交
	}); !errors.Is(err, ErrNoHandler) {
		t.Fatalf("unregistered type should abort the transaction: %v", err)
	}

	// 外部事务接入。
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := c.AddTx(ctx, tx, okTask)
	if err != nil || result.Status != StatusSucceeded {
		t.Fatalf("AddTx: %+v %v", result, err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if statusOf(t, c, result.ID) != StatusSucceeded {
		t.Fatal("AddTx task should be succeeded after commit")
	}
}

func TestPostgreSQLIntegrationAfterCommit(t *testing.T) {
	db := openTestDB(t)
	suffix := uniqueSuffix(t, db)
	ctx := context.Background()
	if _, err := db.Exec(fmt.Sprintf("CREATE TABLE biz_%s (id uuid PRIMARY KEY)", suffix)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS biz_%s", suffix)) })
	var calls sync.Map
	handler := func(ctx context.Context, task *Task) error {
		var id uuid.UUID
		if err := task.Decode(&id); err != nil {
			return err
		}
		// 回调在提交之后执行，必须能从其它连接看到业务数据。
		var n int
		if err := db.GetContext(ctx, &n, fmt.Sprintf("SELECT count(*) FROM biz_%s WHERE id = $1", suffix), id); err != nil {
			return err
		}
		if n != 1 {
			return errors.New("business row not visible")
		}
		counter, _ := calls.LoadOrStore(task.ID, new(atomic.Int64))
		counter.(*atomic.Int64).Add(1)
		return nil
	}
	cfg := fastConfig(suffix)
	cfg.Mode = ModeAfterCommit
	c := mustNew(t, db, cfg, map[string]Handler{"t": handler})

	bizID := uuid.New()
	task, _ := NewTask("t", bizID)
	results, err := c.Transaction(ctx, func(tx *Tx) error {
		if _, err := tx.SQLTx().ExecContext(ctx, fmt.Sprintf("INSERT INTO biz_%s VALUES ($1)", suffix), bizID); err != nil {
			return err
		}
		_, err := tx.Add(ctx, task)
		return err
	})
	if err != nil || len(results) != 1 || results[0].Status != StatusSucceeded || results[0].Err != nil {
		t.Fatalf("after-commit transaction: %+v %v", results, err)
	}

	// 外部事务：提交后显式 Dispatch。
	bizID2 := uuid.New()
	task2, _ := NewTask("t", bizID2)
	tx, _ := db.BeginTxx(ctx, nil)
	tx.MustExecContext(ctx, fmt.Sprintf("INSERT INTO biz_%s VALUES ($1)", suffix), bizID2)
	added, err := c.AddTx(ctx, tx, task2)
	if err != nil || added.Status != StatusRunning {
		t.Fatalf("AddTx: %+v %v", added, err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	dispatched, err := c.Dispatch(ctx, added.ID)
	if err != nil || dispatched[0].Status != StatusSucceeded {
		t.Fatalf("Dispatch: %+v %v", dispatched, err)
	}
	if again, _ := c.Dispatch(ctx, added.ID); again[0].Status != StatusSucceeded {
		t.Fatalf("second Dispatch must not rerun: %+v", again)
	}
	if r, _ := c.Get(ctx, added.ID); r.Attempts != 1 {
		t.Fatalf("dispatch of own task must not add attempts: %d", r.Attempts)
	}

	// 模拟提交后崩溃：登记但不 Dispatch，租约到期后由后台接管。
	bizID3 := uuid.New()
	task3, _ := NewTask("t", bizID3)
	tx, _ = db.BeginTxx(ctx, nil)
	tx.MustExecContext(ctx, fmt.Sprintf("INSERT INTO biz_%s VALUES ($1)", suffix), bizID3)
	crashed, err := c.AddTx(ctx, tx, task3)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return statusOf(t, c, crashed.ID) == StatusSucceeded })
	calls.Range(func(key, value any) bool {
		if n := value.(*atomic.Int64).Load(); n != 1 {
			t.Errorf("task %v executed %d times", key, n)
		}
		return true
	})
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
	results, err := c.Transaction(ctx, func(tx *Tx) error { _, err := tx.Add(ctx, task); return err })
	if err != nil {
		t.Fatal(err)
	}
	id := results[0].ID
	eventually(t, func() bool { return statusOf(t, c, id) == StatusFailed })
	if r, _ := c.Get(ctx, id); r.Attempts != 3 || r.LastError != "downstream unavailable" || attempts.Load() != 3 {
		t.Fatalf("exhausted record: %+v, calls %d", r, attempts.Load())
	}
	if err = c.Retry(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retry missing: %v", err)
	}
	healthy.Store(true)
	if err = c.Retry(ctx, id); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return statusOf(t, c, id) == StatusSucceeded })
	if err = c.Retry(ctx, id); !errors.Is(err, ErrNotFailed) {
		t.Fatalf("retry succeeded task: %v", err)
	}

	// MaxRetries=-1：首次失败直接进入 failed。
	cfg2 := fastConfig(uniqueSuffix(t, db))
	cfg2.MaxRetries = -1
	c2 := mustNew(t, db, cfg2, map[string]Handler{"t": func(context.Context, *Task) error { return errors.New("no") }})
	results, err = c2.Transaction(ctx, func(tx *Tx) error { _, err := tx.Add(ctx, task); return err })
	if err != nil || results[0].Status != StatusFailed {
		t.Fatalf("no-retry: %+v %v", results, err)
	}
}

func TestPostgreSQLIntegrationLeaseTakeover(t *testing.T) {
	db := openTestDB(t)
	suffix := uniqueSuffix(t, db)
	ctx := context.Background()
	var calls atomic.Int64
	cfg := fastConfig(suffix)
	c := mustNew(t, db, cfg, map[string]Handler{"t": func(context.Context, *Task) error { calls.Add(1); return nil }})
	task, _ := NewTask("t", 1)
	results, err := c.Transaction(ctx, func(tx *Tx) error { _, err := tx.Add(ctx, task); return err })
	if err != nil {
		t.Fatal(err)
	}
	// 模拟执行者领取后崩溃：任务停留在 running，租约到期后被重新执行。
	id := results[0].ID
	db.MustExec(fmt.Sprintf("UPDATE compensator_%s SET status = 1, lease_token = 7, completed_at = NULL, next_run_at = now() + interval '1 second' WHERE id = $1", suffix), id)
	eventually(t, func() bool { return statusOf(t, c, id) == StatusSucceeded })
	// 旧执行者的结果不能覆盖新状态。
	if err = c.store.finish(ctx, claimed{Task: Task{ID: id}, token: 7}, StatusPending, "late", time.Second); !errors.Is(err, ErrLeaseLost) {
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
	c := mustNew(t, db, cfg, map[string]Handler{
		"ok":   func(context.Context, *Task) error { return nil },
		"fail": func(context.Context, *Task) error { return errors.New("x") },
	})
	count := func(where string) int {
		var n int
		if err := db.Get(&n, fmt.Sprintf("SELECT count(*) FROM compensator_%s WHERE %s", suffix, where)); err != nil {
			t.Fatal(err)
		}
		return n
	}
	ok, _ := NewTask("ok", 1)
	fail, _ := NewTask("fail", 1)
	if _, err := c.Transaction(ctx, func(tx *Tx) error {
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
	// 未到保留期前不删除（保留期留足余量，避免慢速网络下的时序抖动）。
	if n := count("true"); n != 51 {
		t.Fatalf("rows deleted too early: %d", n)
	}
	eventually(t, func() bool { return count("true") == 0 })
}

func TestPostgreSQLIntegrationMultiNode(t *testing.T) {
	db := openTestDB(t)
	suffix := uniqueSuffix(t, db)
	ctx := context.Background()
	var executions sync.Map
	var firstFail sync.Map
	handler := func(_ context.Context, task *Task) error {
		if _, loaded := firstFail.LoadOrStore(task.ID, true); !loaded {
			return errors.New("first attempt fails")
		}
		counter, _ := executions.LoadOrStore(task.ID, new(atomic.Int64))
		counter.(*atomic.Int64).Add(1)
		return nil
	}
	nodes := make([]*Compensator, 3)
	for i := range nodes {
		nodes[i] = mustNew(t, db, fastConfig(suffix), map[string]Handler{"t": handler})
	}
	ownedBy := func() map[string]int {
		rows, err := db.Query(fmt.Sprintf("SELECT owner_id, count(*) FROM compensator_%s_lease WHERE kind = 0 AND expires_at > now() GROUP BY owner_id", suffix))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		result := map[string]int{}
		for rows.Next() {
			var owner string
			var n int
			if err = rows.Scan(&owner, &n); err != nil {
				t.Fatal(err)
			}
			result[owner] = n
		}
		return result
	}
	balanced := func(n int) bool {
		owned := ownedBy()
		total, lo, hi := 0, shardCount, 0
		for _, v := range owned {
			total += v
			lo, hi = min(lo, v), max(hi, v)
		}
		return len(owned) == n && total == shardCount && hi-lo <= 2
	}
	eventually(t, func() bool { return balanced(3) })

	const total = 300
	ids := make([]uuid.UUID, 0, total)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for n := range nodes {
		wg.Go(func() {
			for range total / len(nodes) / 10 {
				results, err := nodes[n].Transaction(ctx, func(tx *Tx) error {
					for range 10 {
						task, _ := NewTask("t", n)
						if _, err := tx.Add(ctx, task); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				for _, r := range results {
					ids = append(ids, r.ID)
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	// 关闭一个节点，其分片应被其余节点接管，任务仍全部完成。
	if err := nodes[0].Close(); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return balanced(2) })
	eventually(t, func() bool {
		var n int
		_ = db.Get(&n, fmt.Sprintf("SELECT count(*) FROM compensator_%s WHERE status = 2", suffix))
		return n == total
	})
	for _, id := range ids {
		counter, ok := executions.Load(id)
		if !ok || counter.(*atomic.Int64).Load() != 1 {
			t.Fatalf("task %v did not succeed exactly once", id)
		}
	}
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
	if plan := explain(fmt.Sprintf("SELECT id FROM %s WHERE shard_id = 1 AND status < 2 AND next_run_at <= now() ORDER BY next_run_at LIMIT 10", table)); !strings.Contains(plan, table+"_due_idx") {
		t.Fatalf("claim should use due index:\n%s", plan)
	}
	if plan := explain(fmt.Sprintf("SELECT id FROM %s WHERE shard_id = ANY('{1,2}'::int2[]) AND status = 2 AND completed_at < now() LIMIT 10", table)); !strings.Contains(plan, table+"_done_idx") {
		t.Fatalf("cleanup should use done index:\n%s", plan)
	}

	// 写入路径已在其它用例覆盖；这里在服务端生成成功记录，只验证删除后空间复用，
	// 避免测试耗时受网络带宽影响。
	fill := func() {
		if _, err := c.Transaction(ctx, func(tx *Tx) error {
			_, err := tx.SQLTx().ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (id, task_type, payload, shard_id, status, attempts,
  next_run_at, created_at, updated_at, completed_at)
SELECT gen_random_uuid(), 't', repeat('x', 200), g %% %d, 2, 1, now(), now(), now(), now()
FROM generate_series(1, 50000) g`, table, shardCount))
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	size := func() int64 {
		var n int64
		if err := db.Get(&n, "SELECT pg_total_relation_size($1::regclass)", table); err != nil {
			t.Fatal(err)
		}
		return n
	}
	purge := func() {
		shards := make([]int16, shardCount)
		for i := range shards {
			shards[i] = int16(i)
		}
		for {
			n, err := c.store.cleanup(ctx, shards, StatusSucceeded, 0, 5000)
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

func BenchmarkPostgreSQLTransactionInTx(b *testing.B) {
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
					if _, err := c.Transaction(ctx, func(tx *Tx) error {
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
}
