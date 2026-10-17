package outbox

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

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

func TestPostgreSQLIntegration(t *testing.T) { runSQLIntegration(t, "pgx", "TEST_POSTGRES_DSN") }
func TestMySQLIntegration(t *testing.T)      { runSQLIntegration(t, "mysql", "TEST_MYSQL_DSN") }

func sqlTestDB(t testing.TB, driver, environment string) *sqlx.DB {
	t.Helper()
	dsn := os.Getenv(environment)
	if dsn == "" {
		t.Skipf("未设置 %s，跳过 %s 集成测试", environment, driver)
	}
	db, err := sqlx.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(16)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		t.Fatalf("连接测试数据库: %v", err)
	}
	return db
}

var integrationSequence atomic.Int64
var integrationTables sync.Map

func sqlTestConfig() Config {
	return Config{TableSuffix: fmt.Sprintf("it_%d_%d", time.Now().UnixNano(), integrationSequence.Add(1)), DisableBackground: true,
		HandlerTimeout: 100 * time.Millisecond, TaskLease: time.Second, OperationTimeout: 500 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond, ShardLease: 500 * time.Millisecond, ScanInterval: 20 * time.Millisecond,
		RetryMin: 2 * time.Millisecond, RetryMax: 2 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func sqlTestOutbox(t testing.TB, db *sqlx.DB, c Config, handlers map[string]Handler) *Outbox {
	t.Helper()
	o, err := New(context.Background(), db, c, handlers)
	if err != nil {
		t.Fatal(err)
	}
	s := o.store.(*sqlStorage)
	key := fmt.Sprintf("%p/%s/%s", db, s.schema, s.messages)
	if _, loaded := integrationTables.LoadOrStore(key, true); !loaded {
		t.Cleanup(func() {
			defer integrationTables.Delete(key)
			dropCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			for _, name := range []string{s.messages, s.nodes, s.shards} {
				if _, err := db.ExecContext(dropCtx, "DROP TABLE IF EXISTS "+s.table(name)); err != nil {
					t.Error(err)
				}
			}
		})
	}
	t.Cleanup(func() {
		if err := o.Close(); err != nil {
			t.Error(err)
		}
	})
	return o
}

func sqlExec(t testing.TB, s *sqlStorage, query string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := s.db.ExecContext(ctx, s.bind(query), args...); err != nil {
		t.Fatal(err)
	}
}

func enqueueCommitted(t testing.TB, o *Outbox, m Message) int64 {
	t.Helper()
	ctx := context.Background()
	tx, err := o.db.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	id, err := o.EnqueueTx(ctx, tx, m)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return id
}

func eventually(t testing.TB, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("等待条件超时")
}

func runSQLIntegration(t *testing.T, driver, environment string) {
	db := sqlTestDB(t, driver, environment)
	t.Run("transactions-and-json", func(t *testing.T) {
		var calls atomic.Int32
		var businessTable string
		handler := func(ctx context.Context, m *Message) error {
			calls.Add(1)
			var value struct {
				ID   int64  `json:"id"`
				Name string `json:"name"`
			}
			if err := m.Decode(&value); err != nil {
				return err
			}
			if value.ID != 9223372036854775807 || value.Name != "中文🙂" {
				return errors.New("JSON round trip mismatch")
			}
			var count int
			if err := db.QueryRowxContext(ctx, "SELECT COUNT(*) FROM "+businessTable).Scan(&count); err != nil {
				return err
			}
			if count != 1 {
				return errors.New("business commit not visible")
			}
			return nil
		}
		o := sqlTestOutbox(t, db, sqlTestConfig(), map[string]Handler{"test": handler})
		s := o.store.(*sqlStorage)
		businessTable = s.table("business_" + o.config.TableSuffix)
		sqlExec(t, s, "CREATE TABLE "+businessTable+" (id BIGINT PRIMARY KEY)")
		t.Cleanup(func() { sqlExec(t, s, "DROP TABLE "+businessTable) })
		message, err := NewMessage("test", struct {
			ID   int64  `json:"id"`
			Name string `json:"name"`
		}{9223372036854775807, "中文🙂"})
		if err != nil {
			t.Fatal(err)
		}
		results, err := o.Transaction(context.Background(), func(tx *Tx) error {
			if _, err := tx.SQLTx().ExecContext(context.Background(), "INSERT INTO "+businessTable+" (id) VALUES (1)"); err != nil {
				return err
			}
			_, err := tx.Enqueue(context.Background(), message)
			if calls.Load() != 0 {
				t.Error("提交前执行了回调")
			}
			return err
		})
		if err != nil || len(results) != 1 || results[0].AttemptError != nil || results[0].Status != StatusSucceeded {
			t.Fatalf("首次处理: %+v %v", results, err)
		}
		id := results[0].ID
		rollbackErr := errors.New("rollback business")
		_, err = o.Transaction(context.Background(), func(tx *Tx) error {
			if _, err := tx.SQLTx().ExecContext(context.Background(), "INSERT INTO "+businessTable+" (id) VALUES (2)"); err != nil {
				return err
			}
			if _, err := tx.Enqueue(context.Background(), message); err != nil {
				return err
			}
			return rollbackErr
		})
		if !errors.Is(err, rollbackErr) || calls.Load() != 1 {
			t.Fatalf("回滚行为: %v %d", err, calls.Load())
		}
		var count int
		if err = db.QueryRowx("SELECT COUNT(*) FROM " + s.table(s.messages)).Scan(&count); err != nil || count != 1 {
			t.Fatalf("回滚后消息数: %d %v", count, err)
		}
		if err = db.QueryRowx("SELECT COUNT(*) FROM " + businessTable).Scan(&count); err != nil || count != 1 {
			t.Fatalf("回滚后业务数: %d %v", count, err)
		}
		if _, err = o.Publish(context.Background(), Message{Type: "test", Payload: `{"broken":`}); err == nil {
			t.Fatal("数据库接受了非法 JSON")
		}
		if calls.Load() != 1 {
			t.Fatal("非法载荷触发回调")
		}
		// 已有事务不触发回调，ScanOnce 可以补偿未显式 Dispatch 的消息。
		externalID := enqueueCommitted(t, o, message)
		if calls.Load() != 1 {
			t.Fatal("入队时执行了回调")
		}
		if err = o.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		r, err := o.Get(context.Background(), externalID)
		if err != nil || r.Status != StatusSucceeded {
			t.Fatalf("补偿结果: %+v %v", r, err)
		}
		if _, err = o.Dispatch(context.Background(), id); err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 2 {
			t.Fatal("成功记录被重复处理")
		}
		var kind string
		query := s.bind("SELECT data_type FROM information_schema.columns WHERE table_schema=? AND table_name=? AND column_name='payload'")
		if err = db.QueryRowx(query, s.schema, s.messages).Scan(&kind); err != nil {
			t.Fatal(err)
		}
		want := "json"
		if driver == "pgx" {
			want = "jsonb"
		}
		if kind != want {
			t.Fatalf("载荷字段类型: %s", kind)
		}
	})
	t.Run("retry-budget-and-replay", func(t *testing.T) {
		var calls atomic.Int32
		var succeed atomic.Bool
		o := sqlTestOutbox(t, db, sqlTestConfig(), map[string]Handler{"test": func(context.Context, *Message) error {
			calls.Add(1)
			if succeed.Load() {
				return nil
			}
			return errors.New("temporary error")
		}})
		s := o.store.(*sqlStorage)
		m, _ := NewMessage("test", map[string]int{"n": 1})
		result, err := o.Publish(context.Background(), m)
		if err != nil || result.AttemptError == nil {
			t.Fatalf("持久化与处理错误未区分: %+v %v", result, err)
		}
		for range 20 {
			sqlExec(t, s, "UPDATE "+s.table(s.messages)+" SET next_attempt_at=0 WHERE id=?", result.ID)
			if _, err = o.Dispatch(context.Background(), result.ID); err != nil {
				t.Fatal(err)
			}
		}
		r, err := o.Get(context.Background(), result.ID)
		if err != nil || r.Status != StatusFailed || r.Attempts != 21 || calls.Load() != 21 {
			t.Fatalf("重试预算: %+v %d %v", r, calls.Load(), err)
		}
		_, _ = o.Dispatch(context.Background(), r.ID)
		if calls.Load() != 21 {
			t.Fatal("耗尽后继续回调")
		}
		if err = o.Retry(context.Background(), r.ID); err != nil {
			t.Fatal(err)
		}
		succeed.Store(true)
		results, err := o.Dispatch(context.Background(), r.ID)
		if err != nil || results[0].Status != StatusSucceeded || results[0].ID != r.ID {
			t.Fatalf("重投: %+v %v", results, err)
		}
	})
	t.Run("task-lease-fencing", func(t *testing.T) {
		o := sqlTestOutbox(t, db, sqlTestConfig(), map[string]Handler{"test": func(context.Context, *Message) error { return nil }})
		s := o.store.(*sqlStorage)
		m, _ := NewMessage("test", nil)
		id := enqueueCommitted(t, o, m)
		first, err := s.claimIDs(context.Background(), []int64{id}, 1)
		if err != nil || len(first) != 1 {
			t.Fatalf("首次领取: %v %v", first, err)
		}
		second, err := s.claimIDs(context.Background(), []int64{id}, 1)
		if err != nil || len(second) != 0 {
			t.Fatal("未过期任务被重复领取")
		}
		sqlExec(t, s, "UPDATE "+s.table(s.messages)+" SET lease_until=0 WHERE id=?", id)
		if _, err = s.finish(context.Background(), first[0], nil, 0); !errors.Is(err, ErrLeaseLost) {
			t.Fatal("过期执行者可以更新结果", err)
		}
		second, err = s.claimIDs(context.Background(), []int64{id}, 1)
		if err != nil || len(second) != 1 {
			t.Fatalf("过期接管: %+v %v", second, err)
		}
		if _, err = s.finish(context.Background(), first[0], nil, 0); !errors.Is(err, ErrLeaseLost) {
			t.Fatal("旧令牌可以更新结果", err)
		}
		if _, err = s.finish(context.Background(), second[0], nil, 0); err != nil {
			t.Fatal(err)
		}
		// 崩溃也消耗投递预算，最后一次领取后崩溃不能无限重复执行。
		id = enqueueCommitted(t, o, m)
		sqlExec(t, s, "UPDATE "+s.table(s.messages)+" SET status=1,attempts=21,lease_until=0 WHERE id=?", id)
		rows, err := s.claimIDs(context.Background(), []int64{id}, 1)
		if err != nil || len(rows) != 0 {
			t.Fatal(rows, err)
		}
		r, _ := o.Get(context.Background(), id)
		if r.Status != StatusFailed {
			t.Fatal(r)
		}
	})
	t.Run("membership-rebalance-and-crash", func(t *testing.T) {
		c := sqlTestConfig()
		handler := map[string]Handler{"test": func(context.Context, *Message) error { return nil }}
		a := sqlTestOutbox(t, db, c, handler)
		b := sqlTestOutbox(t, db, c, handler)
		d := sqlTestOutbox(t, db, c, handler)
		for range 4 {
			for _, o := range []*Outbox{a, b, d} {
				if err := o.refresh(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
		}
		total := 0
		for _, o := range []*Outbox{a, b, d} {
			n := len(o.owned())
			if n < 21 || n > 22 {
				t.Fatalf("三节点分配不均: %d", n)
			}
			total += n
		}
		if total != shardCount {
			t.Fatal(total)
		}
		old := d.owned()[0]
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		for range 3 {
			if err := a.refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := b.refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if len(a.owned()) != 32 || len(b.owned()) != 32 {
			t.Fatal("缩容后未均衡")
		}
		s := a.store.(*sqlStorage)
		if _, err := s.claimShard(context.Background(), old, 1); !errors.Is(err, ErrLeaseLost) {
			t.Fatal("旧分片持有者仍可领取", err)
		}
		old = b.owned()[0]
		b.cancel()
		sqlExec(t, s, "UPDATE "+s.table(s.nodes)+" SET lease_until=0 WHERE node_id=?", b.nodeID)
		sqlExec(t, s, "UPDATE "+s.table(s.shards)+" SET lease_until=0 WHERE owner_id=?", b.nodeID)
		if err := a.refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(a.owned()) != shardCount {
			t.Fatal("未接管故障节点")
		}
		if _, err := s.claimShard(context.Background(), old, 1); !errors.Is(err, ErrLeaseLost) {
			t.Fatal(err)
		}
	})
	t.Run("concurrent-shard-handoffs", func(t *testing.T) {
		c := sqlTestConfig()
		var nodes []*Outbox
		for range 4 {
			nodes = append(nodes, sqlTestOutbox(t, db, c, map[string]Handler{"test": func(context.Context, *Message) error { return nil }}))
		}
		refreshAll := func() {
			t.Helper()
			var wg sync.WaitGroup
			errors := make(chan error, len(nodes))
			for _, node := range nodes {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := node.refresh(context.Background()); err != nil {
						errors <- err
					}
				}()
			}
			wg.Wait()
			close(errors)
			for err := range errors {
				t.Fatal(err)
			}
		}
		for range 4 {
			refreshAll()
		}
		for _, node := range nodes {
			if len(node.owned()) != 16 {
				t.Fatal("四节点没有均分分片")
			}
		}
		// 缩容会重新排列剩余节点的归属，同时刷新可暴露交换分片时的锁环。
		if err := nodes[1].Close(); err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes[:1], nodes[2:]...)
		for range 4 {
			refreshAll()
		}
		total := 0
		for _, node := range nodes {
			n := len(node.owned())
			if n < 21 || n > 22 {
				t.Fatal("并发交接后分片分配不均")
			}
			total += n
		}
		if total != shardCount {
			t.Fatal("并发交接丢失分片")
		}
	})
	t.Run("background-and-foreground-concurrency", func(t *testing.T) {
		c := sqlTestConfig()
		c.DisableBackground = false
		var mu sync.Mutex
		seen := map[int64]int{}
		nodeCalls := make([]int, 2)
		makeHandler := func(node int) Handler {
			return func(_ context.Context, m *Message) error {
				mu.Lock()
				seen[m.ID]++
				nodeCalls[node]++
				mu.Unlock()
				time.Sleep(time.Millisecond)
				return nil
			}
		}
		a := sqlTestOutbox(t, db, c, map[string]Handler{"test": makeHandler(0)})
		b := sqlTestOutbox(t, db, c, map[string]Handler{"test": makeHandler(1)})
		s := a.store.(*sqlStorage)
		eventually(t, func() bool { return len(a.owned()) == 32 && len(b.owned()) == 32 })
		m, _ := NewMessage("test", map[string]string{"value": "shared"})
		tx, err := db.BeginTxx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		var ids []int64
		for range 256 {
			id, err := a.EnqueueTx(context.Background(), tx, m)
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		mu.Lock()
		before := len(seen)
		mu.Unlock()
		if before != 0 {
			t.Fatal("未提交消息被扫描")
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if _, err = a.Dispatch(context.Background(), ids[:16]...); err != nil {
			t.Fatal(err)
		}
		eventually(t, func() bool {
			var count int
			err := db.QueryRowx("SELECT COUNT(*) FROM " + s.table(s.messages) + " WHERE status=2").Scan(&count)
			return err == nil && count == len(ids)
		})
		mu.Lock()
		defer mu.Unlock()
		if len(seen) != len(ids) || nodeCalls[0] == 0 || nodeCalls[1] == 0 {
			t.Fatalf("节点没有共同处理: %d %+v", len(seen), nodeCalls)
		}
		for id, count := range seen {
			if count != 1 {
				t.Fatalf("正常运行重复处理 %d: %d", id, count)
			}
		}
	})
	t.Run("cleanup-state-and-batch-boundaries", func(t *testing.T) {
		c := sqlTestConfig()
		c.CleanupBatchSize = 1
		o := sqlTestOutbox(t, db, c, map[string]Handler{"test": func(context.Context, *Message) error { return nil }})
		s := o.store.(*sqlStorage)
		if err := o.refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		m, _ := NewMessage("test", nil)
		var ids []int64
		for range 6 {
			r, err := o.Publish(context.Background(), m)
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, r.ID)
		}
		for i, id := range ids {
			status := StatusSucceeded
			if i == 3 {
				status = StatusPending
			}
			if i == 4 {
				status = StatusFailed
			}
			if i == 5 {
				status = StatusProcessing
			}
			completed := s.nowExpression()
			if i != 2 {
				completed = fmt.Sprintf("(%s-%d)", s.nowExpression(), s.config.Retention.Milliseconds()+1000)
			}
			sqlExec(t, s, "UPDATE "+s.table(s.messages)+" SET shard_id=0,status=?,completed_at="+completed+" WHERE id=?", status, id)
		}
		var lease shardLease
		for _, l := range o.owned() {
			if l.ShardID == 0 {
				lease = l
			}
		}
		for range 2 {
			n, err := s.cleanup(context.Background(), lease)
			if err != nil || n != 1 {
				t.Fatal(n, err)
			}
		}
		if n, err := s.cleanup(context.Background(), lease); err != nil || n != 0 {
			t.Fatal(n, err)
		}
		for _, id := range ids[:2] {
			if _, err := o.Get(context.Background(), id); !errors.Is(err, ErrNotFound) {
				t.Fatal("过期成功记录未清理", err)
			}
		}
		for _, id := range ids[2:] {
			if _, err := o.Get(context.Background(), id); err != nil {
				t.Fatal("清理误删了记录", err)
			}
		}
	})
	t.Run("concurrent-schema-initialization", func(t *testing.T) {
		c := sqlTestConfig()
		var wg sync.WaitGroup
		var mu sync.Mutex
		var instances []*Outbox
		var failures []error
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				o, err := New(context.Background(), db, c, map[string]Handler{"test": func(context.Context, *Message) error { return nil }})
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					failures = append(failures, err)
				} else {
					instances = append(instances, o)
				}
			}()
		}
		wg.Wait()
		for _, o := range instances {
			if err := o.Close(); err != nil {
				t.Error(err)
			}
		}
		if len(instances) > 0 {
			s := instances[0].store.(*sqlStorage)
			defer func() {
				for _, name := range []string{s.messages, s.nodes, s.shards} {
					sqlExec(t, s, "DROP TABLE "+s.table(name))
				}
			}()
		}
		if len(failures) > 0 {
			t.Fatal(failures)
		}
	})
	t.Run("reject-incompatible-schema", func(t *testing.T) {
		c := sqlTestConfig()
		table := "outbox_" + c.TableSuffix
		if _, err := db.Exec("CREATE TABLE " + table + " (id BIGINT PRIMARY KEY)"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + table) })
		_, err := New(context.Background(), db, c, map[string]Handler{"test": func(context.Context, *Message) error { return nil }})
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatal(err)
		}
	})
	if driver == "pgx" {
		t.Run("postgres-vacuum-and-indexes", func(t *testing.T) { testPostgresMaintenance(t, db) })
	}
}

func testPostgresMaintenance(t *testing.T, db *sqlx.DB) {
	o := sqlTestOutbox(t, db, sqlTestConfig(), map[string]Handler{"test": func(context.Context, *Message) error { return nil }})
	s := o.store.(*sqlStorage)
	var options string
	if err := db.QueryRowx("SELECT array_to_string(reloptions, ',') FROM pg_class WHERE oid=$1::regclass", s.table(s.messages)).Scan(&options); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"fillfactor=90", "autovacuum_vacuum_scale_factor=0.01", "vacuum_truncate=false"} {
		if !strings.Contains(options, want) {
			t.Fatalf("缺少表参数 %s: %s", want, options)
		}
	}
	var toastOptions string
	if err := db.QueryRowx("SELECT array_to_string(t.reloptions, ',') FROM pg_class c JOIN pg_class t ON t.oid=c.reltoastrelid WHERE c.oid=$1::regclass", s.table(s.messages)).Scan(&toastOptions); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(toastOptions, "autovacuum_vacuum_threshold=1000") {
		t.Fatal(toastOptions)
	}
	// 清空后执行普通 VACUUM，再写入同样规模的数据，检查 heap 页面确实复用。
	insert := fmt.Sprintf(`INSERT INTO %s (message_type,payload,shard_id,status,next_attempt_at)
		SELECT 'test',jsonb_build_object('id',g,'text',md5(g::text)),MOD(g,64),0,0 FROM generate_series(1,5000) g`, s.table(s.messages))
	sqlExec(t, s, insert)
	sqlExec(t, s, "ANALYZE "+s.table(s.messages))
	var before, after int64
	if err := db.QueryRowx("SELECT pg_relation_size($1::regclass)", s.table(s.messages)).Scan(&before); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Queryx("EXPLAIN SELECT id FROM " + s.table(s.messages) + " WHERE shard_id=1 AND status=0 AND next_attempt_at<=0 ORDER BY next_attempt_at,id LIMIT 4")
	if err != nil {
		t.Fatal(err)
	}
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err = rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(line)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan.String(), s.indexes()[0].name) {
		t.Fatalf("扫描没有使用待处理索引: %s", plan.String())
	}
	sqlExec(t, s, "DELETE FROM "+s.table(s.messages))
	sqlExec(t, s, "VACUUM (ANALYZE) "+s.table(s.messages))
	sqlExec(t, s, insert)
	if err := db.QueryRowx("SELECT pg_relation_size($1::regclass)", s.table(s.messages)).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after > before+before/4+8192 {
		t.Fatalf("VACUUM 后空间未有效复用: before=%d after=%d", before, after)
	}
	sqlExec(t, s, "ALTER TABLE "+s.table(s.messages)+" SET (fillfactor=80)")
	again, err := New(context.Background(), db, o.config.Config, o.handlers)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if err = db.QueryRowx("SELECT array_to_string(reloptions, ',') FROM pg_class WHERE oid=$1::regclass", s.table(s.messages)).Scan(&options); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(options, "fillfactor=80") {
		t.Fatal("覆盖了已有表的运维配置")
	}
}

func BenchmarkSQLThroughput(b *testing.B) {
	for _, backend := range []struct{ driver, environment string }{{"pgx", "TEST_POSTGRES_DSN"}, {"mysql", "TEST_MYSQL_DSN"}} {
		b.Run(backend.driver, func(b *testing.B) {
			db := sqlTestDB(b, backend.driver, backend.environment)
			for _, nodes := range []int{1, 4} {
				b.Run(fmt.Sprintf("nodes-%d", nodes), func(b *testing.B) {
					c := sqlTestConfig()
					c.DisableBackground = false
					c.WorkerCount = 8
					var first *Outbox
					for range nodes {
						o := sqlTestOutbox(b, db, c, map[string]Handler{"test": func(context.Context, *Message) error { return nil }})
						if first == nil {
							first = o
						}
					}
					s := first.store.(*sqlStorage)
					m, _ := NewMessage("test", map[string]string{"value": "benchmark"})
					b.ReportAllocs()
					b.ResetTimer()
					for offset := 0; offset < b.N; {
						count := min(256, b.N-offset)
						tx, err := db.BeginTxx(context.Background(), nil)
						if err != nil {
							b.Fatal(err)
						}
						for range count {
							if _, err = first.EnqueueTx(context.Background(), tx, m); err != nil {
								_ = tx.Rollback()
								b.Fatal(err)
							}
						}
						if err = tx.Commit(); err != nil {
							b.Fatal(err)
						}
						offset += count
						eventually(b, func() bool {
							var n int
							err := db.QueryRowx("SELECT COUNT(*) FROM " + s.table(s.messages) + " WHERE status=2").Scan(&n)
							return err == nil && n == offset
						})
					}
					b.StopTimer()
				})
			}
		})
	}
}
