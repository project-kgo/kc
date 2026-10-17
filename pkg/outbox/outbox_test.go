package outbox

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
)

func TestMessageJSONRoundTrip(t *testing.T) {
	type body struct {
		ID       int64               `json:"id"`
		Name     string              `json:"name"`
		Children []map[string]string `json:"children"`
	}
	value := body{9223372036854775807, "中文🙂", []map[string]string{{"nested": "value"}}}
	m, err := NewMessage("created", value)
	if err != nil {
		t.Fatal(err)
	}
	var decoded body
	if err = m.Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(value, decoded) {
		t.Fatalf("JSON 精度或内容变化: %#v", decoded)
	}
	for _, value := range []any{nil, "字符串", []int{1, 2, 3}, map[string]any{"null": nil}} {
		m, err := NewMessage("test", value)
		if err != nil {
			t.Fatal(err)
		}
		var dst any
		if err = m.Decode(&dst); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = NewMessage("bad", make(chan int)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("编码错误: %v", err)
	}
	if _, err = NewMessage(" ", value); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("类型校验: %v", err)
	}
	for _, payload := range []string{`{"id":`, `{"id":"wrong"}`} {
		if err = (Message{Payload: payload}).Decode(&decoded); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("解码错误: %v", err)
		}
	}
	if err = m.Decode(nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
}

func TestConfigAndSchema(t *testing.T) {
	c, err := resolveConfig(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if c.retries != 20 || c.Retention != 24*time.Hour || c.WorkerCount != 4 {
		t.Fatalf("默认配置不符: %+v", c)
	}
	zero := 0
	c, err = resolveConfig(Config{MaxRetries: &zero})
	if err != nil || c.retries != 0 {
		t.Fatalf("禁止重试: %+v %v", c, err)
	}
	pg := DefaultPostgresOptions()
	pg.VacuumScaleFactor = math.NaN()
	for _, config := range []Config{
		{TableSuffix: "x;DROP TABLE"}, {TableSuffix: strings.Repeat("a", 50)}, {SchemaName: "a.b"},
		{WorkerCount: -1}, {ScanInterval: -1}, {HandlerTimeout: time.Minute}, {HeartbeatInterval: 11 * time.Second},
		{RetryMin: time.Hour}, {Postgres: &pg},
	} {
		if _, err := resolveConfig(config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("无效配置未拒绝: %+v %v", config, err)
		}
	}
	c, _ = resolveConfig(Config{TableSuffix: "orders"})
	for _, name := range []string{"pgx", "mysql"} {
		db := sqlx.NewDb(&sql.DB{}, name)
		s, err := newSQLStorage(db, c)
		if err != nil {
			t.Fatal(err)
		}
		s.schema = "app"
		ddl := s.tableDDL(s.messages)
		if name == "pgx" {
			for _, want := range []string{"JSONB NOT NULL", "fillfactor=90", "vacuum_truncate=false", "toast.autovacuum_vacuum_scale_factor=0.01"} {
				if !strings.Contains(ddl, want) {
					t.Fatalf("缺少 %s: %s", want, ddl)
				}
			}
			if got := s.quote(`a"b`); got != `"a""b"` {
				t.Fatal(got)
			}
		} else if !strings.Contains(ddl, "JSON NOT NULL") || !strings.Contains(ddl, "ENGINE=InnoDB") {
			t.Fatal(ddl)
		}
		for _, idx := range s.indexes() {
			if strings.Contains(s.indexDDL(idx), "payload") {
				t.Fatal("载荷不应建立调度索引")
			}
		}
	}
}

func TestAssignmentBalancedAndDeterministic(t *testing.T) {
	for _, n := range []int{1, 2, 3, 7, 64, 70} {
		nodes := make([]string, n)
		for i := range nodes {
			nodes[i] = fmt.Sprintf("node-%03d", n-i)
		}
		seen := make(map[int]bool)
		minimum, maximum := shardCount, 0
		for _, node := range nodes {
			shards := assignment(nodes, node)
			minimum = min(minimum, len(shards))
			maximum = max(maximum, len(shards))
			for _, id := range shards {
				if seen[id] {
					t.Fatalf("分片 %d 重复分配", id)
				}
				seen[id] = true
			}
		}
		if len(seen) != shardCount || maximum-minimum > 1 {
			t.Fatalf("分配不均: %d %d %d", len(seen), minimum, maximum)
		}
	}
	if got := assignment([]string{"a"}, "absent"); len(got) != 0 {
		t.Fatal(got)
	}
}

func TestDispatchConcurrencyAndSingleDelivery(t *testing.T) {
	var running, peak, calls atomic.Int32
	handler := func(ctx context.Context, m *Message) error {
		n := running.Add(1)
		defer running.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		calls.Add(1)
		time.Sleep(time.Millisecond)
		m.ID = -1
		m.Attempts = 999
		return nil
	}
	o, store := testOutbox(t, Config{WorkerCount: 3}, handler)
	for i := int64(1); i <= 30; i++ {
		store.rows[i] = testTask(i)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := int64(1); i <= 30; i++ {
				_, err := o.Dispatch(context.Background(), i)
				if err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 30 || peak.Load() > 3 || peak.Load() < 2 {
		t.Fatalf("回调次数/并发上限不符: %d %d", calls.Load(), peak.Load())
	}
	for i := int64(1); i <= 30; i++ {
		record, err := o.Get(context.Background(), i)
		if err != nil || record.Status != StatusSucceeded || record.Attempts != 1 {
			t.Fatalf("确认使用了业务修改的消息: %+v %v", record, err)
		}
	}
}

func TestDeliveryFailureAndResultErrors(t *testing.T) {
	businessErr := errors.New("downstream unavailable")
	for _, test := range []struct {
		name    string
		handler Handler
		want    error
	}{
		{"error", func(context.Context, *Message) error { return businessErr }, businessErr},
		{"panic", func(context.Context, *Message) error { panic("business panic") }, ErrHandlerPanic},
		{"timeout", func(ctx context.Context, _ *Message) error { <-ctx.Done(); return nil }, context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			o, store := testOutbox(t, Config{HandlerTimeout: 5 * time.Millisecond}, test.handler)
			store.rows[1] = testTask(1)
			results, err := o.Dispatch(context.Background(), 1)
			if err != nil || !errors.Is(results[0].AttemptError, test.want) || results[0].Status != StatusPending {
				t.Fatalf("处理结果: %+v %v", results, err)
			}
			store.mu.Lock()
			store.rows[1] = testTask(1)
			store.finishErr = ErrLeaseLost
			store.mu.Unlock()
			results, err = o.Dispatch(context.Background(), 1)
			if err != nil || !errors.Is(results[0].AttemptError, ErrLeaseLost) {
				t.Fatalf("旧令牌更新结果: %+v %v", results, err)
			}
		})
	}
}

func TestCloseCancelsHandlerAndWaitsForFinalization(t *testing.T) {
	started := make(chan struct{})
	o, store := testOutbox(t, Config{}, func(ctx context.Context, _ *Message) error { close(started); <-ctx.Done(); return ctx.Err() })
	store.rows[1] = testTask(1)
	done := make(chan struct{})
	go func() { defer close(done); _, _ = o.Dispatch(context.Background(), 1) }()
	<-started
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	<-done
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Get(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if !store.released || store.finishCanceled {
		t.Fatal("关闭未等待结果落库，或落库继承了已取消上下文")
	}
}

func TestBackoffCappedAndErrorTruncation(t *testing.T) {
	o, _ := testOutbox(t, Config{}, func(context.Context, *Message) error { return nil })
	for _, attempt := range []int{1, 2, 20, 1000} {
		for range 30 {
			delay := o.backoff(attempt)
			if delay < time.Millisecond || delay > 5*time.Minute {
				t.Fatal(delay)
			}
		}
	}
	text := errorText(errors.New(strings.Repeat("中文🙂", 1000)))
	if len(text) > 4096 || !strings.HasSuffix(text, "文") && !strings.HasSuffix(text, "中") && !strings.HasSuffix(text, "🙂") {
		t.Fatal("错误截断破坏了 UTF-8")
	}
}

func TestTransactionCommitBoundaryAndPoison(t *testing.T) {
	for _, mode := range []string{"success", "rollback", "commit-error", "ignored-enqueue-error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			state := &txDriverState{}
			if mode == "commit-error" {
				state.commitErr = errors.New("uncertain commit")
			}
			db := openTxDB(t, state)
			var calls atomic.Int32
			o, store := testOutbox(t, Config{}, func(context.Context, *Message) error {
				if state.commits.Load() != 1 {
					t.Error("提交前执行了回调")
				}
				calls.Add(1)
				return errors.New("delivery failed after commit")
			})
			o.db = db
			if mode == "ignored-enqueue-error" {
				store.insertErr = errors.New("invalid JSON")
			}
			var bound *Tx
			execute := func() {
				results, err := o.Transaction(context.Background(), func(tx *Tx) error {
					bound = tx
					_, enqueueErr := tx.Enqueue(context.Background(), Message{Type: "test", Payload: `{"n":1}`})
					if mode == "ignored-enqueue-error" {
						return nil
					}
					if enqueueErr != nil {
						return enqueueErr
					}
					if mode == "rollback" {
						return errors.New("business rollback")
					}
					if mode == "panic" {
						panic("rollback panic")
					}
					return nil
				})
				if mode == "success" {
					if err != nil || len(results) != 1 || results[0].AttemptError == nil {
						t.Fatalf("提交后失败不应返回事务错误: %+v %v", results, err)
					}
				} else if err == nil {
					t.Fatal("应返回提交前/提交错误")
				}
			}
			if mode == "panic" {
				func() {
					defer func() {
						if recover() != "rollback panic" {
							t.Error("panic 未原样传播")
						}
					}()
					execute()
				}()
			} else {
				execute()
			}
			if mode == "success" {
				if calls.Load() != 1 {
					t.Fatal("首次处理未执行")
				}
			} else if calls.Load() != 0 {
				t.Fatal("失败事务执行了回调")
			}
			if mode != "success" && mode != "commit-error" && state.rollbacks.Load() != 1 {
				t.Fatal("事务未回滚")
			}
			if bound != nil {
				if _, err := bound.Enqueue(context.Background(), Message{}); !errors.Is(err, ErrInvalidArgument) {
					t.Fatal("事务回调结束后仍可入队")
				}
			}
		})
	}
}

func TestManualScanAndRetryBudget(t *testing.T) {
	zero := 0
	o, store := testOutbox(t, Config{MaxRetries: &zero}, func(context.Context, *Message) error { return errors.New("fail") })
	store.rows[1] = testTask(1)
	if err := o.ScanOnce(context.Background()); err == nil {
		t.Fatal("手动扫描应返回回调错误")
	}
	r, _ := o.Get(context.Background(), 1)
	if r.Status != StatusFailed || r.Attempts != 1 {
		t.Fatal(r)
	}
	if err := o.Retry(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	r, _ = o.Get(context.Background(), 1)
	if r.Status != StatusPending || r.Attempts != 0 {
		t.Fatal(r)
	}
	if err := o.Retry(context.Background(), 1); !errors.Is(err, ErrNotFailed) {
		t.Fatal(err)
	}
}

func testTask(id int64) task {
	return task{Record: Record{Message: Message{ID: id, Type: "test", Payload: `{"id":9223372036854775807}`}, ShardID: int(id % shardCount), Status: StatusPending}}
}

type memoryStorage struct {
	mu                       sync.Mutex
	rows                     map[int64]task
	config                   resolvedConfig
	insertErr, finishErr     error
	released, finishCanceled bool
}

func testOutbox(t testing.TB, config Config, handler Handler) (*Outbox, *memoryStorage) {
	t.Helper()
	config.DisableBackground = true
	config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	c, err := resolveConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	store := &memoryStorage{rows: make(map[int64]task), config: c}
	o := &Outbox{store: store, config: c, handlers: map[string]Handler{"test": handler}, nodeID: "test-node", ctx: ctx, cancel: cancel, slots: make(chan struct{}, c.WorkerCount), wake: make(chan struct{}, 1), jobs: make(chan task, c.WorkerCount), closeDone: make(chan struct{})}
	t.Cleanup(func() {
		if err := o.Close(); err != nil {
			t.Error(err)
		}
	})
	return o, store
}

func (s *memoryStorage) insert(_ context.Context, _ *sqlx.Tx, m Message) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.insertErr != nil {
		return 0, s.insertErr
	}
	id := int64(len(s.rows) + 1)
	r := testTask(id)
	r.Type = m.Type
	r.Payload = m.Payload
	s.rows[id] = r
	return id, nil
}
func (s *memoryStorage) get(ctx context.Context, id int64) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	r, ok := s.rows[id]
	if !ok {
		return Record{}, ErrNotFound
	}
	return r.Record, nil
}
func (s *memoryStorage) claimIDs(ctx context.Context, ids []int64, limit int) ([]task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var tasks []task
	for _, id := range ids {
		r, ok := s.rows[id]
		if !ok || r.Status != StatusPending || r.NextAttemptAt > time.Now().UnixMilli() {
			continue
		}
		r.Status = StatusProcessing
		r.Attempts++
		r.LeaseToken = fmt.Sprintf("%d-%d", id, r.Attempts)
		s.rows[id] = r
		tasks = append(tasks, r)
		if len(tasks) == limit {
			break
		}
	}
	return tasks, nil
}
func (s *memoryStorage) claimShard(ctx context.Context, lease shardLease, limit int) ([]task, error) {
	s.mu.Lock()
	var ids []int64
	for id, r := range s.rows {
		if r.ShardID == lease.ShardID {
			ids = append(ids, id)
		}
	}
	s.mu.Unlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return s.claimIDs(ctx, ids, limit)
}
func (s *memoryStorage) readyShards(_ context.Context, leases []shardLease) ([]shardLease, error) {
	return leases, nil
}
func (s *memoryStorage) finish(ctx context.Context, t task, handlerErr error, delay time.Duration) (Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finishCanceled = ctx.Err() != nil
	if s.finishErr != nil {
		return StatusProcessing, s.finishErr
	}
	r := s.rows[t.ID]
	r.Status = StatusSucceeded
	if handlerErr != nil {
		r.Status = StatusPending
		r.NextAttemptAt = time.Now().Add(delay).UnixMilli()
		if r.Attempts >= s.config.retries+1 {
			r.Status = StatusFailed
		}
	}
	s.rows[t.ID] = r
	return r.Status, nil
}
func (s *memoryStorage) retry(_ context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[id]
	if !ok {
		return ErrNotFound
	}
	if r.Status != StatusFailed {
		return ErrNotFailed
	}
	r.Status = StatusPending
	r.Attempts = 0
	r.NextAttemptAt = 0
	s.rows[id] = r
	return nil
}
func (s *memoryStorage) coordinate(_ context.Context, node string) ([]shardLease, error) {
	leases := make([]shardLease, shardCount)
	for i := range leases {
		leases[i] = shardLease{ShardID: i, OwnerID: node, Token: "token"}
	}
	return leases, nil
}
func (s *memoryStorage) cleanup(context.Context, shardLease) (int64, error) { return 0, nil }
func (s *memoryStorage) release(context.Context, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released = true
	return nil
}

type txDriverState struct {
	commits, rollbacks atomic.Int32
	commitErr          error
}
type txDriver struct{ state *txDriverState }
type txConn struct{ state *txDriverState }
type txDriverTx struct{ state *txDriverState }

var testDriverNumber atomic.Int64

func (d txDriver) Open(string) (driver.Conn, error)   { return &txConn{d.state}, nil }
func (c *txConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (c *txConn) Close() error                        { return nil }
func (c *txConn) Begin() (driver.Tx, error)           { return &txDriverTx{c.state}, nil }
func (t *txDriverTx) Commit() error                   { t.state.commits.Add(1); return t.state.commitErr }
func (t *txDriverTx) Rollback() error                 { t.state.rollbacks.Add(1); return nil }
func openTxDB(t *testing.T, state *txDriverState) *sqlx.DB {
	t.Helper()
	name := fmt.Sprintf("outbox-test-%d", testDriverNumber.Add(1))
	sql.Register(name, txDriver{state})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return sqlx.NewDb(db, name)
}

func BenchmarkMessageJSON(b *testing.B) {
	value := struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	}{9223372036854775807, "中文载荷"}
	b.ReportAllocs()
	for b.Loop() {
		m, err := NewMessage("test", value)
		if err != nil {
			b.Fatal(err)
		}
		var dst struct {
			ID   int64  `json:"id"`
			Name string `json:"name"`
		}
		if err = m.Decode(&dst); err != nil {
			b.Fatal(err)
		}
	}
}
