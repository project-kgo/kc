package scheduler

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
	"github.com/redis/go-redis/v9"
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
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)
	t.Cleanup(func() { db.Close() })
	return db
}

func openTestRedis(t testing.TB) redis.UniversalClient {
	t.Helper()
	dsn := os.Getenv("KC_REDIS_TEST_DSN")
	if dsn == "" {
		t.Skip("跳过 Redis 集成测试：未设置 KC_REDIS_TEST_DSN")
	}
	client, err := newTestRedisClient(dsn)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// newTestRedisClient 按 DSN 选择客户端：带 addr 参数的是 Cluster DSN，其余是单机 DSN。
// 这样同一套端到端用例既能跑在单机上，也能跑在 Cluster 上。
func newTestRedisClient(dsn string) (redis.UniversalClient, error) {
	if strings.Contains(dsn, "addr=") {
		options, err := redis.ParseClusterURL(dsn)
		if err != nil {
			return nil, err
		}
		return redis.NewClusterClient(options), nil
	}
	options, err := redis.ParseURL(dsn)
	if err != nil {
		return nil, err
	}
	return redis.NewClient(options), nil
}

// fastConfig 缩短所有间隔，使重试、租约回收和对账在测试中快速发生。
// VolatileBelow 默认为 -1，让测试显式控制哪些任务走易失路径。
func fastConfig(suffix string) Config {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("TEST_SCHEDULER_LOG") != "" {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return Config{
		TableSuffix: suffix, KeyPrefix: "sched_test_" + suffix, Buckets: 4,
		Window: 2 * time.Second, LoadInterval: 20 * time.Millisecond, LoadBatch: 100,
		PollInterval: 10 * time.Millisecond, WorkerCount: 4,
		HandlerTimeout: 500 * time.Millisecond, LeaseDuration: 2 * time.Second, MaxAttempts: 3,
		RetryBackoffMin: 20 * time.Millisecond, RetryBackoffMax: 200 * time.Millisecond,
		ReclaimInterval: 50 * time.Millisecond, ReconcileInterval: 50 * time.Millisecond,
		ReconcileGrace: 50 * time.Millisecond, VolatileBelow: -1, VolatileMaxPayload: 4096,
		Retention: time.Hour, CleanupInterval: time.Hour, ShutdownTimeout: 2 * time.Second,
		Logger: logger,
	}
}

type testEnv struct {
	db   *sqlx.DB
	rdb  redis.UniversalClient
	cfg  Config
	name string
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	db := openTestDB(t)
	rdb := openTestRedis(t)
	suffix := "t" + strings.ReplaceAll(uuid.NewString()[:13], "-", "")
	cfg := fastConfig(suffix)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS scheduler_tasks_%s", suffix))
		dropKeys(ctx, rdb, cfg)
	})
	return &testEnv{db: db, rdb: rdb, cfg: cfg, name: suffix}
}

func (e *testEnv) resolved(t *testing.T) Config {
	t.Helper()
	cfg, err := resolveConfig(e.cfg)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func (e *testEnv) queue(t *testing.T) *queue {
	t.Helper()
	q, err := newQueue(e.rdb, e.resolved(t))
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func dropKeys(ctx context.Context, rdb redis.UniversalClient, cfg Config) {
	resolved, err := resolveConfig(cfg)
	if err != nil {
		return
	}
	q, err := newQueue(rdb, resolved)
	if err != nil {
		return
	}
	// 逐桶删除：同一个桶的三个 key 在同一个 slot，跨桶一起删在 Cluster 上会 CROSSSLOT。
	for _, k := range q.keys {
		_, _ = rdb.Del(ctx, k.ready, k.processing, k.data).Result()
	}
}

func newTestScheduler(t *testing.T, env *testEnv, handlers map[string]Handler) *Scheduler {
	return startTestScheduler(t, env, handlers, nil)
}

func startTestScheduler(t *testing.T, env *testEnv, handlers map[string]Handler, wrap func(storage) storage) *Scheduler {
	t.Helper()
	cfg := env.resolved(t)
	reg, err := (*registry)(nil).with(handlers)
	if err != nil {
		t.Fatal(err)
	}
	store, err := newPGStorage(env.db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ensureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	q, err := newQueue(env.rdb, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.load(context.Background()); err != nil {
		t.Fatal(err)
	}
	var st storage = store
	if wrap != nil {
		st = wrap(st)
	}
	s := start(context.Background(), env.db, st, q, cfg, reg)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func waitRecord(t *testing.T, s *Scheduler, id uuid.UUID, want Status, timeout time.Duration) Record {
	t.Helper()
	var record Record
	waitFor(t, timeout, "status "+want.String(), func() bool {
		got, err := s.Get(context.Background(), id)
		if err != nil {
			return false
		}
		record = got
		return got.Status == want
	})
	return record
}

func countRows(t *testing.T, db *sqlx.DB, suffix string, id uuid.UUID) int {
	t.Helper()
	var count int
	if err := db.Get(&count, "SELECT count(*) FROM scheduler_tasks_"+suffix+" WHERE id = $1", id); err != nil {
		t.Fatal(err)
	}
	return count
}

func newID(t *testing.T) uuid.UUID {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func addTask(t *testing.T, s *Scheduler, task Task) uuid.UUID {
	t.Helper()
	id, err := s.Add(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestNearTaskExecutesAfterAdd(t *testing.T) {
	env := newTestEnv(t)
	var calls atomic.Int64
	done := make(chan uuid.UUID, 1)
	s := newTestScheduler(t, env, map[string]Handler{"near": func(_ context.Context, task *Task) error {
		calls.Add(1)
		done <- task.ID
		return nil
	}})
	task, err := NewTask("near", map[string]int{"v": 1})
	if err != nil {
		t.Fatal(err)
	}
	id := addTask(t, s, task)
	select {
	case got := <-done:
		if got != id {
			t.Fatalf("executed task %s, want %s", got, id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("near task did not execute in time")
	}
	record := waitRecord(t, s, id, StatusSucceeded, 3*time.Second)
	if calls.Load() != 1 || record.Attempts != 0 {
		t.Fatalf("calls = %d, attempts = %d", calls.Load(), record.Attempts)
	}
}

func TestFarTaskExecutesWhenLoaded(t *testing.T) {
	env := newTestEnv(t)
	env.cfg.Window = 300 * time.Millisecond
	done := make(chan struct{}, 1)
	s := newTestScheduler(t, env, map[string]Handler{"far": func(context.Context, *Task) error {
		done <- struct{}{}
		return nil
	}})
	task, _ := NewTask("far", nil)
	task.RunAt = time.Now().Add(700 * time.Millisecond)
	id := addTask(t, s, task)
	if record, err := s.Get(context.Background(), id); err != nil || record.Status != StatusPending {
		t.Fatalf("far task status = %+v, %v, want pending", record, err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("far task did not execute after entering the window")
	}
	waitRecord(t, s, id, StatusSucceeded, 3*time.Second)
}

func TestAddTxAndTransaction(t *testing.T) {
	env := newTestEnv(t)
	var calls atomic.Int64
	done := make(chan struct{}, 4)
	s := newTestScheduler(t, env, map[string]Handler{"tx": func(context.Context, *Task) error {
		calls.Add(1)
		done <- struct{}{}
		return nil
	}})

	task, _ := NewTask("tx", nil)
	sqlTx, err := env.db.Beginx()
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.AddTx(context.Background(), sqlTx, task)
	if err != nil {
		t.Fatal(err)
	}
	if err = sqlTx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("AddTx task did not execute")
	}
	waitRecord(t, s, id, StatusSucceeded, 3*time.Second)

	inside, _ := NewTask("tx", nil)
	var transactionID uuid.UUID
	if err = s.Transaction(context.Background(), func(tx *Tx) error {
		transactionID, err = tx.Add(context.Background(), inside)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Transaction task did not execute")
	}
	waitRecord(t, s, transactionID, StatusSucceeded, 3*time.Second)

	rolledBack, _ := NewTask("tx", nil)
	var rolledBackID uuid.UUID
	if err = s.Transaction(context.Background(), func(tx *Tx) error {
		var addErr error
		rolledBackID, addErr = tx.Add(context.Background(), rolledBack)
		if addErr != nil {
			t.Fatal(addErr)
		}
		return errors.New("business failure")
	}); err == nil {
		t.Fatal("transaction error must be returned")
	}
	if countRows(t, env.db, env.name, rolledBackID) != 0 {
		t.Fatal("rolled back task leaked into PostgreSQL")
	}
	if _, err = s.Get(context.Background(), rolledBackID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rolled back task Get = %v", err)
	}
	stats, err := s.Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.ReadyDue+stats.Processing != 0 {
		t.Fatalf("rolled back task reached Redis: %+v", stats)
	}
}

func TestRetryBackoffExhaustionAndManualRetry(t *testing.T) {
	env := newTestEnv(t)
	env.cfg.MaxAttempts = 2
	env.cfg.RetryBackoffMin = 20 * time.Millisecond
	env.cfg.RetryBackoffMax = 50 * time.Millisecond
	var failing atomic.Bool
	failing.Store(true)
	done := make(chan struct{}, 1)
	s := newTestScheduler(t, env, map[string]Handler{"retry": func(context.Context, *Task) error {
		if failing.Load() {
			return errors.New("downstream unavailable")
		}
		done <- struct{}{}
		return nil
	}})
	task, _ := NewTask("retry", nil)
	id := addTask(t, s, task)
	record := waitRecord(t, s, id, StatusFailed, 5*time.Second)
	if record.Attempts != 2 || record.LastError == "" || record.CompletedAt.IsZero() {
		t.Fatalf("failed record = %+v", record)
	}
	failing.Store(false)
	if err := s.Retry(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("retried task did not execute")
	}
	waitRecord(t, s, id, StatusSucceeded, 3*time.Second)
}

func TestCancelQueuedAndVolatileTasks(t *testing.T) {
	env := newTestEnv(t)
	var calls atomic.Int64
	s := newTestScheduler(t, env, map[string]Handler{"cancel": func(context.Context, *Task) error {
		calls.Add(1)
		return nil
	}})
	task, _ := NewTask("cancel", nil)
	task.RunAt = time.Now().Add(500 * time.Millisecond)
	id := addTask(t, s, task)
	if err := s.Cancel(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	record := waitRecord(t, s, id, StatusCancelled, time.Second)
	if record.CompletedAt.IsZero() {
		t.Fatal("cancelled task must have completed_at")
	}
	if err := s.Cancel(context.Background(), id); !errors.Is(err, ErrFinished) {
		t.Fatalf("cancel finished task = %v, want ErrFinished", err)
	}

	volatile, _ := NewTask("cancel", nil)
	volatile.Volatile = true
	volatile.RunAt = time.Now().Add(500 * time.Millisecond)
	volatileID := addTask(t, s, volatile)
	if err := s.Cancel(context.Background(), volatileID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(context.Background(), volatileID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("volatile Get = %v", err)
	}
	q := env.queue(t)
	key := q.keys[q.bucket(volatileID)].data
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if got, err := env.rdb.HGet(ctx, key, string(volatileID[:])).Result(); err != redis.Nil || got != "" {
		t.Fatalf("volatile data after cancel = %q, %v", got, err)
	}
	if err := s.Cancel(context.Background(), uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cancel unknown task = %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("cancelled tasks executed %d times", calls.Load())
	}
}

func TestReconcileRestoresRedisAfterLoss(t *testing.T) {
	env := newTestEnv(t)
	done := make(chan struct{}, 1)
	s := newTestScheduler(t, env, map[string]Handler{"reconcile": func(context.Context, *Task) error {
		done <- struct{}{}
		return nil
	}})
	task, _ := NewTask("reconcile", nil)
	task.RunAt = time.Now().Add(300 * time.Millisecond)
	id := addTask(t, s, task)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// 模拟 Redis 丢失全部数据。
	dropKeys(ctx, env.rdb, env.cfg)
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("task was not restored by reconciler")
	}
	waitRecord(t, s, id, StatusSucceeded, 3*time.Second)
}

// faultyStore 在指定步骤注入错误，模拟进程在两步之间被强杀。
type faultyStore struct {
	storage
	mu            sync.Mutex
	loadFailures  int
	fetchFailures int
}

func (f *faultyStore) load(ctx context.Context, limit int) ([]queuedTask, error) {
	tasks, err := f.storage.load(ctx, limit)
	if err != nil {
		return tasks, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loadFailures > 0 {
		f.loadFailures--
		return nil, errors.New("injected loader crash after PostgreSQL update")
	}
	return tasks, nil
}

func (f *faultyStore) fetch(ctx context.Context, ids []uuid.UUID) ([]storedTask, error) {
	f.mu.Lock()
	fail := f.fetchFailures > 0
	if fail {
		f.fetchFailures--
	}
	f.mu.Unlock()
	if fail {
		return nil, errors.New("injected PostgreSQL read failure")
	}
	return f.storage.fetch(ctx, ids)
}

func TestLoaderCrashRecoveredByReconciler(t *testing.T) {
	env := newTestEnv(t)
	done := make(chan struct{}, 1)
	faults := &faultyStore{loadFailures: 1}
	s := startTestScheduler(t, env, map[string]Handler{"loaded": func(context.Context, *Task) error {
		done <- struct{}{}
		return nil
	}}, func(store storage) storage {
		faults.storage = store
		return faults
	})
	task, _ := NewTask("loaded", nil)
	sqlTx, err := env.db.Beginx()
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.AddTx(context.Background(), sqlTx, task)
	if err != nil {
		t.Fatal(err)
	}
	if err = sqlTx.Commit(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, "loader to mark the task queued", func() bool {
		record, err := s.Get(context.Background(), id)
		return err == nil && record.Status == StatusQueued
	})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reconciler did not recover the task after loader crashed")
	}
	waitRecord(t, s, id, StatusSucceeded, 3*time.Second)
}

func TestFetchFailureRequeuesWithoutCountingAttempt(t *testing.T) {
	env := newTestEnv(t)
	done := make(chan struct{}, 1)
	faults := &faultyStore{fetchFailures: 1}
	s := startTestScheduler(t, env, map[string]Handler{"fetch": func(context.Context, *Task) error {
		done <- struct{}{}
		return nil
	}}, func(store storage) storage {
		faults.storage = store
		return faults
	})
	task, _ := NewTask("fetch", nil)
	id := addTask(t, s, task)
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("task was not executed after the injected read failure")
	}
	record := waitRecord(t, s, id, StatusSucceeded, 3*time.Second)
	if record.Attempts != 0 {
		t.Fatalf("read failure counted as an attempt: %+v", record)
	}
}

func TestLeaseTokenValidation(t *testing.T) {
	env := newTestEnv(t)
	q := env.queue(t)
	ctx := context.Background()
	id := newID(t)
	if _, err := q.enqueue(ctx, []queuedTask{{id: id, runAt: time.Now().Add(-time.Second)}}); err != nil {
		t.Fatal(err)
	}
	if n, err := q.enqueue(ctx, []queuedTask{{id: id, runAt: time.Now()}}); err != nil || n != 0 {
		t.Fatalf("duplicate enqueue = %d, %v", n, err)
	}
	first, err := q.pop(ctx, q.bucket(id), 10, 100*time.Millisecond)
	if err != nil || len(first.items) != 1 || first.items[0].id != id {
		t.Fatalf("pop = %+v, %v", first, err)
	}
	if again, err := q.pop(ctx, q.bucket(id), 10, time.Second); err != nil || len(again.items) != 0 {
		t.Fatalf("second pop = %+v, %v", again, err)
	}
	time.Sleep(150 * time.Millisecond)
	second, err := q.reclaim(ctx, q.bucket(id), 10)
	if err != nil || len(second.items) != 1 || second.items[0].id != id {
		t.Fatalf("reclaim = %+v, %v", second, err)
	}
	if n, err := q.ack(ctx, q.bucket(id), first.token, []uuid.UUID{id}); err != nil || n != 0 {
		t.Fatalf("ack with stale token = %d, %v", n, err)
	}
	if n, err := q.requeue(ctx, q.bucket(id), first.token, 0, []requeueItem{{id: id}}); err != nil || n != 0 {
		t.Fatalf("requeue with stale token = %d, %v", n, err)
	}
	if n, err := q.requeue(ctx, q.bucket(id), second.token, 0, []requeueItem{{id: id}}); err != nil || n != 1 {
		t.Fatalf("requeue with current token = %d, %v", n, err)
	}
	third, err := q.pop(ctx, q.bucket(id), 10, time.Second)
	if err != nil || len(third.items) != 1 {
		t.Fatalf("pop after requeue = %+v, %v", third, err)
	}
	if n, err := q.ack(ctx, q.bucket(id), third.token, []uuid.UUID{id}); err != nil || n != 1 {
		t.Fatalf("final ack = %d, %v", n, err)
	}
}

func TestReclaimerIncrementsAttempts(t *testing.T) {
	env := newTestEnv(t)
	cfg := env.resolved(t)
	cfg.LeaseDuration = 200 * time.Millisecond
	cfg.HandlerTimeout = 100 * time.Millisecond
	store, err := newPGStorage(env.db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ensureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	q, err := newQueue(env.rdb, cfg)
	if err != nil {
		t.Fatal(err)
	}
	task, _ := NewTask("reclaim", nil)
	task.ID = newID(t)
	queued, err := store.insert(context.Background(), env.db, []insertTask{{task: task, status: StatusQueued}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = q.enqueue(context.Background(), queued); err != nil {
		t.Fatal(err)
	}
	if _, err = q.pop(context.Background(), q.bucket(task.ID), 10, cfg.LeaseDuration); err != nil {
		t.Fatal(err)
	}
	lifecycle, cancel := context.WithCancel(context.Background())
	s := &Scheduler{
		db: env.db, store: store, queue: q, config: cfg, ctx: lifecycle, cancel: cancel, handlerCtx: lifecycle, handlerCancel: cancel,
		slots: make(chan struct{}, 1), jobs: make(chan workItem, 1),
	}
	defer s.Close()
	time.Sleep(250 * time.Millisecond)
	s.reclaim()
	record, err := s.Get(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Attempts != 1 || record.Status != StatusQueued {
		t.Fatalf("after reclaim = %+v, want attempts 1 and queued", record)
	}
	stats, err := q.stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var ready int64
	for _, keys := range q.keys {
		ready += env.rdb.ZCard(context.Background(), keys.ready).Val()
	}
	if stats.processing != 0 || ready != 1 {
		t.Fatalf("queue after reclaim = %+v, ready = %d", stats, ready)
	}
}

func TestVolatileTaskLifecycle(t *testing.T) {
	env := newTestEnv(t)
	env.cfg.MaxAttempts = 2
	var attempts sync.Map // task ID -> []int
	done := make(chan uuid.UUID, 8)
	s := newTestScheduler(t, env, map[string]Handler{
		"volatile.ok": func(_ context.Context, task *Task) error {
			done <- task.ID
			return nil
		},
		"volatile.retry": func(_ context.Context, task *Task) error {
			values, _ := attempts.LoadOrStore(task.ID, &[]int{})
			list := values.(*[]int)
			*list = append(*list, task.Attempts)
			if task.Attempts == 0 {
				return errors.New("first attempt fails")
			}
			done <- task.ID
			return nil
		},
		"volatile.drop": func(context.Context, *Task) error {
			return errors.New("always fails")
		},
	})
	q := env.queue(t)
	dataKey := func(id uuid.UUID) string { return q.keys[q.bucket(id)].data }

	okTask, _ := NewTask("volatile.ok", nil)
	okTask.Volatile = true
	okID := addTask(t, s, okTask)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("volatile task did not run")
	}
	if countRows(t, env.db, env.name, okID) != 0 {
		t.Fatal("volatile task reached PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := env.rdb.HGet(ctx, dataKey(okID), string(okID[:])).Result(); err != redis.Nil {
		t.Fatalf("volatile data after success = %v", err)
	}

	retryTask, _ := NewTask("volatile.retry", nil)
	retryTask.Volatile = true
	retryID := addTask(t, s, retryTask)
	select {
	case got := <-done:
		if got != retryID {
			t.Fatalf("retried task = %s, want %s", got, retryID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("volatile retry did not succeed")
	}
	values, _ := attempts.Load(retryID)
	if list := *values.(*[]int); len(list) != 2 || list[0] != 0 || list[1] != 1 {
		t.Fatalf("volatile attempts = %v, want [0 1]", list)
	}

	dropTask, _ := NewTask("volatile.drop", nil)
	dropTask.Volatile = true
	dropID := addTask(t, s, dropTask)
	waitFor(t, 5*time.Second, "volatile task to be dropped", func() bool {
		_, err := env.rdb.HGet(ctx, dataKey(dropID), string(dropID[:])).Result()
		return errors.Is(err, redis.Nil)
	})
	if _, err := s.Get(context.Background(), dropID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("dropped volatile Get = %v, want ErrNotFound", err)
	}
}

func TestVolatileBelowAppliesOnlyToAdd(t *testing.T) {
	env := newTestEnv(t)
	env.cfg.VolatileBelow = 500 * time.Millisecond
	s := newTestScheduler(t, env, map[string]Handler{"threshold": func(context.Context, *Task) error { return nil }})

	volatile, _ := NewTask("threshold", nil)
	volatile.RunAt = time.Now().Add(100 * time.Millisecond)
	volatileID := addTask(t, s, volatile)
	if countRows(t, env.db, env.name, volatileID) != 0 {
		t.Fatal("task below VolatileBelow must not reach PostgreSQL")
	}

	durable, _ := NewTask("threshold", nil)
	durable.RunAt = time.Now().Add(100 * time.Millisecond)
	durable.Durable = true
	durableID := addTask(t, s, durable)
	if countRows(t, env.db, env.name, durableID) != 1 {
		t.Fatal("Durable task must stay in PostgreSQL")
	}

	explicit, _ := NewTask("threshold", nil)
	explicit.Volatile = true
	if err := s.Transaction(context.Background(), func(tx *Tx) error {
		_, err := tx.Add(context.Background(), explicit)
		return err
	}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("transaction volatile = %v, want ErrInvalidArgument", err)
	}
	sqlTx, err := env.db.Beginx()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlTx.Rollback()
	if _, err = s.AddTx(context.Background(), sqlTx, explicit); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("AddTx volatile = %v, want ErrInvalidArgument", err)
	}

	transaction, _ := NewTask("threshold", nil)
	transaction.RunAt = time.Now().Add(100 * time.Millisecond)
	var txID uuid.UUID
	if err = s.Transaction(context.Background(), func(tx *Tx) error {
		txID, err = tx.Add(context.Background(), transaction)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if countRows(t, env.db, env.name, txID) != 1 {
		t.Fatal("transaction task must stay in PostgreSQL regardless of threshold")
	}
}

func TestMixedTasksInSameBucket(t *testing.T) {
	env := newTestEnv(t)
	q := env.queue(t)
	persistentID := newID(t)
	volatileID := newID(t)
	for i := 0; i < 1000 && q.bucket(persistentID) != q.bucket(volatileID); i++ {
		volatileID = newID(t)
	}
	if q.bucket(persistentID) != q.bucket(volatileID) {
		t.Fatal("failed to generate two IDs in the same bucket")
	}
	done := make(chan uuid.UUID, 2)
	s := newTestScheduler(t, env, map[string]Handler{"mixed": func(_ context.Context, task *Task) error {
		done <- task.ID
		return nil
	}})
	persistent, _ := NewTask("mixed", nil)
	persistent.ID = persistentID
	if _, err := s.Add(context.Background(), persistent); err != nil {
		t.Fatal(err)
	}
	volatile, _ := NewTask("mixed", nil)
	volatile.ID = volatileID
	volatile.Volatile = true
	if _, err := s.Add(context.Background(), volatile); err != nil {
		t.Fatal(err)
	}
	seen := map[uuid.UUID]bool{}
	for range 2 {
		select {
		case id := <-done:
			seen[id] = true
		case <-time.After(3 * time.Second):
			t.Fatal("mixed bucket tasks did not both execute")
		}
	}
	if !seen[persistentID] || !seen[volatileID] {
		t.Fatalf("executed = %v", seen)
	}
	waitRecord(t, s, persistentID, StatusSucceeded, 3*time.Second)
	if _, err := s.Get(context.Background(), volatileID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("volatile Get = %v, want ErrNotFound", err)
	}
}

func TestCleanupRetention(t *testing.T) {
	env := newTestEnv(t)
	env.cfg.Retention = 100 * time.Millisecond
	env.cfg.CleanupInterval = 50 * time.Millisecond
	env.cfg.MaxAttempts = 1
	okDone := make(chan uuid.UUID, 1)
	badDone := make(chan uuid.UUID, 1)
	s := newTestScheduler(t, env, map[string]Handler{
		"clean.ok": func(_ context.Context, task *Task) error {
			okDone <- task.ID
			return nil
		},
		"clean.bad": func(_ context.Context, task *Task) error {
			badDone <- task.ID
			return errors.New("fail")
		},
	})
	okTask, _ := NewTask("clean.ok", nil)
	okID := addTask(t, s, okTask)
	badTask, _ := NewTask("clean.bad", nil)
	badID := addTask(t, s, badTask)
	select {
	case <-okDone:
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup success task did not run")
	}
	select {
	case <-badDone:
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup failure task did not run")
	}
	waitRecord(t, s, badID, StatusFailed, 3*time.Second)
	waitFor(t, 3*time.Second, "succeeded task to be cleaned", func() bool {
		_, err := s.Get(context.Background(), okID)
		return errors.Is(err, ErrNotFound)
	})
	time.Sleep(300 * time.Millisecond)
	if _, err := s.Get(context.Background(), badID); err != nil {
		t.Fatalf("failed task must be retained by default: %v", err)
	}
}

func TestUnregisteredHandlerIsRequeued(t *testing.T) {
	env := newTestEnv(t)
	cfg := env.resolved(t)
	store, err := newPGStorage(env.db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ensureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	q := env.queue(t)
	task, _ := NewTask("missing", nil)
	task.ID = newID(t)
	queued, err := store.insert(context.Background(), env.db, []insertTask{{task: task, status: StatusQueued}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = q.enqueue(context.Background(), queued); err != nil {
		t.Fatal(err)
	}
	id := task.ID

	consumer := newTestScheduler(t, env, map[string]Handler{})
	time.Sleep(300 * time.Millisecond)
	record, err := consumer.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if record.Attempts != 0 || record.Status != StatusQueued {
		t.Fatalf("after missing handler = %+v, want queued with 0 attempts", record)
	}
	done := make(chan struct{}, 1)
	if err = consumer.Register("missing", func(context.Context, *Task) error {
		done <- struct{}{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("task was not executed after handler registration")
	}
	record = waitRecord(t, consumer, id, StatusSucceeded, 3*time.Second)
	if record.Attempts != 0 {
		t.Fatalf("missing handler consumed attempts: %+v", record)
	}
}

func TestMultiNodeExecutesEachTaskOnce(t *testing.T) {
	env := newTestEnv(t)
	var mu sync.Mutex
	counts := map[uuid.UUID]int{}
	handler := func(_ context.Context, task *Task) error {
		mu.Lock()
		counts[task.ID]++
		mu.Unlock()
		return nil
	}
	first := newTestScheduler(t, env, map[string]Handler{"multi": handler})
	newTestScheduler(t, env, map[string]Handler{"multi": handler})
	const total = 20
	ids := make([]uuid.UUID, total)
	for i := range ids {
		task, _ := NewTask("multi", map[string]int{"i": i})
		ids[i] = addTask(t, first, task)
	}
	waitFor(t, 5*time.Second, "all tasks to execute", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(counts) == total
	})
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	for _, id := range ids {
		if counts[id] != 1 {
			t.Fatalf("task %s executed %d times", id, counts[id])
		}
	}
}

// TestQueueScriptsIntegration 覆盖 Lua 脚本在单机和 Cluster 上的行为。
func TestQueueScriptsIntegration(t *testing.T) {
	tests := []struct {
		name string
		env  string
	}{
		{name: "standalone", env: "KC_REDIS_TEST_DSN"},
		{name: "cluster", env: "KC_REDIS_CLUSTER_TEST_DSN"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dsn := os.Getenv(test.env)
			if dsn == "" {
				t.Skipf("%s is not set", test.env)
			}
			client, err := newTestRedisClient(dsn)
			if err != nil {
				t.Fatal(err)
			}
			testQueueScripts(t, client)
		})
	}
}

func testQueueScripts(t *testing.T, client redis.UniversalClient) {
	t.Helper()
	suffix := "t" + strings.ReplaceAll(uuid.NewString()[:13], "-", "")
	cfg, err := resolveConfig(Config{TableSuffix: suffix, KeyPrefix: "sched_test_" + suffix, Buckets: 4})
	if err != nil {
		t.Fatal(err)
	}
	q, err := newQueue(client, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Cleanup(func() { _ = client.Close() })
	t.Cleanup(func() { dropKeys(context.Background(), client, cfg) })
	if err = q.load(ctx); err != nil {
		t.Fatal(err)
	}
	id := newID(t)
	runAt := time.Now().Add(-time.Second)
	if n, err := q.enqueue(ctx, []queuedTask{{id: id, runAt: runAt}}); err != nil || n != 1 {
		t.Fatalf("enqueue = %d, %v", n, err)
	}
	if n, err := q.enqueue(ctx, []queuedTask{{id: id, runAt: runAt}}); err != nil || n != 0 {
		t.Fatalf("duplicate enqueue = %d, %v", n, err)
	}
	batch, err := q.pop(ctx, q.bucket(id), 10, 100*time.Millisecond)
	if err != nil || len(batch.items) != 1 || batch.items[0].id != id {
		t.Fatalf("pop = %+v, %v", batch, err)
	}
	if batch.items[0].data != "" {
		t.Fatalf("persistent task should not carry data: %q", batch.items[0].data)
	}
	if n, err := q.ack(ctx, q.bucket(id), batch.token, []uuid.UUID{id}); err != nil || n != 1 {
		t.Fatalf("ack = %d, %v", n, err)
	}
	if _, err = q.ack(ctx, q.bucket(id), batch.token, []uuid.UUID{id}); err != nil {
		t.Fatalf("second ack = %v", err)
	}

	volatileID := newID(t)
	if err = q.enqueueVolatile(ctx, volatileID, 0, `{"type":"x","payload":"","attempts":0}`); err != nil {
		t.Fatal(err)
	}
	volatileBatch, err := q.pop(ctx, q.bucket(volatileID), 10, time.Second)
	if err != nil || len(volatileBatch.items) != 1 || volatileBatch.items[0].data == "" {
		t.Fatalf("volatile pop = %+v, %v", volatileBatch, err)
	}
	if n, err := q.cancel(ctx, volatileID); err != nil || n {
		t.Fatalf("cancel volatile while processing = %v, %v", n, err)
	}
	if n, err := q.ack(ctx, q.bucket(volatileID), volatileBatch.token, []uuid.UUID{volatileID}); err != nil || n != 1 {
		t.Fatalf("ack volatile = %d, %v", n, err)
	}
	if _, err = client.HGet(ctx, q.keys[q.bucket(volatileID)].data, string(volatileID[:])).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("volatile data after ack = %v", err)
	}
}

func TestAddSucceedsWhenRedisUnavailable(t *testing.T) {
	env := newTestEnv(t)
	cfg := env.resolved(t)
	store, err := newPGStorage(env.db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ensureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	bad := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:1", DialTimeout: 20 * time.Millisecond,
		ReadTimeout: 20 * time.Millisecond, WriteTimeout: 20 * time.Millisecond, MaxRetries: -1,
	})
	defer bad.Close()
	q, err := newQueue(bad, cfg)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle, cancel := context.WithCancel(context.Background())
	s := &Scheduler{db: env.db, store: store, queue: q, config: cfg, ctx: lifecycle, cancel: cancel, handlerCtx: lifecycle, handlerCancel: cancel}
	s.handlers.Store(&registry{handlers: map[string]Handler{"offline": func(context.Context, *Task) error { return nil }}})
	// 登记不依赖 Redis：任务以 queued 写入 PostgreSQL，入队失败只记录日志。
	task, _ := NewTask("offline", nil)
	id, err := s.Add(context.Background(), task)
	if err != nil {
		t.Fatalf("Add with Redis down = %v", err)
	}
	record, err := s.Get(context.Background(), id)
	if err != nil || record.Status != StatusQueued {
		t.Fatalf("record after offline Add = %+v, %v", record, err)
	}
	// 易失任务没有 PostgreSQL 兜底，Redis 写入失败必须返回错误。
	volatile, _ := NewTask("offline", nil)
	volatile.Volatile = true
	if _, err = s.Add(context.Background(), volatile); err == nil {
		t.Fatal("volatile Add with Redis down must fail")
	}
	_ = s.Close()
}

func TestVolatileReclaimedAfterLease(t *testing.T) {
	env := newTestEnv(t)
	cfg := env.resolved(t)
	cfg.LeaseDuration = 200 * time.Millisecond
	cfg.HandlerTimeout = 100 * time.Millisecond
	q, err := newQueue(env.rdb, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.load(context.Background()); err != nil {
		t.Fatal(err)
	}
	lifecycle, cancel := context.WithCancel(context.Background())
	s := &Scheduler{db: env.db, store: nil, queue: q, config: cfg, ctx: lifecycle, cancel: cancel, handlerCtx: lifecycle, handlerCancel: cancel}
	defer s.Close()

	assertReclaimed := func(t *testing.T, maxAttempts int, wantAttempts int) {
		cfg.MaxAttempts = maxAttempts
		s.config.MaxAttempts = maxAttempts
		id := newID(t)
		data, err := encodeVolatile(Task{Type: "volatile.reclaim", Volatile: true}, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err = q.enqueueVolatile(context.Background(), id, 0, data); err != nil {
			t.Fatal(err)
		}
		if _, err = q.pop(context.Background(), q.bucket(id), 10, cfg.LeaseDuration); err != nil {
			t.Fatal(err)
		}
		time.Sleep(250 * time.Millisecond)
		s.reclaim()
		stats, err := q.stats(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if wantAttempts == 0 {
			if stats.processing != 0 {
				t.Fatalf("exhausted volatile task still processing: %+v", stats)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err = env.rdb.HGet(ctx, q.keys[q.bucket(id)].data, string(id[:])).Result(); !errors.Is(err, redis.Nil) {
				t.Fatalf("exhausted volatile data = %v, want redis.Nil", err)
			}
			return
		}
		again, err := q.pop(context.Background(), q.bucket(id), 10, time.Second)
		if err != nil || len(again.items) != 1 {
			t.Fatalf("pop after reclaim = %+v, %v", again, err)
		}
		task, err := decodeVolatile(again.items[0].id, again.items[0].data)
		if err != nil {
			t.Fatal(err)
		}
		if task.Attempts != wantAttempts {
			t.Fatalf("volatile attempts after reclaim = %d, want %d", task.Attempts, wantAttempts)
		}
		if _, err = q.ack(context.Background(), q.bucket(id), again.token, []uuid.UUID{id}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("requeued", func(t *testing.T) { assertReclaimed(t, 3, 1) })
	t.Run("exhausted", func(t *testing.T) { assertReclaimed(t, 1, 0) })
}

// rowState 直接读库，用于 Close 之后（公开方法已返回 ErrClosed）核对任务状态。
func rowState(t *testing.T, env *testEnv, id uuid.UUID) (Status, int) {
	t.Helper()
	var status, attempts int
	row := env.db.QueryRow("SELECT status, attempts FROM scheduler_tasks_"+env.name+" WHERE id = $1", id)
	if err := row.Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	return Status(status), attempts
}

// redisCounts 汇总所有桶中 ready、processing、data 的成员数。
func redisCounts(t *testing.T, env *testEnv) (ready, processing, data int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, keys := range env.queue(t).keys {
		ready += env.rdb.ZCard(ctx, keys.ready).Val()
		processing += env.rdb.ZCard(ctx, keys.processing).Val()
		data += env.rdb.HLen(ctx, keys.data).Val()
	}
	return ready, processing, data
}

// Close 等待执行中的回调结束：等待期间不取消回调的 context，结果正常写回。
func TestCloseWaitsForRunningHandler(t *testing.T) {
	env := newTestEnv(t)
	started := make(chan struct{})
	var cancelled atomic.Bool
	s := newTestScheduler(t, env, map[string]Handler{"slow": func(ctx context.Context, _ *Task) error {
		close(started)
		select {
		case <-ctx.Done():
			cancelled.Store(true)
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
			return nil
		}
	}})
	task, _ := NewTask("slow", nil)
	id := addTask(t, s, task)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}
	_ = s.Close()
	if cancelled.Load() {
		t.Fatal("Close cancelled a running handler before ShutdownTimeout")
	}
	if status, attempts := rowState(t, env, id); status != StatusSucceeded || attempts != 0 {
		t.Fatalf("after Close: status = %s, attempts = %d, want succeeded and 0", status, attempts)
	}
	if ready, processing, _ := redisCounts(t, env); ready != 0 || processing != 0 {
		t.Fatalf("redis after Close: ready = %d, processing = %d, want 0 and 0", ready, processing)
	}
}

// 回调超过 ShutdownTimeout 仍未结束时才被取消，任务立即放回 ready 且不计尝试次数。
func TestCloseTimeoutRequeuesWithoutCountingAttempt(t *testing.T) {
	env := newTestEnv(t)
	env.cfg.ShutdownTimeout = 100 * time.Millisecond
	env.cfg.HandlerTimeout = 5 * time.Second
	env.cfg.LeaseDuration = 10 * time.Second
	started := make(chan struct{})
	s := newTestScheduler(t, env, map[string]Handler{"stuck": func(ctx context.Context, _ *Task) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}})
	task, _ := NewTask("stuck", nil)
	id := addTask(t, s, task)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}
	_ = s.Close()
	if status, attempts := rowState(t, env, id); status != StatusQueued || attempts != 0 {
		t.Fatalf("after Close: status = %s, attempts = %d, want queued and 0", status, attempts)
	}
	if ready, processing, _ := redisCounts(t, env); ready != 1 || processing != 0 {
		t.Fatalf("redis after Close: ready = %d, processing = %d, want 1 and 0", ready, processing)
	}
}

// 被 VolatileBelow 阈值自动判定的任务，payload 超过上限时回落为持久任务并正常执行。
func TestAutoVolatileFallsBackToDurable(t *testing.T) {
	env := newTestEnv(t)
	env.cfg.VolatileBelow = 500 * time.Millisecond
	env.cfg.VolatileMaxPayload = 16
	s := newTestScheduler(t, env, map[string]Handler{"fallback": func(context.Context, *Task) error { return nil }})
	task, _ := NewTask("fallback", map[string]string{"text": strings.Repeat("x", 64)})
	id := addTask(t, s, task)
	if countRows(t, env.db, env.name, id) != 1 {
		t.Fatal("oversized auto-volatile task must be stored in PostgreSQL")
	}
	waitRecord(t, s, id, StatusSucceeded, 5*time.Second)

	explicit := task
	explicit.Volatile = true
	if _, err := s.Add(context.Background(), explicit); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("explicit oversized volatile = %v, want ErrInvalidArgument", err)
	}
}

// 无法解码为任务 ID 的成员被跳过，不影响同批的正常任务；pop 同时返回原到期时间。
func TestPopSkipsMalformedMember(t *testing.T) {
	env := newTestEnv(t)
	q := env.queue(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id := newID(t)
	bucket := q.bucket(id)
	due := time.Now().Add(-time.Second)
	if _, err := q.enqueue(ctx, []queuedTask{{id: id, runAt: due}}); err != nil {
		t.Fatal(err)
	}
	if err := env.rdb.ZAdd(ctx, q.keys[bucket].ready, redis.Z{Score: float64(due.UnixMilli()), Member: "bad"}).Err(); err != nil {
		t.Fatal(err)
	}
	batch, err := q.pop(ctx, bucket, 10, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.items) != 1 || batch.items[0].id != id || batch.malformed != 1 {
		t.Fatalf("pop = %+v, want one valid item and one malformed", batch)
	}
	if got := batch.items[0].runAt.UnixMilli(); got != due.UnixMilli() {
		t.Fatalf("popped runAt = %d, want %d", got, due.UnixMilli())
	}
}

// 三个节点、持久与易失任务混合、并发登记：每个任务恰好执行一次，结束后 PostgreSQL 中
// 全部成功，Redis 中没有残留。
func TestMultiNodeMixedLoadExactlyOnce(t *testing.T) {
	env := newTestEnv(t)
	env.cfg.WorkerCount = 8
	var mu sync.Mutex
	counts := map[uuid.UUID]int{}
	handler := func(_ context.Context, task *Task) error {
		mu.Lock()
		counts[task.ID]++
		mu.Unlock()
		return nil
	}
	handlers := map[string]Handler{"mixed": handler}
	nodes := []*Scheduler{newTestScheduler(t, env, handlers), newTestScheduler(t, env, handlers), newTestScheduler(t, env, handlers)}

	const durable, volatile, writers = 300, 300, 8
	ids := make([]uuid.UUID, durable+volatile)
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := w; i < len(ids); i += writers {
				task, _ := NewTask("mixed", map[string]int{"i": i})
				task.Volatile = i >= durable
				task.RunAt = time.Now().Add(time.Duration(i%4) * 100 * time.Millisecond)
				id, err := nodes[i%len(nodes)].Add(context.Background(), task)
				if err != nil {
					errs <- err
					return
				}
				ids[i] = id
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	waitFor(t, 60*time.Second, "all tasks to execute", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(counts) == len(ids)
	})
	waitFor(t, 10*time.Second, "redis to drain", func() bool {
		ready, processing, data := redisCounts(t, env)
		return ready == 0 && processing == 0 && data == 0
	})
	// 留出一个租约的时间：如果有任务没被正确确认，会在这段时间里被回收并重复执行。
	time.Sleep(env.cfg.LeaseDuration + 500*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	for _, id := range ids {
		if counts[id] != 1 {
			t.Fatalf("task %s executed %d times, want 1", id, counts[id])
		}
	}
	var total, succeeded int
	if err := env.db.QueryRow("SELECT count(*), count(*) FILTER (WHERE status = 2) FROM scheduler_tasks_"+env.name).Scan(&total, &succeeded); err != nil {
		t.Fatal(err)
	}
	if total != durable || succeeded != durable {
		t.Fatalf("postgres rows = %d, succeeded = %d, want %d and %d", total, succeeded, durable, durable)
	}
}
