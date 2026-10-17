package compensator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

func TestResolveConfigDefaults(t *testing.T) {
	c, err := resolveConfig(Config{TableSuffix: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if c.WorkerCount != 8 || c.BatchSize != 16 || c.ScanInterval != time.Second || c.retries != 10 ||
		c.HandlerTimeout != 30*time.Second || c.LeaseDuration != 40*time.Second || c.CleanupAfter != 5*time.Minute ||
		c.CleanupBatchSize != 1000 || c.NodeTTL != 15*time.Second || c.Mode != ModeInTx || c.Logger == nil {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	if c, err = resolveConfig(Config{TableSuffix: "x", MaxRetries: -1}); err != nil || c.retries != 0 {
		t.Fatalf("MaxRetries=-1 should disable retries: %v %d", err, c.retries)
	}
}

func TestResolveConfigRejectsInvalid(t *testing.T) {
	for name, c := range map[string]Config{
		"empty suffix":        {},
		"upper suffix":        {TableSuffix: "Orders"},
		"long suffix":         {TableSuffix: "a12345678901234567890123456789012345678901"},
		"unsafe schema":       {TableSuffix: "x", Schema: `a"b`},
		"negative workers":    {TableSuffix: "x", WorkerCount: -1},
		"negative interval":   {TableSuffix: "x", ScanInterval: -time.Second},
		"lease <= handler":    {TableSuffix: "x", HandlerTimeout: time.Second, LeaseDuration: time.Second},
		"ttl < 3 heartbeats":  {TableSuffix: "x", HeartbeatInterval: time.Second, NodeTTL: 2 * time.Second},
		"backoff max < min":   {TableSuffix: "x", RetryBackoffMin: time.Minute, RetryBackoffMax: time.Second},
		"retries < -1":        {TableSuffix: "x", MaxRetries: -2},
		"unknown mode":        {TableSuffix: "x", Mode: Mode(9)},
		"negative failed ret": {TableSuffix: "x", FailedRetention: -1},
	} {
		if _, err := resolveConfig(c); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: expected ErrInvalidConfig, got %v", name, err)
		}
	}
}

func TestTaskPayloadRoundTrip(t *testing.T) {
	type event struct {
		OrderID int64  `json:"order_id"`
		Name    string `json:"name"`
	}
	task, err := NewTask("order.created", event{OrderID: 9223372036854775807, Name: "中文"})
	if err != nil {
		t.Fatal(err)
	}
	var got event
	if err = task.Decode(&got); err != nil || got.OrderID != 9223372036854775807 || got.Name != "中文" {
		t.Fatalf("decode = %+v, %v", got, err)
	}
	if _, err = NewTask(" ", 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("blank type: %v", err)
	}
	if err = task.Decode(nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil destination: %v", err)
	}
}

func TestBackoffBounds(t *testing.T) {
	c := &Compensator{config: resolvedConfig{Config: Config{RetryBackoffMin: time.Second, RetryBackoffMax: 10 * time.Second}}}
	for attempt, ceiling := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 4: 8 * time.Second, 5: 10 * time.Second, 50: 10 * time.Second} {
		for range 100 {
			d := c.backoff(attempt)
			if d < ceiling/2 || d > ceiling {
				t.Fatalf("attempt %d: %v outside [%v, %v]", attempt, d, ceiling/2, ceiling)
			}
		}
	}
}

func TestInvokeAndOutcome(t *testing.T) {
	c := &Compensator{config: resolvedConfig{Config: Config{HandlerTimeout: 20 * time.Millisecond, RetryBackoffMin: time.Second, RetryBackoffMax: time.Second}, retries: 2},
		handlers: map[string]Handler{
			"panic":  func(context.Context, *Task) error { panic("boom") },
			"ignore": func(context.Context, *Task) error { time.Sleep(40 * time.Millisecond); return nil },
			"mutate": func(_ context.Context, task *Task) error { task.Attempt = 99; return nil },
		}}
	if err := c.invoke(context.Background(), &Task{Type: "panic"}); !errors.Is(err, ErrHandlerPanic) {
		t.Fatalf("panic: %v", err)
	}
	if err := c.invoke(context.Background(), &Task{Type: "ignore"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout ignored by handler should fail: %v", err)
	}
	if err := c.invoke(context.Background(), &Task{Type: "missing"}); !errors.Is(err, ErrNoHandler) {
		t.Fatalf("missing handler: %v", err)
	}
	task := &Task{Type: "mutate", Attempt: 1}
	if err := c.invoke(context.Background(), task); err != nil || task.Attempt != 1 {
		t.Fatalf("handler must receive a copy: %v %d", err, task.Attempt)
	}
	if s, _, _ := c.outcome(1, nil); s != StatusSucceeded {
		t.Fatal("success outcome")
	}
	if s, msg, d := c.outcome(2, errors.New("x")); s != StatusPending || msg != "x" || d <= 0 {
		t.Fatalf("retry outcome: %v %q %v", s, msg, d)
	}
	if s, _, _ := c.outcome(3, errors.New("x")); s != StatusFailed {
		t.Fatal("exhausted outcome")
	}
}

// fakeStorage 是内存版存储，按与 SQL 相同的令牌语义领取和落库。
type fakeStorage struct {
	mu       sync.Mutex
	tasks    map[uuid.UUID]*fakeRow
	released atomic.Bool
}

type fakeRow struct {
	task   Task
	status Status
	token  int64
	due    time.Time
}

func newFakeStorage() *fakeStorage { return &fakeStorage{tasks: make(map[uuid.UUID]*fakeRow)} }

func (f *fakeStorage) add(n int) []uuid.UUID {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]uuid.UUID, n)
	for i := range ids {
		ids[i] = uuid.Must(uuid.NewV7())
		f.tasks[ids[i]] = &fakeRow{task: Task{ID: ids[i], Type: "t"}, status: StatusPending}
	}
	return ids
}

func (f *fakeStorage) insert(context.Context, *sqlx.Tx, []newRow) error { return nil }

func (f *fakeStorage) claim(_ context.Context, shards []int16, limit int, token int64) ([]claimed, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []claimed
	for _, row := range f.tasks {
		if len(out) == limit {
			break
		}
		if row.status < StatusSucceeded && !row.due.After(time.Now()) {
			row.status, row.token = StatusRunning, token
			row.task.Attempt++
			row.due = time.Now().Add(time.Minute)
			out = append(out, claimed{Task: row.task, token: token})
		}
	}
	return out, nil
}

func (f *fakeStorage) claimIDs(_ context.Context, ids []uuid.UUID, own, token int64) ([]claimed, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []claimed
	for _, id := range ids {
		row := f.tasks[id]
		if row == nil || row.status >= StatusSucceeded || (row.status == StatusRunning && row.token != own) {
			continue
		}
		if row.token != own {
			row.task.Attempt++
		}
		row.status, row.token = StatusRunning, token
		out = append(out, claimed{Task: row.task, token: token})
	}
	return out, nil
}

func (f *fakeStorage) finish(_ context.Context, c claimed, status Status, _ string, delay time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	row := f.tasks[c.ID]
	if row == nil || row.status != StatusRunning || row.token != c.token {
		return ErrLeaseLost
	}
	row.status, row.token, row.due = status, 0, time.Now().Add(delay)
	return nil
}

func (f *fakeStorage) abandon(_ context.Context, c claimed) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if row := f.tasks[c.ID]; row != nil && row.token == c.token {
		row.status, row.token, row.due = StatusPending, 0, time.Time{}
		row.task.Attempt--
	}
	return nil
}

func (f *fakeStorage) get(_ context.Context, id uuid.UUID) (Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	row := f.tasks[id]
	if row == nil {
		return Record{}, ErrNotFound
	}
	return Record{Task: row.task, Status: row.status, Attempts: row.task.Attempt}, nil
}

func (f *fakeStorage) statuses(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]Status, error) {
	result := map[uuid.UUID]Status{}
	for _, id := range ids {
		if r, err := f.get(ctx, id); err == nil {
			result[id] = r.Status
		}
	}
	return result, nil
}

func (f *fakeStorage) retry(context.Context, uuid.UUID) error { return nil }
func (f *fakeStorage) cleanup(context.Context, []int16, Status, time.Duration, int) (int64, error) {
	return 0, nil
}
func (f *fakeStorage) stats(context.Context) (Stats, error) {
	return Stats{Pending: int64(f.count(StatusPending)), Running: int64(f.count(StatusRunning)), Failed: int64(f.count(StatusFailed))}, nil
}
func (f *fakeStorage) coordinate(context.Context, string) ([]int16, error) {
	return []int16{0, 1, 2}, nil
}
func (f *fakeStorage) release(context.Context, string) error { f.released.Store(true); return nil }

func (f *fakeStorage) count(status Status) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, row := range f.tasks {
		if row.status == status {
			n++
		}
	}
	return n
}

// recordingMetrics 统计收到的事件，用于验证指标钩子被调用。
type recordingMetrics struct {
	mu       sync.Mutex
	executed map[Source]map[Status]int
	shards   atomic.Int64
}

func (m *recordingMetrics) Executed(e ExecEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.executed == nil {
		m.executed = map[Source]map[Status]int{}
	}
	if m.executed[e.Source] == nil {
		m.executed[e.Source] = map[Status]int{}
	}
	m.executed[e.Source][e.Status]++
}
func (m *recordingMetrics) Cleaned(Status, int64) {}
func (m *recordingMetrics) ShardsOwned(n int)     { m.shards.Store(int64(n)) }

func (m *recordingMetrics) count(source Source, status Status) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.executed[source][status]
}

func testConfig(t *testing.T, c Config) resolvedConfig {
	t.Helper()
	c.TableSuffix = "test"
	c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	resolved, err := resolveConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestBackgroundRespectsWorkerCount(t *testing.T) {
	store := newFakeStorage()
	store.add(200)
	var running, peak atomic.Int64
	handler := func(_ context.Context, task *Task) error {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		running.Add(-1)
		// 每个任务第一次失败，验证退避后会被再次领取。
		if task.Attempt == 1 {
			return errors.New("transient")
		}
		return nil
	}
	metrics := &recordingMetrics{}
	cfg := testConfig(t, Config{WorkerCount: 4, ScanInterval: 10 * time.Millisecond, RetryBackoffMin: time.Millisecond, RetryBackoffMax: 2 * time.Millisecond, Metrics: metrics})
	c, err := start(context.Background(), nil, store, cfg, map[string]Handler{"t": handler})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool { return store.count(StatusSucceeded) == 200 })
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	if peak.Load() > 4 {
		t.Fatalf("concurrency %d exceeds worker count", peak.Load())
	}
	if !store.released.Load() {
		t.Fatal("Close must release leases")
	}
	if len(c.slots) != 0 {
		t.Fatalf("leaked %d slots", len(c.slots))
	}
	if metrics.count(SourceBackground, StatusPending) != 200 || metrics.count(SourceBackground, StatusSucceeded) != 200 || metrics.shards.Load() != 3 {
		t.Fatalf("unexpected metrics: %+v shards=%d", metrics.executed, metrics.shards.Load())
	}
}

func TestCloseAbandonsInterruptedTasks(t *testing.T) {
	store := newFakeStorage()
	ids := store.add(2)
	started := make(chan struct{}, 2)
	handler := func(ctx context.Context, _ *Task) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	cfg := testConfig(t, Config{WorkerCount: 2, ScanInterval: 10 * time.Millisecond})
	c, err := start(context.Background(), nil, store, cfg, map[string]Handler{"t": handler})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	<-started
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		r, _ := store.get(context.Background(), id)
		if r.Status != StatusPending || r.Attempts != 0 {
			t.Fatalf("interrupted task should be returned without consuming an attempt: %+v", r)
		}
	}
	if _, err = c.Get(context.Background(), ids[0]); !errors.Is(err, ErrClosed) {
		t.Fatalf("calls after Close: %v", err)
	}
	if err = c.Close(); err != nil {
		t.Fatal("Close must be idempotent")
	}
}

func TestDispatchSkipsTasksHeldByOthers(t *testing.T) {
	store := newFakeStorage()
	ids := store.add(2)
	cfg := testConfig(t, Config{DisableWorker: true})
	var calls atomic.Int64
	c, err := start(context.Background(), nil, store, cfg, map[string]Handler{"t": func(context.Context, *Task) error { calls.Add(1); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	store.tasks[ids[1]].status, store.tasks[ids[1]].token = StatusRunning, 42
	results, err := c.Dispatch(context.Background(), ids...)
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Status != StatusSucceeded || results[1].Status != StatusRunning || calls.Load() != 1 {
		t.Fatalf("unexpected results %+v, calls %d", results, calls.Load())
	}
	if st, err := c.Stats(context.Background()); err != nil || st.Running != 1 {
		t.Fatalf("stats: %+v %v", st, err)
	}
	if _, err = c.Dispatch(context.Background(), ids[0], ids[0]); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("duplicate IDs: %v", err)
	}
}

func TestCanceledSuccessIsNotFailure(t *testing.T) {
	c := &Compensator{config: resolvedConfig{Config: Config{HandlerTimeout: time.Second}},
		handlers: map[string]Handler{"t": func(context.Context, *Task) error { return nil }}}
	ctx, cancel := context.WithCancel(context.Background())
	c.handlers["t"] = func(context.Context, *Task) error { cancel(); return nil }
	if err := c.invoke(ctx, &Task{Type: "t"}); err != nil {
		t.Fatalf("handler succeeded before cancellation was observed: %v", err)
	}
}

func TestTxAddRejectsDuplicateIDs(t *testing.T) {
	c := &Compensator{handlers: map[string]Handler{"t": func(context.Context, *Task) error { return nil }}}
	tx := &Tx{c: c}
	id := uuid.Must(uuid.NewV7())
	if _, err := tx.Add(context.Background(), Task{ID: id, Type: "t"}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Add(context.Background(), Task{ID: id, Type: "t"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("duplicate ID: %v", err)
	}
	if _, err := tx.Add(context.Background(), Task{Type: "t"}); err == nil {
		t.Fatal("transaction must stay failed after an Add error")
	}
}

func TestParallelRespectsLimit(t *testing.T) {
	c := &Compensator{config: resolvedConfig{Config: Config{WorkerCount: 3}}}
	var running, peak atomic.Int64
	var done atomic.Int64
	c.parallel(50, func(int) {
		n := running.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(time.Millisecond)
		running.Add(-1)
		done.Add(1)
	})
	if done.Load() != 50 || peak.Load() > 3 {
		t.Fatalf("done=%d peak=%d", done.Load(), peak.Load())
	}
}

func TestRunnableReportsExhausted(t *testing.T) {
	metrics := &recordingMetrics{}
	c := &Compensator{config: testConfig(t, Config{Metrics: metrics})}
	tasks := []claimed{{Task: Task{ID: uuid.New()}}, {Task: Task{ID: uuid.New()}, exhausted: true}}
	if got := c.runnable(tasks, SourceBackground); len(got) != 1 || got[0].ID != tasks[0].ID {
		t.Fatalf("runnable = %+v", got)
	}
	if metrics.count(SourceBackground, StatusFailed) != 1 {
		t.Fatal("exhausted task must be reported")
	}
}
