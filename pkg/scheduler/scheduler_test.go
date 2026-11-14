package scheduler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestResolveConfigDefaults(t *testing.T) {
	cfg, err := resolveConfig(Config{TableSuffix: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		got  any
		want any
	}{
		{"KeyPrefix", cfg.KeyPrefix, "sched"},
		{"Buckets", cfg.Buckets, 8},
		{"Window", cfg.Window, 5 * time.Minute},
		{"LoadInterval", cfg.LoadInterval, time.Second},
		{"LoadBatch", cfg.LoadBatch, 1000},
		{"PollInterval", cfg.PollInterval, 100 * time.Millisecond},
		{"WorkerCount", cfg.WorkerCount, 16},
		{"HandlerTimeout", cfg.HandlerTimeout, 30 * time.Second},
		{"LeaseDuration", cfg.LeaseDuration, 60 * time.Second},
		{"MaxAttempts", cfg.MaxAttempts, 10},
		{"RetryBackoffMin", cfg.RetryBackoffMin, time.Second},
		{"RetryBackoffMax", cfg.RetryBackoffMax, 5 * time.Minute},
		{"ReclaimInterval", cfg.ReclaimInterval, 5 * time.Second},
		{"ReconcileInterval", cfg.ReconcileInterval, 5 * time.Second},
		{"ReconcileGrace", cfg.ReconcileGrace, 10 * time.Second},
		{"VolatileBelow", cfg.VolatileBelow, 10 * time.Second},
		{"VolatileMaxPayload", cfg.VolatileMaxPayload, 4096},
		{"Retention", cfg.Retention, 24 * time.Hour},
		{"FailedRetention", cfg.FailedRetention, time.Duration(0)},
		{"CleanupInterval", cfg.CleanupInterval, time.Minute},
		{"ShutdownTimeout", cfg.ShutdownTimeout, 30 * time.Second},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if cfg.Logger == nil || cfg.Metrics == nil {
		t.Fatal("logger and metrics must be filled")
	}
}

func TestResolveConfigValidation(t *testing.T) {
	valid := func() Config { return Config{TableSuffix: "orders"} }
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"missing suffix", func(c *Config) { c.TableSuffix = "" }},
		{"unsafe suffix", func(c *Config) { c.TableSuffix = "Orders" }},
		{"unsafe schema", func(c *Config) { c.Schema = "public; drop table" }},
		{"blank key prefix", func(c *Config) { c.KeyPrefix = " " }},
		{"negative buckets", func(c *Config) { c.Buckets = -1 }},
		{"negative batch", func(c *Config) { c.LoadBatch = -1 }},
		{"negative workers", func(c *Config) { c.WorkerCount = -1 }},
		{"zero max attempts", func(c *Config) { c.MaxAttempts = -1 }},
		{"short poll interval", func(c *Config) { c.PollInterval = time.Microsecond }},
		{"lease equals timeout", func(c *Config) {
			c.HandlerTimeout = time.Second
			c.LeaseDuration = time.Second
		}},
		{"backoff max below min", func(c *Config) {
			c.RetryBackoffMin = time.Second
			c.RetryBackoffMax = time.Millisecond
		}},
		{"backoff max above window", func(c *Config) {
			c.Window = time.Second
			c.RetryBackoffMax = 2 * time.Second
		}},
		{"negative failed retention", func(c *Config) { c.FailedRetention = -time.Second }},
		{"negative volatile threshold is allowed", func(c *Config) { c.VolatileBelow = -time.Second }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid()
			test.mutate(&cfg)
			_, err := resolveConfig(cfg)
			allow := test.name == "negative volatile threshold is allowed"
			if allow && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !allow && !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestStatusString(t *testing.T) {
	for status, want := range map[Status]string{
		StatusPending: "pending", StatusQueued: "queued", StatusSucceeded: "succeeded",
		StatusFailed: "failed", StatusCancelled: "cancelled",
	} {
		if got := status.String(); got != want {
			t.Errorf("%d.String() = %q, want %q", status, got, want)
		}
	}
	if got := Status(9).String(); got != "Status(9)" {
		t.Errorf("unknown status = %q", got)
	}
}

func TestNewTaskAndDecode(t *testing.T) {
	type payload struct {
		OrderID int64 `json:"order_id"`
	}
	task, err := NewTask("order.timeout", payload{OrderID: 42})
	if err != nil {
		t.Fatal(err)
	}
	if task.Type != "order.timeout" || task.Payload == "" {
		t.Fatalf("unexpected task: %+v", task)
	}
	var decoded payload
	if err = task.Decode(&decoded); err != nil || decoded.OrderID != 42 {
		t.Fatalf("decode = %+v, %v", decoded, err)
	}
	if err = task.Decode(nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("decode nil = %v", err)
	}
	if _, err = NewTask(" ", 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("invalid type = %v", err)
	}
}

func TestQueueKeyLayoutAndBucket(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	defer client.Close()
	cfg, err := resolveConfig(Config{TableSuffix: "orders", KeyPrefix: "sched", Buckets: 4})
	if err != nil {
		t.Fatal(err)
	}
	q, err := newQueue(client, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := q.keys[2].ready, "sched:{orders:2}:ready"; got != want {
		t.Fatalf("ready key = %q, want %q", got, want)
	}
	if got, want := q.keys[2].processing, "sched:{orders:2}:processing"; got != want {
		t.Fatalf("processing key = %q, want %q", got, want)
	}
	if got, want := q.keys[2].data, "sched:{orders:2}:data"; got != want {
		t.Fatalf("data key = %q, want %q", got, want)
	}
	id := uuid.MustParse("0190a2b4-0000-7000-8000-000000000001")
	if first, second := q.bucket(id), q.bucket(id); first != second {
		t.Fatalf("bucket is not stable: %d != %d", first, second)
	}
	if bucket := q.bucket(id); bucket < 0 || bucket >= cfg.Buckets {
		t.Fatalf("bucket %d out of range", bucket)
	}
	if _, err = newQueue(nil, cfg); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil redis client = %v", err)
	}
}

func TestVolatileDecision(t *testing.T) {
	newScheduler := func(below time.Duration) *Scheduler {
		return &Scheduler{config: Config{Window: 5 * time.Minute, VolatileBelow: below}}
	}
	s := newScheduler(10 * time.Second)
	tests := []struct {
		name string
		task Task
		want bool
	}{
		{"immediate", Task{}, true},
		{"below threshold", Task{RunAt: time.Now().Add(3 * time.Second)}, true},
		{"above threshold", Task{RunAt: time.Now().Add(20 * time.Second)}, false},
		{"durable excludes threshold", Task{RunAt: time.Now().Add(3 * time.Second), Durable: true}, false},
		{"explicit volatile", Task{RunAt: time.Now().Add(time.Hour), Volatile: true}, true},
	}
	for _, test := range tests {
		if got := s.isVolatile(test.task); got != test.want {
			t.Errorf("%s: isVolatile = %v, want %v", test.name, got, test.want)
		}
	}
	off := newScheduler(-time.Second)
	if off.isVolatile(Task{RunAt: time.Now().Add(time.Second)}) {
		t.Fatal("negative threshold must disable automatic volatile tasks")
	}
	if !off.isVolatile(Task{Volatile: true}) {
		t.Fatal("explicit volatile must still work")
	}
}

func TestValidateVolatile(t *testing.T) {
	s := &Scheduler{config: Config{Window: time.Minute, VolatileMaxPayload: 8}}
	if err := s.validateVolatile(Task{Payload: "12345678"}); err != nil {
		t.Fatalf("payload at limit: %v", err)
	}
	if err := s.validateVolatile(Task{Payload: "123456789"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("oversized payload = %v", err)
	}
	if err := s.validateVolatile(Task{RunAt: time.Now().Add(2 * time.Minute)}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("delay beyond window = %v", err)
	}
}

func TestVolatileForFallsBackToDurable(t *testing.T) {
	s := &Scheduler{config: Config{Window: 5 * time.Second, VolatileBelow: 10 * time.Second, VolatileMaxPayload: 8}}
	big := "123456789"
	if got, err := s.volatileFor(Task{}); err != nil || !got {
		t.Fatalf("small immediate task = %v, %v, want volatile", got, err)
	}
	// 阈值自动判定的任务不满足易失条件时回落为持久任务，不报错。
	if got, err := s.volatileFor(Task{Payload: big}); err != nil || got {
		t.Fatalf("auto volatile with oversized payload = %v, %v, want durable", got, err)
	}
	if got, err := s.volatileFor(Task{RunAt: time.Now().Add(7 * time.Second)}); err != nil || got {
		t.Fatalf("auto volatile beyond window = %v, %v, want durable", got, err)
	}
	// 显式要求易失的任务不满足条件时报错。
	if _, err := s.volatileFor(Task{Payload: big, Volatile: true}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("explicit volatile with oversized payload = %v", err)
	}
	if got, err := s.volatileFor(Task{Durable: true}); err != nil || got {
		t.Fatalf("durable task = %v, %v, want durable", got, err)
	}
}

// Close 取消后台循环的 context 时，执行中的回调不应被打断；只有 handlerCtx 取消才中断。
func TestInvokeIgnoresLifecycleCancel(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	handlerCtx, handlerCancel := context.WithCancel(context.Background())
	defer handlerCancel()
	s := &Scheduler{
		config: Config{HandlerTimeout: time.Minute},
		ctx:    lifecycle, cancel: cancel, handlerCtx: handlerCtx, handlerCancel: handlerCancel,
	}
	cancel()
	err := s.invoke(func(ctx context.Context, _ *Task) error { return ctx.Err() }, Task{})
	if err != nil {
		t.Fatalf("handler context after lifecycle cancel = %v, want nil", err)
	}
	handlerCancel()
	err = s.invoke(func(ctx context.Context, _ *Task) error { return ctx.Err() }, Task{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("handler context after handler cancel = %v, want canceled", err)
	}
}

func TestVolatileContentRoundTrip(t *testing.T) {
	task := Task{ID: uuid.New(), Type: "room.heartbeat", Payload: `{"room_id":7}`}
	data, err := encodeVolatile(task, 3)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeVolatile(task.ID, data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ID != task.ID {
		t.Fatalf("decoded ID = %s, want %s", decoded.ID, task.ID)
	}
	if decoded.Type != task.Type || decoded.Payload != task.Payload || decoded.Attempts != 3 || !decoded.Volatile {
		t.Fatalf("decoded = %+v", decoded)
	}
	if _, err = decodeVolatile(task.ID, "{"); err == nil {
		t.Fatal("invalid content must fail")
	}
}

func TestBackoffWithinBounds(t *testing.T) {
	s := &Scheduler{config: Config{RetryBackoffMin: 10 * time.Millisecond, RetryBackoffMax: 2 * time.Second}}
	for attempt := 1; attempt <= 64; attempt++ {
		got := s.backoff(attempt)
		if got < s.config.RetryBackoffMin || got > s.config.RetryBackoffMax {
			t.Fatalf("backoff(%d) = %v, out of [%v, %v]", attempt, got, s.config.RetryBackoffMin, s.config.RetryBackoffMax)
		}
	}
	if got := s.backoff(1000); got < s.config.RetryBackoffMax/2 || got > s.config.RetryBackoffMax {
		t.Fatalf("backoff(1000) = %v, want within [%v, %v]", got, s.config.RetryBackoffMax/2, s.config.RetryBackoffMax)
	}
}

func TestJitterWithinTenPercent(t *testing.T) {
	for _, d := range []time.Duration{time.Second, 100 * time.Millisecond} {
		for range 100 {
			got := jitter(d)
			if got < d || got > d+d/10+time.Nanosecond {
				t.Fatalf("jitter(%v) = %v", d, got)
			}
		}
	}
	if got := jitter(0); got != 0 {
		t.Fatalf("jitter(0) = %v", got)
	}
}

func TestErrorTextTruncates(t *testing.T) {
	if got := errorText(nil); got != "" {
		t.Fatalf("errorText(nil) = %q", got)
	}
	got := errorText(errors.New(strings.Repeat("错", 1000)))
	if len(got) > 1024 || !strings.HasPrefix(got, "错") {
		t.Fatalf("truncated length = %d", len(got))
	}
}

func TestRegistry(t *testing.T) {
	handler := func(context.Context, *Task) error { return nil }
	reg, err := (*registry)(nil).with(map[string]Handler{"a": handler})
	if err != nil {
		t.Fatal(err)
	}
	if reg.get("a") == nil || reg.get("b") != nil {
		t.Fatal("registry lookup failed")
	}
	if _, err = reg.with(map[string]Handler{"a": handler}); err == nil {
		t.Fatal("duplicate handler must fail")
	}
	if _, err = reg.with(map[string]Handler{"b": nil}); err == nil {
		t.Fatal("nil handler must fail")
	}
	if _, err = (*registry)(nil).with(map[string]Handler{" ": handler}); err == nil {
		t.Fatal("empty type must fail")
	}
}

func TestNearTerm(t *testing.T) {
	s := &Scheduler{config: Config{Window: time.Minute}}
	if !s.nearTerm(time.Time{}) || !s.nearTerm(time.Now().Add(-time.Second)) {
		t.Fatal("zero and past run_at must be near term")
	}
	if s.nearTerm(time.Now().Add(2 * time.Minute)) {
		t.Fatal("run_at beyond window must be far term")
	}
}

func TestIndexNameFitsPostgresLimit(t *testing.T) {
	table := "scheduler_tasks_" + strings.Repeat("a", 40)
	names := map[string]bool{}
	for _, suffix := range []string{"_load_idx", "_queued_idx", "_done_idx"} {
		name := indexName(table, suffix)
		if len(name) > 63 {
			t.Fatalf("index name %q is %d bytes", name, len(name))
		}
		if names[name] {
			t.Fatalf("duplicate index name %q", name)
		}
		names[name] = true
	}
}
