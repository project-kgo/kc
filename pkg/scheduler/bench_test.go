package scheduler

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// 压测入口：
//
//	TEST_POSTGRES_DSN=... KC_REDIS_TEST_DSN=... \
//	go test ./pkg/scheduler -run '^$' -bench BenchmarkAdd -benchtime 10s
func BenchmarkAddPersistent(b *testing.B) { benchmarkAdd(b, false) }
func BenchmarkAddVolatile(b *testing.B)   { benchmarkAdd(b, true) }

func benchmarkAdd(b *testing.B, volatile bool) {
	db := openTestDB(b)
	rdb := openTestRedis(b)
	suffix := "b" + strings.ReplaceAll(uuid.NewString()[:13], "-", "")
	cfg := fastConfig(suffix)
	cfg.PollInterval = time.Second
	resolved, err := resolveConfig(cfg)
	if err != nil {
		b.Fatal(err)
	}
	reg, err := (*registry)(nil).with(map[string]Handler{"bench": func(context.Context, *Task) error { return nil }})
	if err != nil {
		b.Fatal(err)
	}
	store, err := newPGStorage(db, resolved)
	if err != nil {
		b.Fatal(err)
	}
	if err = store.ensureSchema(context.Background()); err != nil {
		b.Fatal(err)
	}
	q, err := newQueue(rdb, resolved)
	if err != nil {
		b.Fatal(err)
	}
	if err = q.load(context.Background()); err != nil {
		b.Fatal(err)
	}
	s := start(context.Background(), db, store, q, resolved, reg)
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS scheduler_tasks_%s", suffix))
		dropKeys(ctx, rdb, cfg)
	})
	b.Cleanup(func() { _ = s.Close() })

	task, err := NewTask("bench", map[string]string{"order": "42"})
	if err != nil {
		b.Fatal(err)
	}
	task.Volatile = volatile
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err = s.Add(context.Background(), task); err != nil {
			b.Fatal(err)
		}
	}
}
