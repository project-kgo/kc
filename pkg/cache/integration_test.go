package cache

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisInvalidatesPeerCache(t *testing.T) {
	dsn := os.Getenv("KC_REDIS_TEST_DSN")
	if dsn == "" {
		t.Skip("KC_REDIS_TEST_DSN is not set")
	}
	options, err := redis.ParseURL(dsn)
	if err != nil {
		t.Fatal(err)
	}
	publisher := redis.NewClient(options)
	subscriber := redis.NewClient(options)
	t.Cleanup(func() {
		_ = publisher.Close()
		_ = subscriber.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := publisher.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	namespace := fmt.Sprintf("cache-test-%d", time.Now().UnixNano())
	configA := Config[int]{
		DefaultTTL:    time.Minute,
		Redis:         publisher,
		PubSubChannel: "kc-cache-test",
		Namespace:     namespace,
	}
	configB := Config[int]{
		DefaultTTL:    time.Minute,
		Redis:         subscriber,
		PubSubChannel: "kc-cache-test",
		Namespace:     namespace,
	}
	a, err := New[int, string](ctx, configA)
	if err != nil {
		t.Fatal(err)
	}
	b, err := New[int, string](ctx, configB)
	if err != nil {
		_ = a.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})

	if err := b.Set(42, "value"); err != nil {
		t.Fatal(err)
	}
	if err := a.Delete(ctx, 42); err != nil {
		t.Fatal(err)
	}

	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		if _, ok := b.Get(42); !ok {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("peer cache was not invalidated")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
