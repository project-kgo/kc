package infra

import (
	"context"
	"os"
	"testing"
	"time"

	. "github.com/project-kgo/kc/pkg/mq"
	"github.com/redis/go-redis/v9"
)

// TestRedisStreamStoreIntegration 仅在显式提供 Redis 7.0+ 测试实例时运行，避免普通单元测试依赖外部服务。
// KC_REDIS_TEST_DSN 用于单机；KC_REDIS_CLUSTER_TEST_DSN 用于 Cluster。
func TestRedisStreamStoreIntegration(t *testing.T) {
	tests := []struct {
		name    string
		env     string
		cluster bool
	}{
		{name: "standalone", env: "KC_REDIS_TEST_DSN"},
		{name: "cluster", env: "KC_REDIS_CLUSTER_TEST_DSN", cluster: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dsn := os.Getenv(test.env)
			if dsn == "" {
				t.Skipf("%s is not set", test.env)
			}
			testRedisStreamStoreIntegration(t, dsn, test.cluster)
		})
	}
}

func testRedisStreamStoreIntegration(t *testing.T, dsn string, cluster bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var client redis.UniversalClient
	if cluster {
		options, err := redis.ParseClusterURL(dsn)
		if err != nil {
			t.Fatal(err)
		}
		client = redis.NewClusterClient(options)
	} else {
		options, err := redis.ParseURL(dsn)
		if err != nil {
			t.Fatal(err)
		}
		client = redis.NewClient(options)
	}
	store := &redisStreamRedisStore{client: client}
	t.Cleanup(func() { _ = store.Close() })
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	config := testRedisStreamRuntimeConfig()
	config.keyPrefix = "kc:mq:integration:" + randomRedisStreamID("")
	mq := newRedisStreamMQ(store, config)
	const (
		topic = "orders"
		group = "billing"
	)
	stream := mq.streamKey(topic)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = client.Del(cleanupCtx, stream).Err()
	})

	if err := store.EnsureGroup(ctx, stream, group, "0"); err != nil {
		t.Fatal(err)
	}
	values, err := marshalRedisStreamMessage(&Message{Key: []byte("key"), Body: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Add(ctx, stream, values, 0); err != nil {
		t.Fatal(err)
	}
	messages, err := store.ReadGroup(ctx, stream, group, "consumer-a", 64, time.Millisecond)
	if err != nil || len(messages) != 1 {
		t.Fatalf("ReadGroup() messages = %v, error = %v", messages, err)
	}

	claimed, nextStart, err := store.AutoClaim(ctx, stream, group, "consumer-b", 0, "0-0", 64)
	if err != nil || len(claimed) != 1 || claimed[0].ID != messages[0].ID {
		t.Fatalf("AutoClaim() = (%v, %q, %v)", claimed, nextStart, err)
	}
	if nextStart != "0-0" {
		t.Fatalf("AutoClaim() next start = %q", nextStart)
	}
	decoded, err := messageFromRedisStreamRecord(claimed[0])
	if err != nil || string(decoded.Key) != "key" || string(decoded.Body) != "payload" {
		t.Fatalf("decoded message = (%#v, %v)", decoded, err)
	}
	if acknowledged, err := store.Ack(ctx, stream, group, messages[0].ID); err != nil || acknowledged != 1 {
		t.Fatalf("Ack() = (%d, %v)", acknowledged, err)
	}
	summary, err := client.XPending(ctx, stream, group).Result()
	if err != nil || summary.Count != 0 {
		t.Fatalf("pending summary = (%#v, %v)", summary, err)
	}

	// Redis 7+ 的 XAUTOCLAIM 会自动删除载荷已裁剪的 PEL 墓碑。
	trimmedID, err := store.Add(ctx, stream, values, 0)
	if err != nil {
		t.Fatal(err)
	}
	trimmed, err := store.ReadGroup(ctx, stream, group, "consumer-a", 64, time.Millisecond)
	if err != nil || len(trimmed) != 1 || trimmed[0].ID != trimmedID {
		t.Fatalf("ReadGroup() trimmed message = (%v, %v)", trimmed, err)
	}
	if err := client.XDel(ctx, stream, trimmedID).Err(); err != nil {
		t.Fatal(err)
	}
	claimed, nextStart, err = store.AutoClaim(ctx, stream, group, "consumer-b", 0, "0-0", 64)
	if err != nil || len(claimed) != 0 || nextStart != "0-0" {
		t.Fatalf("AutoClaim() trimmed message = (%v, %q, %v)", claimed, nextStart, err)
	}
	summary, err = client.XPending(ctx, stream, group).Result()
	if err != nil || summary.Count != 0 {
		t.Fatalf("pending summary after tombstone cleanup = (%#v, %v)", summary, err)
	}
}
