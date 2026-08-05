package infra

import (
	"context"
	"errors"
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
	dlqStream := mq.streamKey(topic + redisStreamDLQSuffix)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = client.Del(cleanupCtx, stream).Err()
		_ = client.Del(cleanupCtx, dlqStream).Err()
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
	pendingEntry, exists, err := store.PendingEntry(ctx, stream, group, messages[0].ID)
	if err != nil || !exists || pendingEntry.RetryCount < 2 {
		t.Fatalf("PendingEntry() = (%#v, %v, %v)", pendingEntry, exists, err)
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

	// 批量 ACK 应一次移除多个 Pending 条目。
	firstID, err := store.Add(ctx, stream, values, 0)
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := store.Add(ctx, stream, values, 0)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.ReadGroup(ctx, stream, group, "consumer-batch", 64, time.Millisecond)
	if err != nil || len(batch) != 2 {
		t.Fatalf("batch ReadGroup() = (%v, %v)", batch, err)
	}
	if acknowledged, err := store.Ack(ctx, stream, group, firstID, secondID); err != nil || acknowledged != 2 {
		t.Fatalf("batch Ack() = (%d, %v)", acknowledged, err)
	}

	// Lua 清理只能删除零 Pending consumer，并支持排除当前 consumer。
	for _, consumer := range []string{"empty-consumer", "excluded-consumer"} {
		if err := client.XGroupCreateConsumer(ctx, stream, group, consumer).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if deleted, err := store.CleanupConsumers(ctx, stream, group, "empty-consumer", "", 0, 1); err != nil || deleted != 1 {
		t.Fatalf("cleanup current consumer = (%d, %v)", deleted, err)
	}
	pendingID, err := store.Add(ctx, stream, values, 0)
	if err != nil {
		t.Fatal(err)
	}
	if messages, err := store.ReadGroup(ctx, stream, group, "pending-consumer", 1, time.Millisecond); err != nil || len(messages) != 1 {
		t.Fatalf("pending consumer read = (%v, %v)", messages, err)
	}
	if deleted, err := store.CleanupConsumers(ctx, stream, group, "pending-consumer", "", 0, 1); err != nil || deleted != 0 {
		t.Fatalf("cleanup pending consumer = (%d, %v)", deleted, err)
	}
	if _, exists, err := store.PendingEntry(ctx, stream, group, pendingID); err != nil || !exists {
		t.Fatalf("pending entry after cleanup = (%v, %v)", exists, err)
	}
	if _, err := store.Ack(ctx, stream, group, pendingID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CleanupConsumers(ctx, stream, group, "", "excluded-consumer", 0, 128); err != nil {
		t.Fatal(err)
	}
	consumers, err := client.XInfoConsumers(ctx, stream, group).Result()
	if err != nil {
		t.Fatal(err)
	}
	foundExcluded := false
	for _, consumer := range consumers {
		foundExcluded = foundExcluded || consumer.Name == "excluded-consumer"
	}
	if !foundExcluded {
		t.Fatal("excluded consumer was deleted")
	}

	// 达到最大投递次数后写入自动 DLQ，再确认源消息。
	poisonID, err := store.Add(ctx, stream, values, 0)
	if err != nil {
		t.Fatal(err)
	}
	poison, err := store.ReadGroup(ctx, stream, group, "poison-consumer", 1, time.Millisecond)
	if err != nil || len(poison) != 1 || poison[0].ID != poisonID {
		t.Fatalf("poison read = (%v, %v)", poison, err)
	}
	dlqConfig := config
	dlqConfig.maxDeliveryAttempts = 1
	mq.processRedisStreamMessage(ctx, topic, stream, group,
		func(context.Context, *Message) error { return errors.New("poison") }, poison[0], dlqConfig)
	dlqEntries, err := client.XRange(ctx, dlqStream, "-", "+").Result()
	if err != nil || len(dlqEntries) != 1 {
		t.Fatalf("DLQ entries = (%v, %v)", dlqEntries, err)
	}
	dlqMessage, err := messageFromRedisStreamRecord(dlqEntries[0])
	if err != nil || string(dlqMessage.Headers[redisStreamDLQHeaderSourceMessageID]) != poisonID {
		t.Fatalf("DLQ message = (%#v, %v)", dlqMessage, err)
	}
	if _, exists, err := store.PendingEntry(ctx, stream, group, poisonID); err != nil || exists {
		t.Fatalf("source pending after DLQ = (%v, %v)", exists, err)
	}
}
