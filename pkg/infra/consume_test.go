package infra

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	coremq "github.com/project-kgo/kc/pkg/mq"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

// consumeStopCase 描述 Consume 优雅停止的公共断言：取消 Consume 的 ctx 不影响订阅，
// Stop 等进行中的 Handler 跑完并确认后才返回。
type consumeStopCase struct {
	consume func(context.Context, coremq.Handler) (coremq.Subscription, error)
	// deliver 投递一条消息，返回该消息确认后的 ID 列表读取函数。
	deliver func() func() []string
	wantAck string
}

func runConsumeStopWaitsForHandler(t *testing.T, c consumeStopCase) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	release := make(chan struct{})
	handlerErr := make(chan error, 1)
	subscription, err := c.consume(ctx, func(ctx context.Context, _ *coremq.Message) error {
		close(started)
		<-release
		handlerErr <- ctx.Err()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	acked := c.deliver()
	waitForSignal(t, started)
	cancel()

	stopped := make(chan error, 1)
	go func() { stopped <- subscription.Stop(context.Background()) }()
	select {
	case err := <-stopped:
		t.Fatalf("Stop returned before handler finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatalf("Stop = %v", err)
	}
	if err := <-handlerErr; err != nil {
		t.Fatalf("handler context canceled during graceful stop: %v", err)
	}
	if ids := acked(); !reflect.DeepEqual(ids, []string{c.wantAck}) {
		t.Fatalf("acked IDs = %v, want %v", ids, []string{c.wantAck})
	}
	waitForSignal(t, subscription.Done())
}

func runConsumeStopCancelsAfterDeadline(t *testing.T, c consumeStopCase) {
	t.Helper()
	started := make(chan struct{})
	cause := make(chan error, 1)
	subscription, err := c.consume(context.Background(), func(ctx context.Context, _ *coremq.Message) error {
		close(started)
		<-ctx.Done()
		cause <- context.Cause(ctx)
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	acked := c.deliver()
	waitForSignal(t, started)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := subscription.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v, want deadline exceeded", err)
	}
	if err := <-cause; !errors.Is(err, coremq.ErrClosed) {
		t.Fatalf("handler cancel cause = %v", err)
	}
	if ids := acked(); len(ids) != 0 {
		t.Fatalf("canceled message was acked: %v", ids)
	}
	// 重复 Stop 立即返回。
	if err := subscription.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop = %v", err)
	}
}

func kafkaConsumeStopCase(t *testing.T) consumeStopCase {
	t.Helper()
	consumer := newFakeKafkaConsumer()
	hooks := make(chan kafkaGroupHooks, 1)
	mq := newKafkaMQ(&fakeKafkaProducer{}, func(_, _ string, _ kafkaSubscriptionConfig, setup kafkaConsumerSetup) (kafkaConsumer, error) {
		hooks <- setup.hooks
		return consumer, nil
	}, kafkaConsumerGroup, testKafkaRuntimeConfig(kafkaConsumerGroup))
	t.Cleanup(func() { _ = mq.Close() })
	record := &kgo.Record{Topic: "orders", Partition: 0, Offset: 1}
	return consumeStopCase{
		consume: func(ctx context.Context, handler coremq.Handler) (coremq.Subscription, error) {
			return mq.Consume(ctx, "orders", "billing", handler)
		},
		deliver: func() func() []string {
			(<-hooks).assigned(map[string][]int32{"orders": {0}})
			consumer.push(fetchKafkaRecords(record))
			return consumer.markedIDs
		},
		wantAck: kafkaMessageID(record),
	}
}

func redisStreamConsumeStopCase(t *testing.T) consumeStopCase {
	t.Helper()
	var delivered atomic.Bool
	store := &fakeRedisStreamStore{}
	record := testRedisStreamRecord(t, "1-0", &coremq.Message{})
	store.readFunc = func(ctx context.Context, _ string, _ string, _ string, _ int64, _ time.Duration) ([]redis.XMessage, error) {
		if delivered.CompareAndSwap(false, true) {
			return []redis.XMessage{record}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	mq := newRedisStreamMQ(store, testRedisStreamRuntimeConfig())
	t.Cleanup(func() { _ = mq.Close() })
	return consumeStopCase{
		consume: func(ctx context.Context, handler coremq.Handler) (coremq.Subscription, error) {
			return mq.Consume(ctx, "orders", "billing", handler)
		},
		deliver: func() func() []string { return store.ackedIDs },
		wantAck: record.ID,
	}
}

func TestKafkaConsumeStopWaitsForHandlerAndCommits(t *testing.T) {
	runConsumeStopWaitsForHandler(t, kafkaConsumeStopCase(t))
}

func TestKafkaConsumeStopCancelsHandlerAfterDeadline(t *testing.T) {
	runConsumeStopCancelsAfterDeadline(t, kafkaConsumeStopCase(t))
}

func TestRedisStreamConsumeStopWaitsForHandlerAndAcks(t *testing.T) {
	runConsumeStopWaitsForHandler(t, redisStreamConsumeStopCase(t))
}

func TestRedisStreamConsumeStopCancelsHandlerAfterDeadline(t *testing.T) {
	runConsumeStopCancelsAfterDeadline(t, redisStreamConsumeStopCase(t))
}

func TestCloseStopsConsumeSubscriptions(t *testing.T) {
	mq := newRedisStreamMQ(&fakeRedisStreamStore{}, testRedisStreamRuntimeConfig())
	subscription, err := mq.Consume(context.Background(), "orders", "billing", func(context.Context, *coremq.Message) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := mq.Close(); err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, subscription.Done())
	if _, err := mq.Consume(context.Background(), "orders", "billing", func(context.Context, *coremq.Message) error { return nil }); !errors.Is(err, coremq.ErrClosed) {
		t.Fatalf("Consume after Close = %v", err)
	}
}
