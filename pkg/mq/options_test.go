package mq

import (
	"errors"
	"testing"
	"time"
)

func TestResolveSubscribeOptions(t *testing.T) {
	options, err := ResolveSubscribeOptions(
		WithHandlerTimeout(5*time.Second),
		WithBatchSize(32),
		WithConcurrency(4),
		WithRetryBackoff(100*time.Millisecond, 3*time.Second),
		WithKafkaMaxRetries(3),
		WithKafkaShareMaxDeliveryAttempts(4),
		WithRedisStreamQueueDepth(128),
		WithRedisStreamRedelivery(time.Minute, 10*time.Second),
		WithRedisStreamAckBatch(64, 2*time.Millisecond),
		WithRedisStreamReclaimMaxBatches(4),
		WithRedisStreamMaxDeliveryAttempts(5),
	)
	if err != nil {
		t.Fatalf("ResolveSubscribeOptions() error = %v", err)
	}
	if *options.HandlerTimeout != 5*time.Second || *options.BatchSize != 32 || *options.Concurrency != 4 {
		t.Fatalf("generic options = %+v", options)
	}
	if options.RetryBackoff.Min != 100*time.Millisecond || options.RetryBackoff.Max != 3*time.Second {
		t.Fatalf("retry backoff = %+v", options.RetryBackoff)
	}
	if *options.Kafka.MaxRetries != 3 || *options.Kafka.MaxDeliveryAttempts != 4 {
		t.Fatalf("kafka options = %+v", options.Kafka)
	}
	if *options.RedisStream.QueueDepth != 128 || *options.RedisStream.PendingIdleTimeout != time.Minute || *options.RedisStream.RedeliverInterval != 10*time.Second {
		t.Fatalf("redis stream options = %+v", options.RedisStream)
	}
	if *options.RedisStream.AckBatchSize != 64 || *options.RedisStream.AckFlushInterval != 2*time.Millisecond ||
		*options.RedisStream.ReclaimMaxBatches != 4 || *options.RedisStream.MaxDeliveryAttempts != 5 {
		t.Fatalf("redis stream reliability options = %+v", options.RedisStream)
	}
}

func TestResolveSubscribeOptionsLastWins(t *testing.T) {
	options, err := ResolveSubscribeOptions(WithConcurrency(1), WithConcurrency(8))
	if err != nil {
		t.Fatalf("ResolveSubscribeOptions() error = %v", err)
	}
	if got := *options.Concurrency; got != 8 {
		t.Fatalf("Concurrency = %d, want 8", got)
	}
}

func TestResolveSubscribeOptionsRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name   string
		option SubscribeOption
	}{
		{name: "nil option", option: nil},
		{name: "typed nil option", option: subscribeOptionFunc(nil)},
		{name: "handler timeout", option: WithHandlerTimeout(0)},
		{name: "batch size", option: WithBatchSize(0)},
		{name: "concurrency", option: WithConcurrency(-1)},
		{name: "retry minimum", option: WithRetryBackoff(0, time.Second)},
		{name: "retry order", option: WithRetryBackoff(time.Second, time.Millisecond)},
		{name: "kafka max retries", option: WithKafkaMaxRetries(-1)},
		{name: "kafka share attempts", option: WithKafkaShareMaxDeliveryAttempts(0)},
		{name: "queue depth", option: WithRedisStreamQueueDepth(0)},
		{name: "pending idle", option: WithRedisStreamPendingIdleTimeout(0)},
		{name: "redeliver interval", option: WithRedisStreamRedeliverInterval(0)},
		{name: "redelivery", option: WithRedisStreamRedelivery(time.Second, 0)},
		{name: "ack batch size", option: WithRedisStreamAckBatch(0, time.Millisecond)},
		{name: "ack flush interval", option: WithRedisStreamAckBatch(1, 0)},
		{name: "reclaim max batches", option: WithRedisStreamReclaimMaxBatches(0)},
		{name: "max delivery attempts", option: WithRedisStreamMaxDeliveryAttempts(0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ResolveSubscribeOptions(test.option); !errors.Is(err, ErrInvalidSubscribeOption) {
				t.Fatalf("ResolveSubscribeOptions() error = %v, want ErrInvalidSubscribeOption", err)
			}
		})
	}
}
