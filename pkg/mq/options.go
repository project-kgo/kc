package mq

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrInvalidSubscribeOption 表示订阅选项的值非法。
	ErrInvalidSubscribeOption = errors.New("mq: invalid subscribe option")
	// ErrUnsupportedSubscribeOption 表示当前 MQ 实现不支持指定的订阅选项。
	ErrUnsupportedSubscribeOption = errors.New("mq: unsupported subscribe option")
)

// RetryBackoff 描述发生基础设施错误后的指数退避区间。
type RetryBackoff struct {
	Min time.Duration
	Max time.Duration
}

// RedisStreamSubscribeOptions 包含仅适用于 Redis Streams 的订阅参数。
type RedisStreamSubscribeOptions struct {
	QueueDepth          *int
	PendingIdleTimeout  *time.Duration
	RedeliverInterval   *time.Duration
	AckBatchSize        *int
	AckFlushInterval    *time.Duration
	ReclaimMaxBatches   *int
	MaxDeliveryAttempts *int
}

// SubscribeOptions 是 SubscribeOption 解析后的覆盖配置。
// 指针为 nil 表示沿用客户端初始化时的默认值。
type SubscribeOptions struct {
	HandlerTimeout *time.Duration
	BatchSize      *int
	Concurrency    *int
	RetryBackoff   *RetryBackoff
	RedisStream    *RedisStreamSubscribeOptions
}

// SubscribeOption 配置单次订阅。重复设置同一选项时，后设置的值生效。
// 调用方只能通过本包提供的 With 系列函数创建选项，具体 MQ 实现负责判断后端专属选项是否受支持。
type SubscribeOption interface {
	apply(*SubscribeOptions) error
}

type subscribeOptionFunc func(*SubscribeOptions) error

func (option subscribeOptionFunc) apply(options *SubscribeOptions) error {
	if option == nil {
		return fmt.Errorf("%w: option function is nil", ErrInvalidSubscribeOption)
	}
	return option(options)
}

// ResolveSubscribeOptions 解析订阅选项，供具体 MQ 实现使用。
func ResolveSubscribeOptions(options ...SubscribeOption) (SubscribeOptions, error) {
	var resolved SubscribeOptions
	for index, option := range options {
		if option == nil {
			return SubscribeOptions{}, fmt.Errorf("%w: option %d is nil", ErrInvalidSubscribeOption, index)
		}
		if err := option.apply(&resolved); err != nil {
			return SubscribeOptions{}, err
		}
	}
	return resolved, nil
}

// WithHandlerTimeout 覆盖单条消息的 Handler 超时。
func WithHandlerTimeout(timeout time.Duration) SubscribeOption {
	return subscribeOptionFunc(func(options *SubscribeOptions) error {
		if timeout <= 0 {
			return fmt.Errorf("%w: handler timeout must be positive", ErrInvalidSubscribeOption)
		}
		options.HandlerTimeout = durationPointer(timeout)
		return nil
	})
}

// WithBatchSize 覆盖单次从消息中间件拉取的最大消息数。
func WithBatchSize(size int) SubscribeOption {
	return subscribeOptionFunc(func(options *SubscribeOptions) error {
		if size <= 0 {
			return fmt.Errorf("%w: batch size must be positive", ErrInvalidSubscribeOption)
		}
		options.BatchSize = intPointer(size)
		return nil
	})
}

// WithConcurrency 覆盖本次订阅允许同时执行的最大任务数。
func WithConcurrency(concurrency int) SubscribeOption {
	return subscribeOptionFunc(func(options *SubscribeOptions) error {
		if concurrency <= 0 {
			return fmt.Errorf("%w: concurrency must be positive", ErrInvalidSubscribeOption)
		}
		options.Concurrency = intPointer(concurrency)
		return nil
	})
}

// WithRetryBackoff 覆盖基础设施错误的指数退避区间。
func WithRetryBackoff(minimum, maximum time.Duration) SubscribeOption {
	return subscribeOptionFunc(func(options *SubscribeOptions) error {
		if minimum <= 0 || maximum <= 0 {
			return fmt.Errorf("%w: retry backoff must be positive", ErrInvalidSubscribeOption)
		}
		if maximum < minimum {
			return fmt.Errorf("%w: retry max backoff is less than retry backoff", ErrInvalidSubscribeOption)
		}
		options.RetryBackoff = &RetryBackoff{Min: minimum, Max: maximum}
		return nil
	})
}

// WithRedisStreamQueueDepth 覆盖 Redis Streams 本地有界队列容量。
func WithRedisStreamQueueDepth(depth int) SubscribeOption {
	return subscribeOptionFunc(func(options *SubscribeOptions) error {
		if depth <= 0 {
			return fmt.Errorf("%w: redis stream queue depth must be positive", ErrInvalidSubscribeOption)
		}
		redisOptions(options).QueueDepth = intPointer(depth)
		return nil
	})
}

// WithRedisStreamPendingIdleTimeout 覆盖 Redis Streams Pending 消息可被接管前的空闲时间。
func WithRedisStreamPendingIdleTimeout(timeout time.Duration) SubscribeOption {
	return subscribeOptionFunc(func(options *SubscribeOptions) error {
		if timeout <= 0 {
			return fmt.Errorf("%w: redis stream pending idle timeout must be positive", ErrInvalidSubscribeOption)
		}
		redisOptions(options).PendingIdleTimeout = durationPointer(timeout)
		return nil
	})
}

// WithRedisStreamRedeliverInterval 覆盖 Redis Streams 扫描 Pending 消息的间隔。
func WithRedisStreamRedeliverInterval(interval time.Duration) SubscribeOption {
	return subscribeOptionFunc(func(options *SubscribeOptions) error {
		if interval <= 0 {
			return fmt.Errorf("%w: redis stream redeliver interval must be positive", ErrInvalidSubscribeOption)
		}
		redisOptions(options).RedeliverInterval = durationPointer(interval)
		return nil
	})
}

// WithRedisStreamRedelivery 同时覆盖 Pending 接管阈值和扫描间隔。
func WithRedisStreamRedelivery(pendingIdleTimeout, interval time.Duration) SubscribeOption {
	return subscribeOptionFunc(func(options *SubscribeOptions) error {
		if pendingIdleTimeout <= 0 || interval <= 0 {
			return fmt.Errorf("%w: redis stream redelivery durations must be positive", ErrInvalidSubscribeOption)
		}
		redis := redisOptions(options)
		redis.PendingIdleTimeout = durationPointer(pendingIdleTimeout)
		redis.RedeliverInterval = durationPointer(interval)
		return nil
	})
}

// WithRedisStreamAckBatch 覆盖 Redis Streams 异步 ACK 的批量大小和刷新间隔。
func WithRedisStreamAckBatch(size int, flushInterval time.Duration) SubscribeOption {
	return subscribeOptionFunc(func(options *SubscribeOptions) error {
		if size <= 0 || flushInterval <= 0 {
			return fmt.Errorf("%w: redis stream ack batch values must be positive", ErrInvalidSubscribeOption)
		}
		redis := redisOptions(options)
		redis.AckBatchSize = intPointer(size)
		redis.AckFlushInterval = durationPointer(flushInterval)
		return nil
	})
}

// WithRedisStreamReclaimMaxBatches 覆盖 Redis Streams 单轮最多执行的 XAUTOCLAIM 次数。
func WithRedisStreamReclaimMaxBatches(limit int) SubscribeOption {
	return subscribeOptionFunc(func(options *SubscribeOptions) error {
		if limit <= 0 {
			return fmt.Errorf("%w: redis stream reclaim max batches must be positive", ErrInvalidSubscribeOption)
		}
		redisOptions(options).ReclaimMaxBatches = intPointer(limit)
		return nil
	})
}

// WithRedisStreamMaxDeliveryAttempts 覆盖消息进入自动 DLQ 前的最大投递次数。
func WithRedisStreamMaxDeliveryAttempts(attempts int) SubscribeOption {
	return subscribeOptionFunc(func(options *SubscribeOptions) error {
		if attempts <= 0 {
			return fmt.Errorf("%w: redis stream max delivery attempts must be positive", ErrInvalidSubscribeOption)
		}
		redisOptions(options).MaxDeliveryAttempts = intPointer(attempts)
		return nil
	})
}

func redisOptions(options *SubscribeOptions) *RedisStreamSubscribeOptions {
	if options.RedisStream == nil {
		options.RedisStream = &RedisStreamSubscribeOptions{}
	}
	return options.RedisStream
}

func intPointer(value int) *int {
	return &value
}

func durationPointer(value time.Duration) *time.Duration {
	return &value
}
