package infra

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	coremq "github.com/project-kgo/kc/pkg/mq"
	"github.com/project-kgo/kc/pkg/resource"
	"github.com/redis/go-redis/v9"
)

var _ coremq.MQ = (*redisStreamMQ)(nil)

const (
	defaultRedisStreamKeyPrefix          = "kc:mq"
	defaultRedisStreamConsumerBatchSize  = int64(64)
	defaultRedisStreamQueueDepth         = 64
	defaultRedisStreamConcurrency        = 10
	defaultRedisStreamGroupStartID       = "0"
	defaultRedisStreamReadBlock          = time.Second
	defaultRedisStreamHandlerTimeout     = 30 * time.Second
	defaultRedisStreamPendingIdleTimeout = time.Minute
	defaultRedisStreamRedeliverInterval  = 15 * time.Second
	defaultRedisStreamRetryBackoff       = time.Second
	defaultRedisStreamRetryMaxBackoff    = 30 * time.Second
	redisStreamMaxBatchSize              = int64(10000)
	redisStreamMaxQueueDepth             = 10000
	redisStreamMaxConcurrency            = 10000
	redisStreamMessageVersion            = "1"
	redisStreamFieldVersion              = "v"
	redisStreamFieldKey                  = "key"
	redisStreamFieldBody                 = "body"
	redisStreamFieldHeaders              = "headers"
	redisStreamFieldTimestamp            = "timestamp"
)

type redisStreamRuntimeConfig struct {
	keyPrefix          string
	batchSize          int64
	queueDepth         int
	concurrency        int
	consumerID         string
	groupStartID       string
	readBlock          time.Duration
	handlerTimeout     time.Duration
	pendingIdleTimeout time.Duration
	redeliverInterval  time.Duration
	retryBackoff       time.Duration
	retryMaxBackoff    time.Duration
	maxLen             int64
	logger             *slog.Logger
}

type redisStreamMQState uint8

const (
	redisStreamMQOpen redisStreamMQState = iota
	redisStreamMQClosing
	redisStreamMQClosed
)

type redisStreamSubscription struct {
	stop context.CancelFunc
	done chan struct{}

	queue chan redis.XMessage
	mu    sync.Mutex
	// inFlight 同时覆盖已经入队和正在执行的消息，避免 poll 与 reclaim 重复入队。
	inFlight map[string]struct{}
}

func (s *redisStreamSubscription) enqueue(ctx context.Context, message redis.XMessage) bool {
	s.mu.Lock()
	if _, exists := s.inFlight[message.ID]; exists {
		s.mu.Unlock()
		return true
	}
	s.inFlight[message.ID] = struct{}{}
	s.mu.Unlock()

	select {
	case s.queue <- message:
		return true
	case <-ctx.Done():
		s.release(message.ID)
		return false
	}
}

func (s *redisStreamSubscription) release(messageID string) {
	s.mu.Lock()
	delete(s.inFlight, messageID)
	s.mu.Unlock()
}

type redisStreamMQ struct {
	store  redisStreamStore
	config redisStreamRuntimeConfig

	mu            sync.Mutex
	state         redisStreamMQState
	subscriptions map[*redisStreamSubscription]struct{}
	operations    sync.WaitGroup
	closeDone     chan struct{}
	closeErr      error
}

type redisStreamStore interface {
	Close() error
	Add(context.Context, string, []interface{}, int64) (string, error)
	EnsureGroup(context.Context, string, string, string) error
	ReadGroup(context.Context, string, string, string, int64, time.Duration) ([]redis.XMessage, error)
	AutoClaim(context.Context, string, string, string, time.Duration, string, int64) ([]redis.XMessage, string, error)
	Ack(context.Context, string, string, ...string) (int64, error)
}

type redisStreamRedisStore struct {
	client redis.UniversalClient
}

func newRedisStreamMQ(store redisStreamStore, config redisStreamRuntimeConfig) *redisStreamMQ {
	return &redisStreamMQ{
		store:         store,
		config:        config,
		state:         redisStreamMQOpen,
		subscriptions: make(map[*redisStreamSubscription]struct{}),
		closeDone:     make(chan struct{}),
	}
}

func (m *redisStreamMQ) Publish(ctx context.Context, topic string, message *coremq.Message) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", coremq.ErrInvalidArgument)
	}
	if strings.TrimSpace(topic) == "" {
		return fmt.Errorf("%w: topic is empty", coremq.ErrInvalidArgument)
	}
	if message == nil {
		return fmt.Errorf("%w: message is nil", coremq.ErrInvalidArgument)
	}
	if !m.beginOperation() {
		return coremq.ErrClosed
	}
	defer m.endOperation()

	values, err := marshalRedisStreamMessage(message)
	if err != nil {
		return fmt.Errorf("marshal redis stream message: %w", err)
	}
	if _, err := m.store.Add(ctx, m.streamKey(topic), values, m.config.maxLen); err != nil {
		return wrapRedisStreamError("publish redis stream message", err)
	}
	return nil
}

func (m *redisStreamMQ) Subscribe(
	ctx context.Context,
	topic string,
	group string,
	handler coremq.Handler,
	options ...coremq.SubscribeOption,
) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", coremq.ErrInvalidArgument)
	}
	if strings.TrimSpace(topic) == "" {
		return fmt.Errorf("%w: topic is empty", coremq.ErrInvalidArgument)
	}
	if strings.TrimSpace(group) == "" {
		return fmt.Errorf("%w: consumer group is empty", coremq.ErrInvalidArgument)
	}
	if handler == nil {
		return fmt.Errorf("%w: handler is nil", coremq.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("subscribe redis stream: %w", err)
	}
	subscriptionConfig, err := m.subscriptionConfig(options...)
	if err != nil {
		return fmt.Errorf("subscribe redis stream: %w", err)
	}
	if !m.beginOperation() {
		return coremq.ErrClosed
	}
	defer m.endOperation()

	stream := m.streamKey(topic)
	if err := m.store.EnsureGroup(ctx, stream, group, m.config.groupStartID); err != nil {
		return fmt.Errorf("create redis stream group: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("subscribe redis stream: %w", err)
	}

	pollCtx, stop := context.WithCancel(ctx)
	subscription := &redisStreamSubscription{
		stop:     stop,
		done:     make(chan struct{}),
		queue:    make(chan redis.XMessage, subscriptionConfig.queueDepth),
		inFlight: make(map[string]struct{}),
	}
	if !m.trackSubscription(subscription) {
		stop()
		return coremq.ErrClosed
	}

	consumer := m.subscriptionConsumerID()
	go m.runSubscription(pollCtx, ctx, stream, group, consumer, handler, subscriptionConfig, subscription)
	return nil
}

func (m *redisStreamMQ) Close() error {
	m.mu.Lock()
	if m.state != redisStreamMQOpen {
		done := m.closeDone
		m.mu.Unlock()
		<-done
		m.mu.Lock()
		err := m.closeErr
		m.mu.Unlock()
		return err
	}
	m.state = redisStreamMQClosing
	subscriptions := make([]*redisStreamSubscription, 0, len(m.subscriptions))
	for subscription := range m.subscriptions {
		subscription.stop()
		subscriptions = append(subscriptions, subscription)
	}
	m.mu.Unlock()

	// state 已经变为 closing，不会再有新的 operations.Add 与 Wait 并发发生。
	m.operations.Wait()
	for _, subscription := range subscriptions {
		<-subscription.done
	}
	closeErr := m.store.Close()

	m.mu.Lock()
	m.closeErr = closeErr
	m.state = redisStreamMQClosed
	close(m.closeDone)
	m.mu.Unlock()
	return closeErr
}

func (m *redisStreamMQ) beginOperation() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state != redisStreamMQOpen {
		return false
	}
	m.operations.Add(1)
	return true
}

func (m *redisStreamMQ) endOperation() {
	m.operations.Done()
}

func (m *redisStreamMQ) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state != redisStreamMQOpen
}

func (m *redisStreamMQ) trackSubscription(subscription *redisStreamSubscription) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state != redisStreamMQOpen {
		return false
	}
	m.subscriptions[subscription] = struct{}{}
	return true
}

func (m *redisStreamMQ) untrackSubscription(subscription *redisStreamSubscription) {
	m.mu.Lock()
	delete(m.subscriptions, subscription)
	m.mu.Unlock()
}

func (m *redisStreamMQ) runSubscription(
	pollCtx context.Context,
	subscriptionCtx context.Context,
	stream string,
	group string,
	consumer string,
	handler coremq.Handler,
	config redisStreamRuntimeConfig,
	subscription *redisStreamSubscription,
) {
	defer func() {
		m.untrackSubscription(subscription)
		close(subscription.done)
	}()

	var loops sync.WaitGroup
	loops.Add(2 + config.concurrency)
	go func() {
		defer loops.Done()
		m.pollRedisStreamMessages(pollCtx, stream, group, consumer, config, subscription)
	}()
	go func() {
		defer loops.Done()
		m.reclaimRedisStreamMessages(pollCtx, stream, group, consumer, config, subscription)
	}()
	for range config.concurrency {
		go func() {
			defer loops.Done()
			m.runRedisStreamWorker(pollCtx, subscriptionCtx, stream, group, handler, config, subscription)
		}()
	}

	<-pollCtx.Done()
	loops.Wait()
}

func (m *redisStreamMQ) pollRedisStreamMessages(
	ctx context.Context,
	stream string,
	group string,
	consumer string,
	config redisStreamRuntimeConfig,
	subscription *redisStreamSubscription,
) {
	retryAttempt := 0
	for ctx.Err() == nil {
		messages, err := m.store.ReadGroup(ctx, stream, group, consumer, config.batchSize, config.readBlock)
		if err != nil {
			if redisStreamCommandCanceled(ctx, err) {
				return
			}
			if redisStreamNoGroup(err) {
				err = m.store.EnsureGroup(ctx, stream, group, "0")
				if err == nil {
					retryAttempt = 0
					continue
				}
				if redisStreamCommandCanceled(ctx, err) {
					return
				}
			}
			if !waitRedisStreamRetry(ctx, redisStreamRetryDelay(config.retryBackoff, config.retryMaxBackoff, retryAttempt)) {
				return
			}
			retryAttempt++
			continue
		}
		retryAttempt = 0

		for _, message := range messages {
			if !subscription.enqueue(ctx, message) {
				return
			}
		}
	}
}

func (m *redisStreamMQ) reclaimRedisStreamMessages(
	ctx context.Context,
	stream string,
	group string,
	consumer string,
	config redisStreamRuntimeConfig,
	subscription *redisStreamSubscription,
) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	retryAttempt := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		err := m.reclaimRedisStreamPending(ctx, stream, group, consumer, config, subscription)
		if redisStreamCommandCanceled(ctx, err) {
			return
		}
		if redisStreamNoGroup(err) {
			err = m.store.EnsureGroup(ctx, stream, group, "0")
			if redisStreamCommandCanceled(ctx, err) {
				return
			}
		}

		delay := config.redeliverInterval
		if err != nil {
			delay = redisStreamRetryDelay(config.retryBackoff, config.retryMaxBackoff, retryAttempt)
			retryAttempt++
		} else {
			retryAttempt = 0
		}

		timer.Reset(delay)
	}
}

func (m *redisStreamMQ) reclaimRedisStreamPending(
	ctx context.Context,
	stream string,
	group string,
	consumer string,
	config redisStreamRuntimeConfig,
	subscription *redisStreamSubscription,
) error {
	start := "0-0"
	for ctx.Err() == nil {
		claimed, nextStart, err := m.store.AutoClaim(
			ctx, stream, group, consumer, config.pendingIdleTimeout, start, config.batchSize,
		)
		if err != nil {
			return err
		}
		for _, message := range claimed {
			if !subscription.enqueue(ctx, message) {
				return ctx.Err()
			}
		}

		// Redis 7+ 会在 XAUTOCLAIM 扫描时自动清理载荷已裁剪的 PEL 条目。
		if nextStart == "0-0" {
			return nil
		}
		if nextStart == "" || nextStart == start {
			return fmt.Errorf("redis stream XAUTOCLAIM returned invalid cursor %q", nextStart)
		}
		start = nextStart
	}
	return ctx.Err()
}

func (m *redisStreamMQ) runRedisStreamWorker(
	pollCtx context.Context,
	subscriptionCtx context.Context,
	stream string,
	group string,
	handler coremq.Handler,
	config redisStreamRuntimeConfig,
	subscription *redisStreamSubscription,
) {
	for {
		if pollCtx.Err() != nil {
			return
		}
		select {
		case <-pollCtx.Done():
			return
		case record := <-subscription.queue:
			// Close 或订阅取消后，即使 select 同时选中了队列，也不再启动新 handler。
			if pollCtx.Err() != nil {
				subscription.release(record.ID)
				return
			}
			func() {
				defer subscription.release(record.ID)
				m.processRedisStreamMessage(subscriptionCtx, stream, group, handler, record, config)
			}()
		}
	}
}

func (m *redisStreamMQ) processRedisStreamMessage(
	subscriptionCtx context.Context,
	stream string,
	group string,
	handler coremq.Handler,
	record redis.XMessage,
	config redisStreamRuntimeConfig,
) {
	message, err := messageFromRedisStreamRecord(record)
	if err != nil {
		logger := config.logger
		if logger == nil {
			logger = slog.Default()
		}
		logCtx := context.WithoutCancel(subscriptionCtx)
		logger.ErrorContext(logCtx, "丢弃无法解码的 Redis Stream 消息",
			"stream", stream,
			"group", group,
			"message_id", record.ID,
			"error", err,
		)
		if ackErr := m.ackRedisStreamMessage(subscriptionCtx, stream, group, record.ID, config.retryMaxBackoff); ackErr != nil {
			// ACK 失败时消息仍保留在 PEL，后续会再次领取并重试丢弃。
			logger.ErrorContext(logCtx, "确认无法解码的 Redis Stream 消息失败",
				"stream", stream,
				"group", group,
				"message_id", record.ID,
				"error", ackErr,
			)
		}
		return
	}
	if err := m.handleRedisStreamMessage(subscriptionCtx, handler, message, config.handlerTimeout); err != nil {
		return
	}

	_ = m.ackRedisStreamMessage(subscriptionCtx, stream, group, record.ID, config.retryMaxBackoff)
}

func (m *redisStreamMQ) ackRedisStreamMessage(
	parent context.Context,
	stream string,
	group string,
	messageID string,
	timeout time.Duration,
) error {
	// handler 已经成功或消息已确定无法解码，ACK 不再继承随后发生的关闭取消。
	ackCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
	defer cancel()
	_, err := m.store.Ack(ackCtx, stream, group, messageID)
	return err
}

func (m *redisStreamMQ) handleRedisStreamMessage(
	parent context.Context,
	handler coremq.Handler,
	message *coremq.Message,
	handlerTimeout time.Duration,
) (err error) {
	handlerCtx, cancel := context.WithTimeout(parent, handlerTimeout)
	defer func() {
		cancel()
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("handler panic: %v", recovered)
		}
	}()
	if ctxErr := handlerCtx.Err(); ctxErr != nil {
		return ctxErr
	}

	err = handler(handlerCtx, message)
	if err != nil {
		return err
	}
	// handler 即使吞掉取消或超时，也不能确认已经失效的处理结果。
	if ctxErr := handlerCtx.Err(); ctxErr != nil {
		return ctxErr
	}
	return nil
}

func (m *redisStreamMQ) subscriptionConfig(options ...coremq.SubscribeOption) (redisStreamRuntimeConfig, error) {
	resolved, err := coremq.ResolveSubscribeOptions(options...)
	if err != nil {
		return redisStreamRuntimeConfig{}, err
	}

	// 每次订阅复制一份运行配置，避免不同订阅之间互相污染。
	config := m.config
	if resolved.BatchSize != nil {
		config.batchSize = int64(*resolved.BatchSize)
	}
	if resolved.HandlerTimeout != nil {
		config.handlerTimeout = *resolved.HandlerTimeout
	}
	if resolved.Concurrency != nil {
		config.concurrency = *resolved.Concurrency
	}
	if resolved.RetryBackoff != nil {
		config.retryBackoff = resolved.RetryBackoff.Min
		config.retryMaxBackoff = resolved.RetryBackoff.Max
	}
	if resolved.RedisStream != nil {
		if resolved.RedisStream.QueueDepth != nil {
			config.queueDepth = *resolved.RedisStream.QueueDepth
		}
		if resolved.RedisStream.PendingIdleTimeout != nil {
			config.pendingIdleTimeout = *resolved.RedisStream.PendingIdleTimeout
		}
		if resolved.RedisStream.RedeliverInterval != nil {
			config.redeliverInterval = *resolved.RedisStream.RedeliverInterval
		}
	}

	if config.batchSize > redisStreamMaxBatchSize {
		return redisStreamRuntimeConfig{}, fmt.Errorf(
			"%w: redis stream batch size exceeds %d", coremq.ErrInvalidSubscribeOption, redisStreamMaxBatchSize,
		)
	}
	if config.queueDepth > redisStreamMaxQueueDepth {
		return redisStreamRuntimeConfig{}, fmt.Errorf(
			"%w: redis stream queue depth exceeds %d", coremq.ErrInvalidSubscribeOption, redisStreamMaxQueueDepth,
		)
	}
	if config.concurrency > redisStreamMaxConcurrency {
		return redisStreamRuntimeConfig{}, fmt.Errorf(
			"%w: redis stream concurrency exceeds %d", coremq.ErrInvalidSubscribeOption, redisStreamMaxConcurrency,
		)
	}
	if config.pendingIdleTimeout < config.handlerTimeout {
		return redisStreamRuntimeConfig{}, fmt.Errorf(
			"%w: redis stream pending idle timeout is less than handler timeout", coremq.ErrInvalidSubscribeOption,
		)
	}
	return config, nil
}

func (m *redisStreamMQ) streamKey(topic string) string {
	return fmt.Sprintf("%s:stream:%s", m.config.keyPrefix, redisStreamKeyToken(topic))
}

func (m *redisStreamMQ) subscriptionConsumerID() string {
	prefix := strings.TrimSpace(m.config.consumerID)
	if prefix == "" {
		hostname, err := os.Hostname()
		if err != nil || hostname == "" {
			hostname = "unknown-host"
		}
		prefix = fmt.Sprintf("%s:%d", hostname, os.Getpid())
	}
	return randomRedisStreamID(prefix + ":")
}

func redisStreamKeyToken(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func randomRedisStreamID(prefix string) string {
	var data [16]byte
	if _, err := rand.Read(data[:]); err == nil {
		return prefix + fmt.Sprintf("%x", data[:])
	}
	return prefix + strconv.FormatInt(time.Now().UnixNano(), 36)
}

func waitRedisStreamRetry(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func redisStreamRetryDelay(base, maximum time.Duration, attempt int) time.Duration {
	delay := base
	for range attempt {
		if delay >= maximum || delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	return delay
}

func redisStreamNoGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOGROUP")
}

func redisStreamCommandCanceled(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func wrapRedisStreamError(operation string, err error) error {
	if errors.Is(err, redis.ErrClosed) {
		return fmt.Errorf("%s: %w: %w", operation, coremq.ErrClosed, err)
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func marshalRedisStreamMessage(message *coremq.Message) ([]interface{}, error) {
	headers, err := json.Marshal(message.Headers)
	if err != nil {
		return nil, err
	}
	timestamp := int64(0)
	if !message.Timestamp.IsZero() {
		timestamp = message.Timestamp.UnixNano()
	}
	return []interface{}{
		redisStreamFieldVersion, redisStreamMessageVersion,
		redisStreamFieldKey, cloneRedisStreamBytes(message.Key),
		redisStreamFieldBody, cloneRedisStreamBytes(message.Body),
		redisStreamFieldHeaders, headers,
		redisStreamFieldTimestamp, strconv.FormatInt(timestamp, 10),
	}, nil
}

func messageFromRedisStreamRecord(record redis.XMessage) (*coremq.Message, error) {
	if len(record.Values) == 0 {
		return nil, errors.New("redis stream message payload is missing")
	}
	version, err := redisStreamValueBytes(record.Values, redisStreamFieldVersion)
	if err != nil {
		return nil, err
	}
	if string(version) != redisStreamMessageVersion {
		return nil, fmt.Errorf("unsupported redis stream message version %q", version)
	}
	key, err := redisStreamValueBytes(record.Values, redisStreamFieldKey)
	if err != nil {
		return nil, err
	}
	body, err := redisStreamValueBytes(record.Values, redisStreamFieldBody)
	if err != nil {
		return nil, err
	}
	headerData, err := redisStreamValueBytes(record.Values, redisStreamFieldHeaders)
	if err != nil {
		return nil, err
	}
	headers := make(map[string][]byte)
	if len(headerData) > 0 && string(headerData) != "null" {
		if err := json.Unmarshal(headerData, &headers); err != nil {
			return nil, fmt.Errorf("decode redis stream headers: %w", err)
		}
	}
	timestampData, err := redisStreamValueBytes(record.Values, redisStreamFieldTimestamp)
	if err != nil {
		return nil, err
	}
	timestampNanos, err := strconv.ParseInt(string(timestampData), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("decode redis stream timestamp: %w", err)
	}
	timestamp := time.Time{}
	if timestampNanos != 0 {
		timestamp = time.Unix(0, timestampNanos)
	} else if split := strings.SplitN(record.ID, "-", 2); len(split) == 2 {
		if milliseconds, parseErr := strconv.ParseInt(split[0], 10, 64); parseErr == nil {
			timestamp = time.UnixMilli(milliseconds)
		}
	}
	return &coremq.Message{
		ID:        record.ID,
		Key:       cloneRedisStreamBytes(key),
		Body:      cloneRedisStreamBytes(body),
		Headers:   headers,
		Timestamp: timestamp,
	}, nil
}

func redisStreamValueBytes(values map[string]interface{}, key string) ([]byte, error) {
	value, ok := values[key]
	if !ok {
		return nil, fmt.Errorf("redis stream field %q is missing", key)
	}
	switch typed := value.(type) {
	case string:
		return []byte(typed), nil
	case []byte:
		return cloneRedisStreamBytes(typed), nil
	default:
		return nil, fmt.Errorf("redis stream field %q has type %T", key, value)
	}
}

func cloneRedisStreamBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}

func (s *redisStreamRedisStore) Close() error {
	return s.client.Close()
}

func (s *redisStreamRedisStore) Add(ctx context.Context, stream string, values []interface{}, maxLen int64) (string, error) {
	args := &redis.XAddArgs{Stream: stream, Values: values}
	if maxLen > 0 {
		args.MaxLen = maxLen
		args.Approx = true
	}
	return s.client.XAdd(ctx, args).Result()
}

func (s *redisStreamRedisStore) EnsureGroup(ctx context.Context, stream, group, startID string) error {
	err := s.client.XGroupCreateMkStream(ctx, stream, group, startID).Err()
	if err != nil && redis.HasErrorPrefix(err, "BUSYGROUP") {
		return nil
	}
	return err
}

func (s *redisStreamRedisStore) ReadGroup(
	ctx context.Context,
	stream string,
	group string,
	consumer string,
	count int64,
	block time.Duration,
) ([]redis.XMessage, error) {
	streams, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: consumer, Streams: []string{stream, ">"}, Count: count, Block: block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(streams) == 0 {
		return nil, nil
	}
	return streams[0].Messages, nil
}

func (s *redisStreamRedisStore) AutoClaim(
	ctx context.Context,
	stream string,
	group string,
	consumer string,
	minIdle time.Duration,
	start string,
	count int64,
) ([]redis.XMessage, string, error) {
	result, nextStart, err := s.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: stream, Group: group, Consumer: consumer, MinIdle: minIdle, Start: start, Count: count,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, "0-0", nil
	}
	return result, nextStart, err
}

func (s *redisStreamRedisStore) Ack(ctx context.Context, stream, group string, ids ...string) (int64, error) {
	return s.client.XAck(ctx, stream, group, ids...).Result()
}

func validateRedisStreamConfig(config MQConfig) error {
	if config.RedisStream == nil {
		return fmt.Errorf("%w: redis stream config is missing", ErrInvalidConfig)
	}
	streamConfig := config.RedisStream
	if streamConfig.ConsumerBatchSize < 0 || streamConfig.ConsumerBatchSize > redisStreamMaxBatchSize {
		return fmt.Errorf("%w: redis stream consumer batch size must be between 1 and %d", ErrInvalidConfig, redisStreamMaxBatchSize)
	}
	if streamConfig.QueueDepth < 0 || streamConfig.QueueDepth > redisStreamMaxQueueDepth {
		return fmt.Errorf("%w: redis stream queue depth must be between 1 and %d", ErrInvalidConfig, redisStreamMaxQueueDepth)
	}
	if streamConfig.Concurrency < 0 || streamConfig.Concurrency > redisStreamMaxConcurrency {
		return fmt.Errorf("%w: redis stream concurrency must be between 1 and %d", ErrInvalidConfig, redisStreamMaxConcurrency)
	}
	if streamConfig.MaxLen < 0 {
		return fmt.Errorf("%w: redis stream max len is negative", ErrInvalidConfig)
	}
	if streamConfig.ReadBlock < 0 || streamConfig.HandlerTimeout < 0 || streamConfig.PendingIdleTimeout < 0 || streamConfig.RedeliverInterval < 0 {
		return fmt.Errorf("%w: redis stream timeout is negative", ErrInvalidConfig)
	}
	if streamConfig.RetryBackoff < 0 || streamConfig.RetryMaxBackoff < 0 {
		return fmt.Errorf("%w: redis stream retry backoff is negative", ErrInvalidConfig)
	}
	if strings.ContainsAny(streamConfig.KeyPrefix, "\r\n\t") {
		return fmt.Errorf("%w: redis stream key prefix contains a control character", ErrInvalidConfig)
	}
	if streamConfig.KeyPrefix != "" && strings.TrimSpace(streamConfig.KeyPrefix) != streamConfig.KeyPrefix {
		return fmt.Errorf("%w: redis stream key prefix has surrounding whitespace", ErrInvalidConfig)
	}
	if streamConfig.ConsumerID != "" && strings.TrimSpace(streamConfig.ConsumerID) != streamConfig.ConsumerID {
		return fmt.Errorf("%w: redis stream consumer id has surrounding whitespace", ErrInvalidConfig)
	}

	runtimeConfig := redisStreamRuntimeConfigFrom(streamConfig)
	if strings.TrimSpace(runtimeConfig.keyPrefix) == "" {
		return fmt.Errorf("%w: redis stream key prefix is empty", ErrInvalidConfig)
	}
	if strings.TrimSpace(runtimeConfig.groupStartID) == "" || strings.TrimSpace(runtimeConfig.groupStartID) != runtimeConfig.groupStartID {
		return fmt.Errorf("%w: redis stream group start id is invalid", ErrInvalidConfig)
	}
	if !validRedisStreamStartID(runtimeConfig.groupStartID) {
		return fmt.Errorf("%w: redis stream group start id is invalid", ErrInvalidConfig)
	}
	if runtimeConfig.retryMaxBackoff < runtimeConfig.retryBackoff {
		return fmt.Errorf("%w: redis stream retry max backoff is less than retry backoff", ErrInvalidConfig)
	}
	if runtimeConfig.pendingIdleTimeout < runtimeConfig.handlerTimeout {
		return fmt.Errorf("%w: redis stream pending idle timeout is less than handler timeout", ErrInvalidConfig)
	}

	dataType := DataTypeRedis
	if config.Type == MQTypeRedisStreamCluster {
		dataType = DataTypeRedisCluster
	}
	if config.Redis != nil && config.Type == MQTypeRedisStreamCluster && config.Redis.ReadOnly {
		return fmt.Errorf("%w: redis stream cluster cannot be read only", ErrInvalidConfig)
	}
	return validateRedisConfig(DataConfig{Type: dataType, DSN: config.DSN, Redis: config.Redis})
}

func validRedisStreamStartID(value string) bool {
	if value == "$" {
		return true
	}
	parts := strings.Split(value, "-")
	if len(parts) < 1 || len(parts) > 2 || parts[0] == "" {
		return false
	}
	if _, err := strconv.ParseUint(parts[0], 10, 64); err != nil {
		return false
	}
	if len(parts) == 2 {
		if parts[1] == "" {
			return false
		}
		if _, err := strconv.ParseUint(parts[1], 10, 64); err != nil {
			return false
		}
	}
	return true
}

func redisStreamRuntimeConfigFrom(config *RedisStreamConfig) redisStreamRuntimeConfig {
	runtimeConfig := redisStreamRuntimeConfig{
		keyPrefix:          config.KeyPrefix,
		batchSize:          config.ConsumerBatchSize,
		queueDepth:         config.QueueDepth,
		concurrency:        config.Concurrency,
		consumerID:         config.ConsumerID,
		groupStartID:       config.GroupStartID,
		readBlock:          config.ReadBlock,
		handlerTimeout:     config.HandlerTimeout,
		pendingIdleTimeout: config.PendingIdleTimeout,
		redeliverInterval:  config.RedeliverInterval,
		retryBackoff:       config.RetryBackoff,
		retryMaxBackoff:    config.RetryMaxBackoff,
		maxLen:             config.MaxLen,
		logger:             config.Logger,
	}
	if runtimeConfig.keyPrefix == "" {
		runtimeConfig.keyPrefix = defaultRedisStreamKeyPrefix
	}
	if runtimeConfig.batchSize == 0 {
		runtimeConfig.batchSize = defaultRedisStreamConsumerBatchSize
	}
	if runtimeConfig.queueDepth == 0 {
		runtimeConfig.queueDepth = defaultRedisStreamQueueDepth
	}
	if runtimeConfig.concurrency == 0 {
		runtimeConfig.concurrency = defaultRedisStreamConcurrency
	}
	if runtimeConfig.groupStartID == "" {
		runtimeConfig.groupStartID = defaultRedisStreamGroupStartID
	}
	if runtimeConfig.readBlock == 0 {
		runtimeConfig.readBlock = defaultRedisStreamReadBlock
	}
	if runtimeConfig.handlerTimeout == 0 {
		runtimeConfig.handlerTimeout = defaultRedisStreamHandlerTimeout
	}
	if runtimeConfig.pendingIdleTimeout == 0 {
		runtimeConfig.pendingIdleTimeout = defaultRedisStreamPendingIdleTimeout
	}
	if runtimeConfig.redeliverInterval == 0 {
		runtimeConfig.redeliverInterval = defaultRedisStreamRedeliverInterval
	}
	if runtimeConfig.retryBackoff == 0 {
		runtimeConfig.retryBackoff = defaultRedisStreamRetryBackoff
	}
	if runtimeConfig.retryMaxBackoff == 0 {
		runtimeConfig.retryMaxBackoff = defaultRedisStreamRetryMaxBackoff
	}
	if runtimeConfig.logger == nil {
		runtimeConfig.logger = slog.Default()
	}
	return runtimeConfig
}

func prepareRedisStream(ctx context.Context, name string, config MQConfig) (preparedClient, error) {
	dataConfig := DataConfig{Type: DataTypeRedis, DSN: config.DSN, Redis: config.Redis}
	if config.Type == MQTypeRedisStreamCluster {
		dataConfig.Type = DataTypeRedisCluster
	}

	var client redis.UniversalClient
	if dataConfig.Type == DataTypeRedisCluster {
		options, err := redisClusterOptions(dataConfig)
		if err != nil {
			return preparedClient{}, err
		}
		client = redis.NewClusterClient(options)
	} else {
		options, err := redisOptions(dataConfig)
		if err != nil {
			return preparedClient{}, err
		}
		client = redis.NewClient(options)
	}

	if !config.SkipCheck {
		checkCtx, cancel := checkContext(ctx, config.CheckTimeout)
		err := client.Ping(checkCtx).Err()
		cancel()
		if err != nil {
			return preparedClient{}, errors.Join(fmt.Errorf("redis stream connection check: %w", err), client.Close())
		}
	}

	mq := newRedisStreamMQ(&redisStreamRedisStore{client: client}, redisStreamRuntimeConfigFrom(config.RedisStream))
	return preparedClient{
		name: name, module: "mq", typ: string(config.Type),
		commit: func() error { return resource.Set[coremq.MQ](name, mq) },
		close:  func(context.Context) error { return mq.Close() },
	}, nil
}
