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
	defaultRedisStreamKeyPrefix                  = "kc:mq"
	defaultRedisStreamConsumerBatchSize          = int64(64)
	defaultRedisStreamQueueDepth                 = 64
	defaultRedisStreamConcurrency                = 10
	defaultRedisStreamAckBatchSize               = 64
	defaultRedisStreamAckFlushInterval           = 2 * time.Millisecond
	defaultRedisStreamReclaimMaxBatches          = 4
	defaultRedisStreamMaxDeliveryAttempts        = 5
	defaultRedisStreamGroupStartID               = "0"
	defaultRedisStreamReadBlock                  = time.Second
	defaultRedisStreamHandlerTimeout             = 30 * time.Second
	defaultRedisStreamPendingIdleTimeout         = time.Minute
	defaultRedisStreamRedeliverInterval          = 15 * time.Second
	defaultRedisStreamRetryBackoff               = time.Second
	defaultRedisStreamRetryMaxBackoff            = 30 * time.Second
	defaultRedisStreamConsumerCleanupInterval    = time.Hour
	defaultRedisStreamConsumerCleanupIdleTimeout = 24 * time.Hour
	redisStreamMaxBatchSize                      = int64(10000)
	redisStreamMaxQueueDepth                     = 10000
	redisStreamMaxConcurrency                    = 10000
	redisStreamMaxAckBatchSize                   = 10000
	redisStreamMaxReclaimBatches                 = 10000
	redisStreamMaxDeliveryAttempts               = 10000
	redisStreamMaxConsumerCleanupPerRun          = int64(128)
	redisStreamMessageVersion                    = "1"
	redisStreamFieldVersion                      = "v"
	redisStreamFieldKey                          = "key"
	redisStreamFieldBody                         = "body"
	redisStreamFieldHeaders                      = "headers"
	redisStreamFieldTimestamp                    = "timestamp"
	redisStreamDLQSuffix                         = ".dlq"
	redisStreamDLQHeaderSourceTopic              = "kc.dlq.source_topic"
	redisStreamDLQHeaderSourceGroup              = "kc.dlq.source_group"
	redisStreamDLQHeaderSourceMessageID          = "kc.dlq.source_message_id"
	redisStreamDLQHeaderDeliveryCount            = "kc.dlq.delivery_count"
	redisStreamDLQHeaderError                    = "kc.dlq.error"
	redisStreamDLQHeaderFailedAt                 = "kc.dlq.failed_at"
)

type redisStreamRuntimeConfig struct {
	keyPrefix                  string
	batchSize                  int64
	queueDepth                 int
	concurrency                int
	ackBatchSize               int
	ackFlushInterval           time.Duration
	reclaimMaxBatches          int
	maxDeliveryAttempts        int
	consumerID                 string
	groupStartID               string
	readBlock                  time.Duration
	handlerTimeout             time.Duration
	pendingIdleTimeout         time.Duration
	redeliverInterval          time.Duration
	retryBackoff               time.Duration
	retryMaxBackoff            time.Duration
	consumerCleanupInterval    time.Duration
	consumerCleanupIdleTimeout time.Duration
	maxLen                     int64
	logger                     *slog.Logger
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

	queue    chan redis.XMessage
	ackQueue chan string
	mu       sync.Mutex
	// inFlight 同时覆盖已经入队和正在执行的消息，避免 poll 与 reclaim 重复入队。
	inFlight      map[string]struct{}
	reclaimCursor string
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

func (s *redisStreamSubscription) resetReclaimCursor() {
	s.mu.Lock()
	s.reclaimCursor = "0-0"
	s.mu.Unlock()
}

func (s *redisStreamSubscription) getReclaimCursor() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reclaimCursor == "" {
		return "0-0"
	}
	return s.reclaimCursor
}

func (s *redisStreamSubscription) setReclaimCursor(cursor string) {
	s.mu.Lock()
	s.reclaimCursor = cursor
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
	PendingEntry(context.Context, string, string, string) (redis.XPendingExt, bool, error)
	CleanupConsumers(context.Context, string, string, string, string, time.Duration, int64) (int64, error)
}

type redisStreamRedisStore struct {
	client redis.UniversalClient
}

const redisStreamCleanupConsumersScript = `
local consumers = redis.call('XINFO', 'CONSUMERS', KEYS[1], ARGV[1])
local target = ARGV[2]
local exclude = ARGV[3]
local min_idle = tonumber(ARGV[4])
local max_delete = tonumber(ARGV[5])
local deleted = 0
for _, consumer in ipairs(consumers) do
  local name = nil
  local pending = nil
  local idle = nil
  for index = 1, #consumer, 2 do
    if consumer[index] == 'name' then
      name = consumer[index + 1]
    elseif consumer[index] == 'pending' then
      pending = tonumber(consumer[index + 1])
    elseif consumer[index] == 'idle' then
      idle = tonumber(consumer[index + 1])
    end
  end
  if name ~= nil and pending == 0 and idle ~= nil and idle >= min_idle
      and (target == '' or name == target) and (exclude == '' or name ~= exclude) then
    redis.call('XGROUP', 'DELCONSUMER', KEYS[1], ARGV[1], name)
    deleted = deleted + 1
    if deleted >= max_delete then
      break
    end
  end
end
return deleted
`

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
		stop:          stop,
		done:          make(chan struct{}),
		queue:         make(chan redis.XMessage, subscriptionConfig.queueDepth),
		ackQueue:      make(chan string, subscriptionConfig.queueDepth),
		inFlight:      make(map[string]struct{}),
		reclaimCursor: "0-0",
	}
	if !m.trackSubscription(subscription) {
		stop()
		return coremq.ErrClosed
	}

	consumer := m.subscriptionConsumerID()
	go m.runSubscription(pollCtx, ctx, topic, stream, group, consumer, handler, subscriptionConfig, subscription)
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
	topic string,
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

	var producers sync.WaitGroup
	producers.Add(2)
	go func() {
		defer producers.Done()
		m.pollRedisStreamMessages(pollCtx, stream, group, consumer, config, subscription)
	}()
	go func() {
		defer producers.Done()
		m.reclaimRedisStreamMessages(pollCtx, stream, group, consumer, config, subscription)
	}()

	var workers sync.WaitGroup
	workers.Add(config.concurrency)
	for range config.concurrency {
		go func() {
			defer workers.Done()
			m.runRedisStreamWorker(pollCtx, subscriptionCtx, topic, stream, group, handler, config, subscription)
		}()
	}

	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		m.runRedisStreamAckWorker(subscriptionCtx, stream, group, config, subscription)
	}()

	<-pollCtx.Done()
	producers.Wait()
	workers.Wait()
	close(subscription.ackQueue)
	<-ackDone
	m.cleanupRedisStreamConsumer(subscriptionCtx, stream, group, consumer, config)
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
				err = m.store.EnsureGroup(ctx, stream, group, config.groupStartID)
				if err == nil {
					subscription.resetReclaimCursor()
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
	nextCleanup := time.Now().Add(config.consumerCleanupInterval)

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
			err = m.store.EnsureGroup(ctx, stream, group, config.groupStartID)
			if err == nil {
				subscription.resetReclaimCursor()
			}
			if redisStreamCommandCanceled(ctx, err) {
				return
			}
		}

		if !time.Now().Before(nextCleanup) {
			m.cleanupStaleRedisStreamConsumers(ctx, stream, group, consumer, config)
			nextCleanup = time.Now().Add(config.consumerCleanupInterval)
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
	start := subscription.getReclaimCursor()
	for range config.reclaimMaxBatches {
		if ctx.Err() != nil {
			return ctx.Err()
		}
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
			subscription.setReclaimCursor("0-0")
			return nil
		}
		if nextStart == "" || nextStart == start {
			return fmt.Errorf("redis stream XAUTOCLAIM returned invalid cursor %q", nextStart)
		}
		start = nextStart
		subscription.setReclaimCursor(nextStart)
	}
	return nil
}

func (m *redisStreamMQ) runRedisStreamWorker(
	pollCtx context.Context,
	subscriptionCtx context.Context,
	topic string,
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
			ack := m.processRedisStreamMessage(subscriptionCtx, topic, stream, group, handler, record, config)
			if !ack {
				subscription.release(record.ID)
				continue
			}
			// Close 只取消 pollCtx，仍允许已成功的 Handler 提交 ACK；订阅取消则不确认失效结果。
			select {
			case subscription.ackQueue <- record.ID:
			case <-subscriptionCtx.Done():
				subscription.release(record.ID)
			}
		}
	}
}

func (m *redisStreamMQ) processRedisStreamMessage(
	subscriptionCtx context.Context,
	topic string,
	stream string,
	group string,
	handler coremq.Handler,
	record redis.XMessage,
	config redisStreamRuntimeConfig,
) bool {
	message, err := messageFromRedisStreamRecord(record)
	if err != nil {
		m.deadLetterInvalidRedisStreamRecord(subscriptionCtx, topic, stream, group, record, err, config)
		return false
	}
	if handlerErr := m.handleRedisStreamMessage(subscriptionCtx, handler, message, config.handlerTimeout); handlerErr != nil {
		m.handleFailedRedisStreamMessage(subscriptionCtx, topic, stream, group, record.ID, message, handlerErr, config)
		return false
	}
	return true
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

func (m *redisStreamMQ) runRedisStreamAckWorker(
	parent context.Context,
	stream string,
	group string,
	config redisStreamRuntimeConfig,
	subscription *redisStreamSubscription,
) {
	ids := make([]string, 0, config.ackBatchSize)
	timer := time.NewTimer(config.ackFlushInterval)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	flush := func() {
		if len(ids) == 0 {
			return
		}
		batch := append([]string(nil), ids...)
		ids = ids[:0]
		ackCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), config.retryMaxBackoff)
		_, err := m.store.Ack(ackCtx, stream, group, batch...)
		cancel()
		if err != nil {
			redisStreamLogger(config).ErrorContext(context.WithoutCancel(parent), "批量确认 Redis Stream 消息失败",
				"stream", stream, "group", group, "count", len(batch), "error", err)
		}
		for _, id := range batch {
			subscription.release(id)
		}
	}

	for {
		var timerC <-chan time.Time
		if len(ids) > 0 {
			timerC = timer.C
		}
		select {
		case id, ok := <-subscription.ackQueue:
			if !ok {
				flush()
				return
			}
			ids = append(ids, id)
			if len(ids) == 1 {
				timer.Reset(config.ackFlushInterval)
			}
			if len(ids) >= config.ackBatchSize {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				flush()
			}
		case <-timerC:
			flush()
		}
	}
}

func (m *redisStreamMQ) handleFailedRedisStreamMessage(
	parent context.Context,
	topic string,
	stream string,
	group string,
	messageID string,
	message *coremq.Message,
	handlerErr error,
	config redisStreamRuntimeConfig,
) {
	logger := redisStreamLogger(config)
	logCtx := context.WithoutCancel(parent)
	pending, exists, err := m.store.PendingEntry(parent, stream, group, messageID)
	if err != nil {
		logger.ErrorContext(logCtx, "查询 Redis Stream 消息投递次数失败",
			"stream", stream, "group", group, "message_id", messageID, "error", err)
		return
	}
	if !exists {
		logger.WarnContext(logCtx, "失败的 Redis Stream 消息已不在 Pending 列表",
			"stream", stream, "group", group, "message_id", messageID)
		return
	}
	logger.WarnContext(logCtx, "处理 Redis Stream 消息失败",
		"stream", stream, "group", group, "message_id", messageID,
		"delivery_count", pending.RetryCount, "error", handlerErr)
	if pending.RetryCount < int64(config.maxDeliveryAttempts) {
		return
	}
	m.moveRedisStreamMessageToDLQ(parent, topic, stream, group, messageID, message, pending.RetryCount, handlerErr, config)
}

func (m *redisStreamMQ) deadLetterInvalidRedisStreamRecord(
	parent context.Context,
	topic string,
	stream string,
	group string,
	record redis.XMessage,
	decodeErr error,
	config redisStreamRuntimeConfig,
) {
	logger := redisStreamLogger(config)
	logCtx := context.WithoutCancel(parent)
	pending, exists, err := m.store.PendingEntry(parent, stream, group, record.ID)
	if err != nil {
		logger.ErrorContext(logCtx, "查询无法解码消息的投递次数失败",
			"stream", stream, "group", group, "message_id", record.ID, "error", err)
		return
	}
	if !exists {
		logger.WarnContext(logCtx, "无法解码的消息已不在 Pending 列表",
			"stream", stream, "group", group, "message_id", record.ID)
		return
	}
	body, err := json.Marshal(record.Values)
	if err != nil {
		logger.ErrorContext(logCtx, "序列化无法解码的 Redis Stream 原始字段失败",
			"stream", stream, "group", group, "message_id", record.ID, "error", err)
		return
	}
	message := &coremq.Message{Body: body, Timestamp: redisStreamTimestampFromID(record.ID)}
	m.moveRedisStreamMessageToDLQ(parent, topic, stream, group, record.ID, message, pending.RetryCount, decodeErr, config)
}

func (m *redisStreamMQ) moveRedisStreamMessageToDLQ(
	parent context.Context,
	topic string,
	stream string,
	group string,
	messageID string,
	message *coremq.Message,
	deliveryCount int64,
	reason error,
	config redisStreamRuntimeConfig,
) {
	dlqMessage := cloneRedisStreamMessage(message)
	if dlqMessage.Headers == nil {
		dlqMessage.Headers = make(map[string][]byte, 6)
	}
	dlqMessage.Headers[redisStreamDLQHeaderSourceTopic] = []byte(topic)
	dlqMessage.Headers[redisStreamDLQHeaderSourceGroup] = []byte(group)
	dlqMessage.Headers[redisStreamDLQHeaderSourceMessageID] = []byte(messageID)
	dlqMessage.Headers[redisStreamDLQHeaderDeliveryCount] = []byte(strconv.FormatInt(deliveryCount, 10))
	dlqMessage.Headers[redisStreamDLQHeaderError] = []byte(reason.Error())
	dlqMessage.Headers[redisStreamDLQHeaderFailedAt] = []byte(time.Now().UTC().Format(time.RFC3339Nano))

	values, err := marshalRedisStreamMessage(dlqMessage)
	if err == nil {
		_, err = m.store.Add(parent, m.streamKey(topic+redisStreamDLQSuffix), values, config.maxLen)
	}
	logger := redisStreamLogger(config)
	logCtx := context.WithoutCancel(parent)
	if err != nil {
		logger.ErrorContext(logCtx, "写入 Redis Stream DLQ 失败",
			"stream", stream, "group", group, "message_id", messageID, "error", err)
		return
	}
	if err := m.ackRedisStreamMessage(parent, stream, group, messageID, config.retryMaxBackoff); err != nil {
		logger.ErrorContext(logCtx, "Redis Stream 消息进入 DLQ 后确认源消息失败",
			"stream", stream, "group", group, "message_id", messageID, "error", err)
		return
	}
	logger.WarnContext(logCtx, "Redis Stream 消息已进入 DLQ",
		"stream", stream, "group", group, "message_id", messageID,
		"delivery_count", deliveryCount, "dlq_topic", topic+redisStreamDLQSuffix)
}

func (m *redisStreamMQ) cleanupRedisStreamConsumer(
	parent context.Context,
	stream string,
	group string,
	consumer string,
	config redisStreamRuntimeConfig,
) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), config.retryMaxBackoff)
	defer cancel()
	_, err := m.store.CleanupConsumers(ctx, stream, group, consumer, "", 0, 1)
	if err != nil && !redisStreamNoGroup(err) {
		redisStreamLogger(config).ErrorContext(context.WithoutCancel(parent), "清理当前 Redis Stream consumer 失败",
			"stream", stream, "group", group, "consumer", consumer, "error", err)
	}
}

func (m *redisStreamMQ) cleanupStaleRedisStreamConsumers(
	ctx context.Context,
	stream string,
	group string,
	consumer string,
	config redisStreamRuntimeConfig,
) {
	_, err := m.store.CleanupConsumers(
		ctx, stream, group, "", consumer, config.consumerCleanupIdleTimeout, redisStreamMaxConsumerCleanupPerRun,
	)
	if err != nil && !redisStreamNoGroup(err) && !redisStreamCommandCanceled(ctx, err) {
		redisStreamLogger(config).ErrorContext(context.WithoutCancel(ctx), "清理遗留 Redis Stream consumer 失败",
			"stream", stream, "group", group, "consumer", consumer, "error", err)
	}
}

func redisStreamLogger(config redisStreamRuntimeConfig) *slog.Logger {
	if config.logger != nil {
		return config.logger
	}
	return slog.Default()
}

func cloneRedisStreamMessage(message *coremq.Message) *coremq.Message {
	cloned := &coremq.Message{
		ID: message.ID, Key: cloneRedisStreamBytes(message.Key), Body: cloneRedisStreamBytes(message.Body), Timestamp: message.Timestamp,
	}
	if message.Headers != nil {
		cloned.Headers = make(map[string][]byte, len(message.Headers))
		for key, value := range message.Headers {
			cloned.Headers[key] = cloneRedisStreamBytes(value)
		}
	}
	return cloned
}

func redisStreamTimestampFromID(messageID string) time.Time {
	if split := strings.SplitN(messageID, "-", 2); len(split) == 2 {
		if milliseconds, err := strconv.ParseInt(split[0], 10, 64); err == nil {
			return time.UnixMilli(milliseconds)
		}
	}
	return time.Time{}
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
		if resolved.RedisStream.AckBatchSize != nil {
			config.ackBatchSize = *resolved.RedisStream.AckBatchSize
		}
		if resolved.RedisStream.AckFlushInterval != nil {
			config.ackFlushInterval = *resolved.RedisStream.AckFlushInterval
		}
		if resolved.RedisStream.ReclaimMaxBatches != nil {
			config.reclaimMaxBatches = *resolved.RedisStream.ReclaimMaxBatches
		}
		if resolved.RedisStream.MaxDeliveryAttempts != nil {
			config.maxDeliveryAttempts = *resolved.RedisStream.MaxDeliveryAttempts
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
	if config.ackBatchSize > redisStreamMaxAckBatchSize {
		return redisStreamRuntimeConfig{}, fmt.Errorf(
			"%w: redis stream ack batch size exceeds %d", coremq.ErrInvalidSubscribeOption, redisStreamMaxAckBatchSize,
		)
	}
	if config.reclaimMaxBatches > redisStreamMaxReclaimBatches {
		return redisStreamRuntimeConfig{}, fmt.Errorf(
			"%w: redis stream reclaim max batches exceeds %d", coremq.ErrInvalidSubscribeOption, redisStreamMaxReclaimBatches,
		)
	}
	if config.maxDeliveryAttempts > redisStreamMaxDeliveryAttempts {
		return redisStreamRuntimeConfig{}, fmt.Errorf(
			"%w: redis stream max delivery attempts exceeds %d", coremq.ErrInvalidSubscribeOption, redisStreamMaxDeliveryAttempts,
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

func (s *redisStreamRedisStore) PendingEntry(
	ctx context.Context,
	stream string,
	group string,
	messageID string,
) (redis.XPendingExt, bool, error) {
	entries, err := s.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream, Group: group, Start: messageID, End: messageID, Count: 1,
	}).Result()
	if err != nil {
		return redis.XPendingExt{}, false, err
	}
	if len(entries) == 0 || entries[0].ID != messageID {
		return redis.XPendingExt{}, false, nil
	}
	return entries[0], true, nil
}

func (s *redisStreamRedisStore) CleanupConsumers(
	ctx context.Context,
	stream string,
	group string,
	target string,
	exclude string,
	minIdle time.Duration,
	limit int64,
) (int64, error) {
	return s.client.Eval(ctx, redisStreamCleanupConsumersScript, []string{stream},
		group, target, exclude, minIdle.Milliseconds(), limit,
	).Int64()
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
	if streamConfig.AckBatchSize < 0 || streamConfig.AckBatchSize > redisStreamMaxAckBatchSize {
		return fmt.Errorf("%w: redis stream ack batch size must be between 1 and %d", ErrInvalidConfig, redisStreamMaxAckBatchSize)
	}
	if streamConfig.ReclaimMaxBatches < 0 || streamConfig.ReclaimMaxBatches > redisStreamMaxReclaimBatches {
		return fmt.Errorf("%w: redis stream reclaim max batches must be between 1 and %d", ErrInvalidConfig, redisStreamMaxReclaimBatches)
	}
	if streamConfig.MaxDeliveryAttempts < 0 || streamConfig.MaxDeliveryAttempts > redisStreamMaxDeliveryAttempts {
		return fmt.Errorf("%w: redis stream max delivery attempts must be between 1 and %d", ErrInvalidConfig, redisStreamMaxDeliveryAttempts)
	}
	if streamConfig.MaxLen < 0 {
		return fmt.Errorf("%w: redis stream max len is negative", ErrInvalidConfig)
	}
	if streamConfig.ReadBlock < 0 || streamConfig.HandlerTimeout < 0 || streamConfig.PendingIdleTimeout < 0 || streamConfig.RedeliverInterval < 0 ||
		streamConfig.AckFlushInterval < 0 || streamConfig.ConsumerCleanupInterval < 0 || streamConfig.ConsumerCleanupIdleTimeout < 0 {
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
		keyPrefix:                  config.KeyPrefix,
		batchSize:                  config.ConsumerBatchSize,
		queueDepth:                 config.QueueDepth,
		concurrency:                config.Concurrency,
		ackBatchSize:               config.AckBatchSize,
		ackFlushInterval:           config.AckFlushInterval,
		reclaimMaxBatches:          config.ReclaimMaxBatches,
		maxDeliveryAttempts:        config.MaxDeliveryAttempts,
		consumerID:                 config.ConsumerID,
		groupStartID:               config.GroupStartID,
		readBlock:                  config.ReadBlock,
		handlerTimeout:             config.HandlerTimeout,
		pendingIdleTimeout:         config.PendingIdleTimeout,
		redeliverInterval:          config.RedeliverInterval,
		retryBackoff:               config.RetryBackoff,
		retryMaxBackoff:            config.RetryMaxBackoff,
		consumerCleanupInterval:    config.ConsumerCleanupInterval,
		consumerCleanupIdleTimeout: config.ConsumerCleanupIdleTimeout,
		maxLen:                     config.MaxLen,
		logger:                     config.Logger,
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
	if runtimeConfig.ackBatchSize == 0 {
		runtimeConfig.ackBatchSize = defaultRedisStreamAckBatchSize
	}
	if runtimeConfig.ackFlushInterval == 0 {
		runtimeConfig.ackFlushInterval = defaultRedisStreamAckFlushInterval
	}
	if runtimeConfig.reclaimMaxBatches == 0 {
		runtimeConfig.reclaimMaxBatches = defaultRedisStreamReclaimMaxBatches
	}
	if runtimeConfig.maxDeliveryAttempts == 0 {
		runtimeConfig.maxDeliveryAttempts = defaultRedisStreamMaxDeliveryAttempts
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
	if runtimeConfig.consumerCleanupInterval == 0 {
		runtimeConfig.consumerCleanupInterval = defaultRedisStreamConsumerCleanupInterval
	}
	if runtimeConfig.consumerCleanupIdleTimeout == 0 {
		runtimeConfig.consumerCleanupIdleTimeout = defaultRedisStreamConsumerCleanupIdleTimeout
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
