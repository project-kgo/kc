package infra

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	coremq "github.com/project-kgo/kc/pkg/mq"
	"github.com/project-kgo/kc/pkg/resource"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

var _ coremq.MQ = (*kafkaMQ)(nil)

const (
	defaultKafkaConsumerBatchSize      = 100
	defaultKafkaConcurrency            = 10
	defaultKafkaHandlerTimeout         = 30 * time.Second
	defaultKafkaShareHandlerTimeout    = 15 * time.Second
	defaultKafkaShareDeliveryAttempts  = 4
	defaultKafkaBatchProcessingTimeout = 45 * time.Second
	defaultKafkaRebalanceTimeout       = 60 * time.Second
	defaultKafkaRetryBackoff           = time.Second
	defaultKafkaRetryMaxBackoff        = 30 * time.Second
	kafkaMaxConsumerBatchSize          = 10000
	kafkaMaxConcurrency                = 10000
	kafkaMaxRetries                    = 10000
	// broker 投递上限为 5 时，应用最多在第 4 次投递写 DLQ，给 DLQ 故障保留一次重投机会。
	kafkaBrokerShareDeliveryCountLimit = 5
	kafkaMaxShareDeliveryAttempts      = kafkaBrokerShareDeliveryCountLimit - 1
	kafkaDLQSuffix                     = ".dlq"
	kafkaDLQHeaderSourceTopic          = "kc.dlq.source_topic"
	kafkaDLQHeaderSourceGroup          = "kc.dlq.source_group"
	kafkaDLQHeaderSourceMessageID      = "kc.dlq.source_message_id"
	kafkaDLQHeaderDeliveryCount        = "kc.dlq.delivery_count"
	kafkaDLQHeaderError                = "kc.dlq.error"
	kafkaDLQHeaderFailedAt             = "kc.dlq.failed_at"
)

type kafkaGroupMode uint8

const (
	kafkaConsumerGroup kafkaGroupMode = iota
	kafkaShareGroup
)

type kafkaProducer interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	Close()
}

type kafkaConsumer interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
	AllowRebalance()
	CloseAllowingRebalance()
	FlushAcks(context.Context) error
	Close()
}

type kafkaConsumerFactory func(topic, group string, config kafkaSubscriptionConfig) (kafkaConsumer, error)

type kafkaRuntimeConfig struct {
	batchSize              int
	concurrency            int
	handlerTimeout         time.Duration
	maxRetries             int
	maxDeliveryAttempts    int
	batchProcessingTimeout time.Duration
	rebalanceTimeout       time.Duration
	retryBackoff           time.Duration
	retryMaxBackoff        time.Duration
	logger                 *slog.Logger
}

type kafkaSubscriptionConfig struct {
	kafkaRuntimeConfig
	mode            kafkaGroupMode
	batchController *kafkaBatchController
	startOffset     *coremq.KafkaStartOffset
}

// kafkaBatchController 只中断 rebalance 当前阻塞的批次，不参与 MQ Close 的优雅关闭。
type kafkaBatchController struct {
	mu      sync.Mutex
	cancel  context.CancelCauseFunc
	pending error
}

type kafkaMQState uint8

const (
	kafkaMQOpen kafkaMQState = iota
	kafkaMQClosing
	kafkaMQClosed
)

type kafkaSubscription struct {
	stop context.CancelFunc
	done chan struct{}
}

type kafkaMQ struct {
	producer    kafkaProducer
	newConsumer kafkaConsumerFactory
	config      kafkaRuntimeConfig
	mode        kafkaGroupMode

	mu            sync.Mutex
	state         kafkaMQState
	subscriptions map[*kafkaSubscription]struct{}
	operations    sync.WaitGroup
	closeDone     chan struct{}
}

type kafkaPartitionKey struct {
	topic     string
	partition int32
}

type kafkaPartitionResult struct {
	lastTerminal *kgo.Record
	err          error
}

var (
	errKafkaBatchProcessingTimeout = errors.New("kafka batch processing timeout")
	errKafkaRebalanceBlocked       = errors.New("kafka rebalance callback blocked")
)

func (c *kafkaBatchController) start(cancel context.CancelCauseFunc) {
	c.mu.Lock()
	c.cancel = cancel
	pending := c.pending
	c.pending = nil
	c.mu.Unlock()
	// callback 可能恰好发生在 PollRecords 返回后、批次注册取消函数前。
	if pending != nil {
		cancel(pending)
	}
}

func (c *kafkaBatchController) finish() {
	c.mu.Lock()
	c.cancel = nil
	c.mu.Unlock()
}

func (c *kafkaBatchController) interrupt(cause error) {
	c.mu.Lock()
	cancel := c.cancel
	if cancel == nil {
		c.pending = cause
	}
	c.mu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
}

func (c *kafkaBatchController) reset() {
	c.mu.Lock()
	c.pending = nil
	c.mu.Unlock()
}

func newKafkaMQ(
	producer kafkaProducer,
	newConsumer kafkaConsumerFactory,
	mode kafkaGroupMode,
	config kafkaRuntimeConfig,
) *kafkaMQ {
	return &kafkaMQ{
		producer:      producer,
		newConsumer:   newConsumer,
		config:        config,
		mode:          mode,
		state:         kafkaMQOpen,
		subscriptions: make(map[*kafkaSubscription]struct{}),
		closeDone:     make(chan struct{}),
	}
}

func (m *kafkaMQ) Publish(ctx context.Context, topic string, message *coremq.Message) error {
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

	record := kafkaRecordFromMessage(topic, message)
	if err := m.producer.ProduceSync(ctx, record).FirstErr(); err != nil {
		return wrapKafkaError("publish kafka message", err)
	}
	return nil
}

func (m *kafkaMQ) Subscribe(
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
		return fmt.Errorf("subscribe kafka: %w", err)
	}

	config, err := m.subscriptionConfig(options...)
	if err != nil {
		return fmt.Errorf("subscribe kafka: %w", err)
	}
	if config.mode == kafkaConsumerGroup {
		config.batchController = &kafkaBatchController{}
	}
	if !m.beginOperation() {
		return coremq.ErrClosed
	}
	defer m.endOperation()

	consumer, err := m.newConsumer(topic, group, config)
	if err != nil {
		return fmt.Errorf("create kafka consumer: %w", err)
	}
	pollCtx, stop := context.WithCancel(ctx)
	subscription := &kafkaSubscription{stop: stop, done: make(chan struct{})}
	if !m.trackSubscription(subscription) {
		stop()
		_ = closeKafkaConsumer(consumer, config.mode, config.retryMaxBackoff)
		return coremq.ErrClosed
	}

	go m.runSubscription(pollCtx, ctx, topic, group, handler, config, subscription, consumer)
	return nil
}

func (m *kafkaMQ) Close() error {
	m.mu.Lock()
	if m.state != kafkaMQOpen {
		done := m.closeDone
		m.mu.Unlock()
		<-done
		return nil
	}
	m.state = kafkaMQClosing
	subscriptions := make([]*kafkaSubscription, 0, len(m.subscriptions))
	for subscription := range m.subscriptions {
		subscription.stop()
		subscriptions = append(subscriptions, subscription)
	}
	m.mu.Unlock()

	m.operations.Wait()
	for _, subscription := range subscriptions {
		<-subscription.done
	}
	m.producer.Close()

	m.mu.Lock()
	m.state = kafkaMQClosed
	close(m.closeDone)
	m.mu.Unlock()
	return nil
}

func (m *kafkaMQ) beginOperation() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state != kafkaMQOpen {
		return false
	}
	m.operations.Add(1)
	return true
}

func (m *kafkaMQ) endOperation() {
	m.operations.Done()
}

func (m *kafkaMQ) trackSubscription(subscription *kafkaSubscription) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state != kafkaMQOpen {
		return false
	}
	m.subscriptions[subscription] = struct{}{}
	return true
}

func (m *kafkaMQ) untrackSubscription(subscription *kafkaSubscription) {
	m.mu.Lock()
	delete(m.subscriptions, subscription)
	m.mu.Unlock()
}

func (m *kafkaMQ) runSubscription(
	pollCtx context.Context,
	handlerCtx context.Context,
	topic string,
	group string,
	handler coremq.Handler,
	config kafkaSubscriptionConfig,
	subscription *kafkaSubscription,
	consumer kafkaConsumer,
) {
	defer func() {
		m.untrackSubscription(subscription)
		close(subscription.done)
	}()

	retryAttempt := 0
	for {
		stable, err := m.consume(pollCtx, handlerCtx, topic, group, consumer, handler, config)
		if closeErr := closeKafkaConsumer(consumer, config.mode, config.retryMaxBackoff); closeErr != nil {
			kafkaLogger(config).ErrorContext(context.WithoutCancel(handlerCtx), "关闭 Kafka consumer 失败",
				"topic", topic, "group", group, "error", closeErr)
		}
		if pollCtx.Err() != nil {
			return
		}
		if err != nil {
			kafkaLogger(config).ErrorContext(context.WithoutCancel(handlerCtx), "Kafka 消费循环中断",
				"topic", topic, "group", group, "error", err)
		}
		if stable {
			retryAttempt = 0
		}

		for {
			if !waitKafkaRetry(pollCtx, kafkaRetryDelay(config.retryBackoff, config.retryMaxBackoff, retryAttempt)) {
				return
			}
			retryAttempt++
			consumer, err = m.newConsumer(topic, group, config)
			if err == nil {
				break
			}
			kafkaLogger(config).ErrorContext(context.WithoutCancel(handlerCtx), "重建 Kafka consumer 失败",
				"topic", topic, "group", group, "error", err)
		}
	}
}

func (m *kafkaMQ) consume(
	pollCtx context.Context,
	handlerCtx context.Context,
	topic string,
	group string,
	consumer kafkaConsumer,
	handler coremq.Handler,
	config kafkaSubscriptionConfig,
) (bool, error) {
	if config.mode == kafkaShareGroup {
		return m.consumeShareGroup(pollCtx, handlerCtx, topic, group, consumer, handler, config)
	}
	return m.consumeConsumerGroup(pollCtx, handlerCtx, topic, group, consumer, handler, config)
}

func (m *kafkaMQ) consumeConsumerGroup(
	pollCtx context.Context,
	handlerCtx context.Context,
	topic string,
	group string,
	consumer kafkaConsumer,
	handler coremq.Handler,
	config kafkaSubscriptionConfig,
) (bool, error) {
	stable := false
	for {
		if config.batchController != nil {
			config.batchController.reset()
		}
		fetches := consumer.PollRecords(pollCtx, config.batchSize)
		if err := fetches.Err(); err != nil {
			if ctxErr := pollCtx.Err(); ctxErr != nil {
				return stable, ctxErr
			}
			return stable, wrapKafkaError("poll kafka message", err)
		}
		records := fetches.Records()
		if len(records) == 0 {
			continue
		}
		if err := pollCtx.Err(); err != nil {
			consumer.AllowRebalance()
			return stable, err
		}

		results := m.processConsumerGroupBatch(pollCtx, handlerCtx, topic, group, records, handler, config)
		commits := make([]*kgo.Record, 0, len(results))
		var batchErr error
		for _, result := range results {
			if result.lastTerminal != nil {
				commits = append(commits, result.lastTerminal)
			}
			if batchErr == nil && result.err != nil {
				batchErr = result.err
			}
		}
		if len(commits) > 0 {
			commitCtx, cancel := context.WithTimeout(context.WithoutCancel(handlerCtx), kafkaConsumerGroupCommitTimeout(config))
			err := consumer.CommitRecords(commitCtx, commits...)
			cancel()
			if err != nil {
				consumer.AllowRebalance()
				return stable, wrapKafkaError("commit kafka messages", err)
			}
		}
		consumer.AllowRebalance()
		if err := pollCtx.Err(); err != nil {
			return stable, err
		}
		if batchErr != nil {
			return stable, batchErr
		}
		stable = true
	}
}

func (m *kafkaMQ) processConsumerGroupBatch(
	pollCtx context.Context,
	handlerCtx context.Context,
	topic string,
	group string,
	records []*kgo.Record,
	handler coremq.Handler,
	config kafkaSubscriptionConfig,
) []kafkaPartitionResult {
	deadlineCtx, stopDeadline := context.WithTimeoutCause(handlerCtx, config.batchProcessingTimeout, errKafkaBatchProcessingTimeout)
	batchCtx, stopBatch := context.WithCancelCause(deadlineCtx)
	if config.batchController != nil {
		config.batchController.start(stopBatch)
		defer config.batchController.finish()
	}
	defer stopBatch(nil)
	defer stopDeadline()

	partitions := make(map[kafkaPartitionKey][]*kgo.Record)
	order := make([]kafkaPartitionKey, 0)
	for _, record := range records {
		key := kafkaPartitionKey{topic: record.Topic, partition: record.Partition}
		if _, exists := partitions[key]; !exists {
			order = append(order, key)
		}
		partitions[key] = append(partitions[key], record)
	}

	results := make(chan kafkaPartitionResult, len(order))
	semaphore := make(chan struct{}, config.concurrency)
	var workers sync.WaitGroup
	for _, key := range order {
		partition := partitions[key]
		workers.Add(1)
		go func() {
			defer workers.Done()
			var result kafkaPartitionResult
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-batchCtx.Done():
				result.err = context.Cause(batchCtx)
				results <- result
				return
			case <-pollCtx.Done():
				results <- result
				return
			}
			for _, record := range partition {
				if err := batchCtx.Err(); err != nil {
					result.err = context.Cause(batchCtx)
					break
				}
				if pollCtx.Err() != nil {
					break
				}
				if err := m.processConsumerGroupRecord(pollCtx, batchCtx, topic, group, record, handler, config); err != nil {
					result.err = err
					break
				}
				result.lastTerminal = record
			}
			results <- result
		}()
	}
	workers.Wait()
	close(results)

	batchResults := make([]kafkaPartitionResult, 0, len(order))
	for result := range results {
		batchResults = append(batchResults, result)
	}
	return batchResults
}

func (m *kafkaMQ) processConsumerGroupRecord(
	pollCtx context.Context,
	handlerCtx context.Context,
	topic string,
	group string,
	record *kgo.Record,
	handler coremq.Handler,
	config kafkaSubscriptionConfig,
) error {
	message := messageFromKafkaRecord(record)
	var handlerErr error
	for attempt := 0; attempt <= config.maxRetries; attempt++ {
		handlerErr = handleKafkaMessage(handlerCtx, handler, message, config.handlerTimeout)
		if handlerErr == nil {
			return nil
		}
		if err := handlerCtx.Err(); err != nil {
			return context.Cause(handlerCtx)
		}
		if err := pollCtx.Err(); err != nil {
			return err
		}
		kafkaLogger(config).WarnContext(context.WithoutCancel(handlerCtx), "处理 Kafka 消息失败",
			"topic", topic, "group", group, "message_id", message.ID,
			"attempt", attempt+1, "error", handlerErr)
		if attempt < config.maxRetries &&
			!waitKafkaRetryUntil(pollCtx, handlerCtx, kafkaRetryDelay(config.retryBackoff, config.retryMaxBackoff, attempt)) {
			if err := handlerCtx.Err(); err != nil {
				return context.Cause(handlerCtx)
			}
			return pollCtx.Err()
		}
	}

	deliveryCount := int64(config.maxRetries + 1)
	if err := m.publishKafkaDLQ(handlerCtx, topic, group, record, deliveryCount, handlerErr, config); err != nil {
		return fmt.Errorf("write kafka dlq for message %s: %w", message.ID, err)
	}
	return nil
}

func (m *kafkaMQ) consumeShareGroup(
	pollCtx context.Context,
	handlerCtx context.Context,
	topic string,
	group string,
	consumer kafkaConsumer,
	handler coremq.Handler,
	config kafkaSubscriptionConfig,
) (bool, error) {
	stable := false
	limit := kafkaShareFetchLimit(config)
	for {
		fetches := consumer.PollRecords(pollCtx, limit)
		if err := fetches.Err(); err != nil {
			if ctxErr := pollCtx.Err(); ctxErr != nil {
				return stable, ctxErr
			}
			return stable, wrapKafkaError("poll kafka share message", err)
		}
		records := fetches.Records()
		if len(records) == 0 {
			continue
		}
		if err := pollCtx.Err(); err != nil {
			return stable, err
		}

		m.processShareGroupBatch(pollCtx, handlerCtx, topic, group, records, handler, config)
		if err := pollCtx.Err(); err != nil {
			return stable, err
		}
		stable = true
	}
}

func (m *kafkaMQ) processShareGroupBatch(
	pollCtx context.Context,
	handlerCtx context.Context,
	topic string,
	group string,
	records []*kgo.Record,
	handler coremq.Handler,
	config kafkaSubscriptionConfig,
) {
	var workers sync.WaitGroup
	for _, record := range records {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if pollCtx.Err() != nil {
				record.Ack(kgo.AckRelease)
				return
			}
			status := m.processShareGroupRecord(pollCtx, handlerCtx, topic, group, record, handler, config)
			record.Ack(status)
		}()
	}
	workers.Wait()
}

func (m *kafkaMQ) processShareGroupRecord(
	pollCtx context.Context,
	handlerCtx context.Context,
	topic string,
	group string,
	record *kgo.Record,
	handler coremq.Handler,
	config kafkaSubscriptionConfig,
) kgo.AckStatus {
	return m.processShareGroupRecordWithDelivery(
		pollCtx, handlerCtx, topic, group, record, record.DeliveryCount(), handler, config,
	)
}

func (m *kafkaMQ) processShareGroupRecordWithDelivery(
	pollCtx context.Context,
	handlerCtx context.Context,
	topic string,
	group string,
	record *kgo.Record,
	deliveryCount int32,
	handler coremq.Handler,
	config kafkaSubscriptionConfig,
) kgo.AckStatus {
	message := messageFromKafkaRecord(record)
	handlerErr := handleKafkaMessage(handlerCtx, handler, message, config.handlerTimeout)
	if handlerErr == nil {
		return kgo.AckAccept
	}
	if handlerCtx.Err() != nil || pollCtx.Err() != nil {
		return kgo.AckRelease
	}

	kafkaLogger(config).WarnContext(context.WithoutCancel(handlerCtx), "处理 Kafka share 消息失败",
		"topic", topic, "group", group, "message_id", message.ID,
		"delivery_count", deliveryCount, "error", handlerErr)
	if deliveryCount < int32(config.maxDeliveryAttempts) {
		return kgo.AckRelease
	}
	if err := m.publishKafkaDLQ(handlerCtx, topic, group, record, int64(deliveryCount), handlerErr, config); err != nil {
		kafkaLogger(config).ErrorContext(context.WithoutCancel(handlerCtx), "写入 Kafka share DLQ 失败",
			"topic", topic, "group", group, "message_id", message.ID, "error", err)
		return kgo.AckRelease
	}
	return kgo.AckReject
}

func (m *kafkaMQ) publishKafkaDLQ(
	parent context.Context,
	topic string,
	group string,
	record *kgo.Record,
	deliveryCount int64,
	reason error,
	config kafkaSubscriptionConfig,
) error {
	headers := make([]kgo.RecordHeader, 0, len(record.Headers)+6)
	headers = append(headers, record.Headers...)
	headers = append(headers,
		kgo.RecordHeader{Key: kafkaDLQHeaderSourceTopic, Value: []byte(topic)},
		kgo.RecordHeader{Key: kafkaDLQHeaderSourceGroup, Value: []byte(group)},
		kgo.RecordHeader{Key: kafkaDLQHeaderSourceMessageID, Value: []byte(kafkaMessageID(record))},
		kgo.RecordHeader{Key: kafkaDLQHeaderDeliveryCount, Value: []byte(strconv.FormatInt(deliveryCount, 10))},
		kgo.RecordHeader{Key: kafkaDLQHeaderError, Value: []byte(reason.Error())},
		kgo.RecordHeader{Key: kafkaDLQHeaderFailedAt, Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
	)
	dlqRecord := &kgo.Record{
		Topic: topic + kafkaDLQSuffix, Key: record.Key, Value: record.Value,
		Headers: headers, Timestamp: record.Timestamp,
	}
	produceCtx, cancel := context.WithTimeout(parent, config.retryMaxBackoff)
	err := m.producer.ProduceSync(produceCtx, dlqRecord).FirstErr()
	cancel()
	if err != nil {
		return wrapKafkaError("publish kafka dlq message", err)
	}
	kafkaLogger(config).WarnContext(context.WithoutCancel(parent), "Kafka 消息已进入 DLQ",
		"topic", topic, "group", group, "message_id", kafkaMessageID(record),
		"delivery_count", deliveryCount, "dlq_topic", dlqRecord.Topic)
	return nil
}

func handleKafkaMessage(
	parent context.Context,
	handler coremq.Handler,
	message *coremq.Message,
	timeout time.Duration,
) (err error) {
	handlerCtx, cancel := context.WithTimeout(parent, timeout)
	defer func() {
		cancel()
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("handler panic: %v", recovered)
		}
	}()
	if err := handlerCtx.Err(); err != nil {
		return err
	}
	if err := handler(handlerCtx, message); err != nil {
		return err
	}
	return handlerCtx.Err()
}

func (m *kafkaMQ) subscriptionConfig(options ...coremq.SubscribeOption) (kafkaSubscriptionConfig, error) {
	resolved, err := coremq.ResolveSubscribeOptions(options...)
	if err != nil {
		return kafkaSubscriptionConfig{}, err
	}
	if resolved.RedisStream != nil {
		return kafkaSubscriptionConfig{}, fmt.Errorf("%w: redis stream option on kafka", coremq.ErrUnsupportedSubscribeOption)
	}

	config := kafkaSubscriptionConfig{kafkaRuntimeConfig: m.config, mode: m.mode}
	if resolved.BatchSize != nil {
		config.batchSize = *resolved.BatchSize
	}
	if resolved.Concurrency != nil {
		config.concurrency = *resolved.Concurrency
	}
	if resolved.HandlerTimeout != nil {
		config.handlerTimeout = *resolved.HandlerTimeout
	}
	if resolved.RetryBackoff != nil {
		config.retryBackoff = resolved.RetryBackoff.Min
		config.retryMaxBackoff = resolved.RetryBackoff.Max
	}
	if resolved.Kafka != nil {
		if resolved.Kafka.StartOffset != nil {
			if m.mode != kafkaConsumerGroup {
				return kafkaSubscriptionConfig{}, fmt.Errorf("%w: start offset option on kafka share", coremq.ErrUnsupportedSubscribeOption)
			}
			config.startOffset = resolved.Kafka.StartOffset
		}
		if resolved.Kafka.MaxRetries != nil {
			if m.mode != kafkaConsumerGroup {
				return kafkaSubscriptionConfig{}, fmt.Errorf("%w: traditional retry option on kafka share", coremq.ErrUnsupportedSubscribeOption)
			}
			config.maxRetries = *resolved.Kafka.MaxRetries
		}
		if resolved.Kafka.MaxDeliveryAttempts != nil {
			if m.mode != kafkaShareGroup {
				return kafkaSubscriptionConfig{}, fmt.Errorf("%w: share delivery option on traditional kafka", coremq.ErrUnsupportedSubscribeOption)
			}
			config.maxDeliveryAttempts = *resolved.Kafka.MaxDeliveryAttempts
		}
	}
	if config.batchSize > kafkaMaxConsumerBatchSize || config.concurrency > kafkaMaxConcurrency || config.maxRetries > kafkaMaxRetries ||
		config.maxDeliveryAttempts > kafkaMaxShareDeliveryAttempts {
		return kafkaSubscriptionConfig{}, fmt.Errorf("%w: kafka subscription value exceeds limit", coremq.ErrInvalidSubscribeOption)
	}
	return config, nil
}

func validateKafkaConfig(config MQConfig) error {
	if config.Kafka == nil {
		return fmt.Errorf("%w: kafka config is missing", ErrInvalidConfig)
	}
	brokers := kafkaBrokers(config.Kafka)
	if len(brokers) == 0 {
		return fmt.Errorf("%w: kafka broker is empty", ErrInvalidConfig)
	}
	for _, broker := range brokers {
		if broker == "" {
			return fmt.Errorf("%w: kafka broker is empty", ErrInvalidConfig)
		}
	}
	kafkaConfig := config.Kafka
	if kafkaConfig.DialTimeout < 0 || kafkaConfig.HandlerTimeout < 0 || kafkaConfig.BatchProcessingTimeout < 0 ||
		kafkaConfig.RebalanceTimeout < 0 || kafkaConfig.RetryBackoff < 0 || kafkaConfig.RetryMaxBackoff < 0 {
		return fmt.Errorf("%w: kafka duration is negative", ErrInvalidConfig)
	}
	if kafkaConfig.ConsumerBatchSize < 0 || kafkaConfig.ConsumerBatchSize > kafkaMaxConsumerBatchSize ||
		kafkaConfig.Concurrency < 0 || kafkaConfig.Concurrency > kafkaMaxConcurrency ||
		kafkaConfig.MaxRetries < 0 || kafkaConfig.MaxRetries > kafkaMaxRetries ||
		kafkaConfig.MaxDeliveryAttempts < 0 || kafkaConfig.MaxDeliveryAttempts > kafkaMaxShareDeliveryAttempts {
		return fmt.Errorf("%w: kafka numeric value is invalid", ErrInvalidConfig)
	}
	if config.Type == MQTypeKafka && kafkaConfig.MaxDeliveryAttempts != 0 {
		return fmt.Errorf("%w: share delivery attempts on traditional kafka", ErrInvalidConfig)
	}
	if config.Type == MQTypeKafkaShare && kafkaConfig.MaxRetries != 0 {
		return fmt.Errorf("%w: traditional retries on kafka share", ErrInvalidConfig)
	}
	if config.Type == MQTypeKafkaShare && (kafkaConfig.BatchProcessingTimeout != 0 || kafkaConfig.RebalanceTimeout != 0) {
		return fmt.Errorf("%w: traditional batch timeout on kafka share", ErrInvalidConfig)
	}
	runtimeConfig := kafkaRuntimeConfigFrom(kafkaConfig, kafkaMode(config.Type))
	if runtimeConfig.retryMaxBackoff < runtimeConfig.retryBackoff {
		return fmt.Errorf("%w: kafka retry max backoff is less than retry backoff", ErrInvalidConfig)
	}
	if config.Type == MQTypeKafka && runtimeConfig.batchProcessingTimeout >= runtimeConfig.rebalanceTimeout {
		return fmt.Errorf("%w: kafka batch processing timeout must be less than rebalance timeout", ErrInvalidConfig)
	}
	if kafkaConfig.SASL == nil {
		return nil
	}
	if kafkaConfig.SASL.Username == "" || kafkaConfig.SASL.Password == "" {
		return fmt.Errorf("%w: kafka sasl credentials are empty", ErrInvalidConfig)
	}
	switch kafkaConfig.SASL.Mechanism {
	case SASLPlain, SASLSCRAMSHA256, SASLSCRAMSHA512:
		return nil
	default:
		return fmt.Errorf("%w: unsupported kafka sasl mechanism", ErrInvalidConfig)
	}
}

func prepareKafka(ctx context.Context, name string, config MQConfig) (preparedClient, error) {
	commonOptions := kafkaClientOptions(config.Kafka)
	producer, err := kgo.NewClient(commonOptions...)
	if err != nil {
		return preparedClient{}, fmt.Errorf("create kafka client: %w", err)
	}
	if !config.SkipCheck {
		checkCtx, cancel := checkContext(ctx, config.CheckTimeout)
		err = producer.Ping(checkCtx)
		cancel()
		if err != nil {
			producer.Close()
			return preparedClient{}, fmt.Errorf("kafka connection check: %w", err)
		}
	}

	mode := kafkaMode(config.Type)
	runtimeConfig := kafkaRuntimeConfigFrom(config.Kafka, mode)
	newConsumer := func(topic, group string, subscriptionConfig kafkaSubscriptionConfig) (kafkaConsumer, error) {
		options := append([]kgo.Opt(nil), commonOptions...)
		options = append(options, kgo.ConsumeTopics(topic))
		if mode == kafkaShareGroup {
			limit := kafkaShareFetchLimit(subscriptionConfig)
			options = append(options,
				kgo.ShareGroup(group),
				kgo.ShareMaxRecords(int32(limit)),
				kgo.ShareMaxRecordsStrict(),
				kgo.MaxConcurrentFetches(0),
				kgo.ShareAckCallback(func(_ *kgo.Client, results kgo.ShareAckResults) {
					if err := results.Error(); err != nil {
						kafkaLogger(subscriptionConfig).Error("确认 Kafka share 消息失败", "group", group, "error", err)
					}
				}),
			)
		} else {
			if subscriptionConfig.startOffset != nil {
				offset := kafkaOffset(*subscriptionConfig.startOffset)
				options = append(options, kgo.ConsumeStartOffset(offset), kgo.ConsumeResetOffset(offset))
			}
			options = append(options,
				kgo.ConsumerGroup(group),
				kgo.DisableAutoCommit(),
				kgo.BlockRebalanceOnPoll(),
				kgo.RebalanceTimeout(subscriptionConfig.rebalanceTimeout),
				kgo.OnPartitionsCallbackBlocked(func(context.Context, *kgo.Client) {
					if subscriptionConfig.batchController != nil {
						subscriptionConfig.batchController.interrupt(errKafkaRebalanceBlocked)
					}
				}),
			)
		}
		return kgo.NewClient(options...)
	}

	mq := newKafkaMQ(producer, newConsumer, mode, runtimeConfig)
	return preparedClient{
		name: name, module: "mq", typ: string(config.Type),
		commit: func() error { return resource.Set[coremq.MQ](name, mq) },
		close:  func(context.Context) error { return mq.Close() },
	}, nil
}

func kafkaOffset(start coremq.KafkaStartOffset) kgo.Offset {
	if start == coremq.KafkaStartOffsetEarliest {
		return kgo.NewOffset().AtStart()
	}
	return kgo.NewOffset().AtEnd()
}

func kafkaRuntimeConfigFrom(config *KafkaConfig, mode kafkaGroupMode) kafkaRuntimeConfig {
	runtimeConfig := kafkaRuntimeConfig{
		batchSize: config.ConsumerBatchSize, concurrency: config.Concurrency,
		handlerTimeout: config.HandlerTimeout, maxRetries: config.MaxRetries,
		maxDeliveryAttempts:    config.MaxDeliveryAttempts,
		batchProcessingTimeout: config.BatchProcessingTimeout, rebalanceTimeout: config.RebalanceTimeout,
		retryBackoff: config.RetryBackoff, retryMaxBackoff: config.RetryMaxBackoff,
		logger: config.Logger,
	}
	if runtimeConfig.batchSize == 0 {
		runtimeConfig.batchSize = defaultKafkaConsumerBatchSize
	}
	if runtimeConfig.concurrency == 0 {
		runtimeConfig.concurrency = defaultKafkaConcurrency
	}
	if runtimeConfig.handlerTimeout == 0 {
		if mode == kafkaShareGroup {
			runtimeConfig.handlerTimeout = defaultKafkaShareHandlerTimeout
		} else {
			runtimeConfig.handlerTimeout = defaultKafkaHandlerTimeout
		}
	}
	if mode == kafkaShareGroup && runtimeConfig.maxDeliveryAttempts == 0 {
		runtimeConfig.maxDeliveryAttempts = defaultKafkaShareDeliveryAttempts
	}
	if mode == kafkaConsumerGroup {
		if runtimeConfig.batchProcessingTimeout == 0 {
			runtimeConfig.batchProcessingTimeout = defaultKafkaBatchProcessingTimeout
		}
		if runtimeConfig.rebalanceTimeout == 0 {
			runtimeConfig.rebalanceTimeout = defaultKafkaRebalanceTimeout
		}
	}
	if runtimeConfig.retryBackoff == 0 {
		runtimeConfig.retryBackoff = defaultKafkaRetryBackoff
	}
	if runtimeConfig.retryMaxBackoff == 0 {
		runtimeConfig.retryMaxBackoff = defaultKafkaRetryMaxBackoff
	}
	if runtimeConfig.logger == nil {
		runtimeConfig.logger = slog.Default()
	}
	return runtimeConfig
}

func kafkaClientOptions(config *KafkaConfig) []kgo.Opt {
	options := []kgo.Opt{kgo.SeedBrokers(kafkaBrokers(config)...)}
	if config.ClientID != "" {
		options = append(options, kgo.ClientID(config.ClientID))
	}
	if config.DialTimeout > 0 {
		options = append(options, kgo.DialTimeout(config.DialTimeout))
	}
	if config.TLS {
		options = append(options, kgo.DialTLS())
	}
	if config.SASL != nil {
		options = append(options, kgo.SASL(kafkaSASL(config.SASL)))
	}
	return options
}

func kafkaBrokers(config *KafkaConfig) []string {
	brokers := make([]string, 0, len(config.Brokers))
	for _, broker := range config.Brokers {
		brokers = append(brokers, strings.TrimSpace(broker))
	}
	return brokers
}

func kafkaSASL(config *SASLConfig) sasl.Mechanism {
	switch config.Mechanism {
	case SASLPlain:
		return plain.Auth{User: config.Username, Pass: config.Password}.AsMechanism()
	case SASLSCRAMSHA256:
		return scram.Auth{User: config.Username, Pass: config.Password}.AsSha256Mechanism()
	case SASLSCRAMSHA512:
		return scram.Auth{User: config.Username, Pass: config.Password}.AsSha512Mechanism()
	default:
		panic("infra: unreachable kafka sasl mechanism")
	}
}

func kafkaRecordFromMessage(topic string, message *coremq.Message) *kgo.Record {
	headers := make([]kgo.RecordHeader, 0, len(message.Headers))
	for key, value := range message.Headers {
		headers = append(headers, kgo.RecordHeader{Key: key, Value: value})
	}
	return &kgo.Record{Topic: topic, Key: message.Key, Value: message.Body, Headers: headers, Timestamp: message.Timestamp}
}

func messageFromKafkaRecord(record *kgo.Record) *coremq.Message {
	headers := make(map[string][]byte, len(record.Headers))
	for _, header := range record.Headers {
		headers[header.Key] = header.Value
	}
	return &coremq.Message{
		ID: kafkaMessageID(record), Key: record.Key, Body: record.Value,
		Headers: headers, Timestamp: record.Timestamp,
	}
}

func kafkaMessageID(record *kgo.Record) string {
	return fmt.Sprintf("%d:%d", record.Partition, record.Offset)
}

func kafkaMode(typ MQType) kafkaGroupMode {
	if typ == MQTypeKafkaShare {
		return kafkaShareGroup
	}
	return kafkaConsumerGroup
}

func kafkaShareFetchLimit(config kafkaSubscriptionConfig) int {
	return min(config.batchSize, config.concurrency)
}

func closeKafkaConsumer(consumer kafkaConsumer, mode kafkaGroupMode, timeout time.Duration) error {
	if mode == kafkaShareGroup {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err := consumer.FlushAcks(ctx)
		cancel()
		consumer.Close()
		return err
	}
	consumer.CloseAllowingRebalance()
	return nil
}

func kafkaLogger(config kafkaSubscriptionConfig) *slog.Logger {
	if config.logger != nil {
		return config.logger
	}
	return slog.Default()
}

func waitKafkaRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func waitKafkaRetryUntil(pollCtx, workCtx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-pollCtx.Done():
		return false
	case <-workCtx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func kafkaConsumerGroupCommitTimeout(config kafkaSubscriptionConfig) time.Duration {
	// 批次截止后只使用预留的 rebalance 窗口提交连续成功前缀。
	return min(config.retryMaxBackoff, config.rebalanceTimeout-config.batchProcessingTimeout)
}

func kafkaRetryDelay(base, maximum time.Duration, attempt int) time.Duration {
	delay := base
	for range attempt {
		if delay >= maximum || delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	return delay
}

func wrapKafkaError(operation string, err error) error {
	if errors.Is(err, kgo.ErrClientClosed) {
		return fmt.Errorf("%s: %w: %w", operation, coremq.ErrClosed, err)
	}
	return fmt.Errorf("%s: %w", operation, err)
}
