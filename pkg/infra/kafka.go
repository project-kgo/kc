package infra

import (
	"context"
	"errors"
	"fmt"
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

type kafkaProducer interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	Close()
}

type kafkaConsumer interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
	AllowRebalance()
	CloseAllowingRebalance()
}

type kafkaConsumerFactory func(topic, group string) (kafkaConsumer, error)

const (
	defaultKafkaConsumerBatchSize = 100
	defaultKafkaHandlerTimeout    = 30 * time.Second
	defaultKafkaRetryBackoff      = time.Second
	defaultKafkaRetryMaxBackoff   = 30 * time.Second
)

type kafkaSubscription struct {
	stop context.CancelFunc
	done chan struct{}
}

type kafkaPartitionKey struct {
	topic     string
	partition int32
}

type kafkaPartitionResult struct {
	lastSuccessful *kgo.Record
	err            error
}

type kafkaSubscriptionConfig struct {
	batchSize      int
	handlerTimeout time.Duration
	// concurrency 为 0 时保留 Kafka 的默认行为：不同分区不设额外并发上限。
	concurrency  int
	retryBackoff time.Duration
	retryMax     time.Duration
}

type kafkaMQ struct {
	producer    kafkaProducer
	newConsumer kafkaConsumerFactory
	batchSize   int
	handlerTTL  time.Duration
	retryBase   time.Duration
	retryMax    time.Duration

	mu            sync.Mutex
	closed        bool
	subscriptions map[*kafkaSubscription]struct{}
}

func newKafkaMQ(producer kafkaProducer, newConsumer kafkaConsumerFactory) *kafkaMQ {
	return &kafkaMQ{
		producer:      producer,
		newConsumer:   newConsumer,
		batchSize:     defaultKafkaConsumerBatchSize,
		handlerTTL:    defaultKafkaHandlerTimeout,
		retryBase:     defaultKafkaRetryBackoff,
		retryMax:      defaultKafkaRetryMaxBackoff,
		subscriptions: make(map[*kafkaSubscription]struct{}),
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
	if m.isClosed() {
		return coremq.ErrClosed
	}

	record := &kgo.Record{
		Topic:     topic,
		Key:       cloneBytes(message.Key),
		Value:     cloneBytes(message.Body),
		Timestamp: message.Timestamp,
		Headers:   make([]kgo.RecordHeader, 0, len(message.Headers)),
	}
	for key, value := range message.Headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: key, Value: cloneBytes(value)})
	}

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
	if m.isClosed() {
		return coremq.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("subscribe kafka: %w", err)
	}
	subscriptionConfig, err := m.subscriptionConfig(options...)
	if err != nil {
		return fmt.Errorf("subscribe kafka: %w", err)
	}

	client, err := m.newConsumer(topic, group)
	if err != nil {
		return fmt.Errorf("create kafka consumer: %w", err)
	}
	pollCtx, stop := context.WithCancel(ctx)
	subscription := &kafkaSubscription{stop: stop, done: make(chan struct{})}
	if !m.trackSubscription(subscription) {
		stop()
		client.CloseAllowingRebalance()
		return coremq.ErrClosed
	}

	// 消费循环由 MQ 托管，Subscribe 本身不阻塞调用方。
	// pollCtx 用于停止拉取；原始 ctx 继续承载在途 handler，确保 Close 能优雅排空。
	go m.runSubscription(pollCtx, ctx, topic, group, handler, subscriptionConfig, subscription, client)
	return nil
}

func (m *kafkaMQ) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	subscriptions := make([]*kafkaSubscription, 0, len(m.subscriptions))
	for subscription := range m.subscriptions {
		// 在发布 closed 状态前先停止所有 Poll，避免排空期间再启动新的 handler。
		subscription.stop()
		subscriptions = append(subscriptions, subscription)
	}
	m.mu.Unlock()

	for _, subscription := range subscriptions {
		<-subscription.done
	}
	m.producer.Close()
	return nil
}

func (m *kafkaMQ) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *kafkaMQ) trackSubscription(subscription *kafkaSubscription) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
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
	client kafkaConsumer,
) {
	defer func() {
		m.untrackSubscription(subscription)
		close(subscription.done)
	}()

	retryAttempt := 0
	for {
		stable, _ := m.consume(pollCtx, handlerCtx, client, handler, config)
		client.CloseAllowingRebalance()
		if pollCtx.Err() != nil {
			return
		}
		if stable {
			retryAttempt = 0
		}

		for {
			if !waitKafkaRetry(pollCtx, kafkaRetryDelay(config.retryBackoff, config.retryMax, retryAttempt)) {
				return
			}
			retryAttempt++

			var err error
			client, err = m.newConsumer(topic, group)
			if err == nil {
				break
			}
		}
	}
}

// consume 按分区并行处理一个批次，分区内部保持 Kafka offset 顺序。
// 返回错误时上层会重建同组 consumer，使未提交的消息由 Kafka 再次投递。
func (m *kafkaMQ) consume(
	pollCtx context.Context,
	handlerCtx context.Context,
	client kafkaConsumer,
	handler coremq.Handler,
	config kafkaSubscriptionConfig,
) (bool, error) {
	stable := false
	for {
		fetches := client.PollRecords(pollCtx, config.batchSize)
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
			// 非空 Poll 与 AllowRebalance 必须成对，关闭时不再启动新 handler。
			client.AllowRebalance()
			return stable, err
		}

		results := m.processKafkaBatch(pollCtx, handlerCtx, records, handler, config)
		commitRecords := make([]*kgo.Record, 0, len(results))
		var batchErr error
		for _, result := range results {
			if result.lastSuccessful != nil {
				commitRecords = append(commitRecords, result.lastSuccessful)
			}
			if batchErr == nil && result.err != nil {
				batchErr = result.err
			}
		}

		// BlockRebalanceOnPoll 保证批量提交期间仍持有这些分区。
		if len(commitRecords) > 0 {
			// 使用 handlerCtx 提交，使 MQ.Close 不会取消已经成功完成的在途消息。
			if err := client.CommitRecords(handlerCtx, commitRecords...); err != nil {
				client.AllowRebalance()
				return stable, wrapKafkaError("commit kafka messages", err)
			}
		}
		client.AllowRebalance()
		if err := pollCtx.Err(); err != nil {
			return stable, err
		}

		if batchErr != nil {
			return stable, batchErr
		}
		stable = true
	}
}

func (m *kafkaMQ) processKafkaBatch(
	pollCtx context.Context,
	handlerCtx context.Context,
	records []*kgo.Record,
	handler coremq.Handler,
	config kafkaSubscriptionConfig,
) []kafkaPartitionResult {
	partitionRecords := make(map[kafkaPartitionKey][]*kgo.Record)
	partitionOrder := make([]kafkaPartitionKey, 0)
	for _, record := range records {
		key := kafkaPartitionKey{topic: record.Topic, partition: record.Partition}
		if _, ok := partitionRecords[key]; !ok {
			partitionOrder = append(partitionOrder, key)
		}
		partitionRecords[key] = append(partitionRecords[key], record)
	}

	results := make(chan kafkaPartitionResult, len(partitionOrder))
	var semaphore chan struct{}
	if config.concurrency > 0 {
		semaphore = make(chan struct{}, config.concurrency)
	}
	var workers sync.WaitGroup
	for _, key := range partitionOrder {
		partition := partitionRecords[key]
		workers.Add(1)
		go func() {
			defer workers.Done()
			var result kafkaPartitionResult
			if semaphore != nil {
				select {
				case semaphore <- struct{}{}:
					defer func() { <-semaphore }()
				case <-pollCtx.Done():
					results <- result
					return
				}
			}
			for _, record := range partition {
				// Close 只等待已经开始的 handler，不再启动同批次中的后续消息。
				if pollCtx.Err() != nil {
					break
				}
				if err := m.handleKafkaRecord(handlerCtx, handler, record, config.handlerTimeout); err != nil {
					result.err = fmt.Errorf("handle kafka message %s: %w", messageFromKafkaRecord(record).ID, err)
					break
				}
				result.lastSuccessful = record
			}
			results <- result
		}()
	}
	workers.Wait()
	close(results)

	batchResults := make([]kafkaPartitionResult, 0, len(partitionOrder))
	for result := range results {
		batchResults = append(batchResults, result)
	}
	return batchResults
}

func (m *kafkaMQ) handleKafkaRecord(
	ctx context.Context,
	handler coremq.Handler,
	record *kgo.Record,
	handlerTimeout time.Duration,
) error {
	handlerCtx, cancel := context.WithTimeout(ctx, handlerTimeout)
	defer cancel()

	err := handler(handlerCtx, messageFromKafkaRecord(record))
	if err != nil {
		return err
	}
	// 即使 handler 错误地吞掉超时，也不能提交已经超时的处理结果。
	if err := handlerCtx.Err(); err != nil {
		return err
	}
	return nil
}

func (m *kafkaMQ) subscriptionConfig(options ...coremq.SubscribeOption) (kafkaSubscriptionConfig, error) {
	resolved, err := coremq.ResolveSubscribeOptions(options...)
	if err != nil {
		return kafkaSubscriptionConfig{}, err
	}
	if resolved.RedisStream != nil {
		return kafkaSubscriptionConfig{}, fmt.Errorf("%w: redis stream option on kafka", coremq.ErrUnsupportedSubscribeOption)
	}

	config := kafkaSubscriptionConfig{
		batchSize:      m.batchSize,
		handlerTimeout: m.handlerTTL,
		retryBackoff:   m.retryBase,
		retryMax:       m.retryMax,
	}
	if resolved.BatchSize != nil {
		config.batchSize = *resolved.BatchSize
	}
	if resolved.HandlerTimeout != nil {
		config.handlerTimeout = *resolved.HandlerTimeout
	}
	if resolved.Concurrency != nil {
		config.concurrency = *resolved.Concurrency
	}
	if resolved.RetryBackoff != nil {
		config.retryBackoff = resolved.RetryBackoff.Min
		config.retryMax = resolved.RetryBackoff.Max
	}
	return config, nil
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

func validateKafkaConfig(config MQConfig) error {
	brokers := kafkaBrokers(config)
	if len(brokers) == 0 {
		return fmt.Errorf("%w: kafka broker is empty", ErrInvalidConfig)
	}
	for _, broker := range brokers {
		if broker == "" {
			return fmt.Errorf("%w: kafka broker is empty", ErrInvalidConfig)
		}
	}

	kafkaConfig := config.Kafka
	if kafkaConfig == nil {
		return fmt.Errorf("%w: kafka config is missing", ErrInvalidConfig)
	}
	if kafkaConfig.DialTimeout < 0 {
		return fmt.Errorf("%w: kafka dial timeout is negative", ErrInvalidConfig)
	}
	if kafkaConfig.ConsumerBatchSize < 0 {
		return fmt.Errorf("%w: kafka consumer batch size is negative", ErrInvalidConfig)
	}
	if kafkaConfig.HandlerTimeout < 0 {
		return fmt.Errorf("%w: kafka handler timeout is negative", ErrInvalidConfig)
	}
	if kafkaConfig.RetryBackoff < 0 || kafkaConfig.RetryMaxBackoff < 0 {
		return fmt.Errorf("%w: kafka retry backoff is negative", ErrInvalidConfig)
	}
	_, _, retryBase, retryMax := kafkaRuntimeConfig(kafkaConfig)
	if retryMax < retryBase {
		return fmt.Errorf("%w: kafka retry max backoff is less than retry backoff", ErrInvalidConfig)
	}
	if kafkaConfig.SASL == nil {
		return nil
	}
	saslConfig := kafkaConfig.SASL
	if saslConfig.Username == "" || saslConfig.Password == "" {
		return fmt.Errorf("%w: kafka sasl credentials are empty", ErrInvalidConfig)
	}
	switch saslConfig.Mechanism {
	case SASLPlain, SASLSCRAMSHA256, SASLSCRAMSHA512:
		return nil
	default:
		return fmt.Errorf("%w: unsupported kafka sasl mechanism", ErrInvalidConfig)
	}
}

func prepareKafka(ctx context.Context, name string, config MQConfig) (preparedClient, error) {
	commonOptions := kafkaOptions(config)
	client, err := kgo.NewClient(commonOptions...)
	if err != nil {
		return preparedClient{}, fmt.Errorf("create kafka client: %w", err)
	}
	if !config.SkipCheck {
		checkCtx, cancel := checkContext(ctx, config.CheckTimeout)
		err = client.Ping(checkCtx)
		cancel()
		if err != nil {
			client.Close()
			return preparedClient{}, fmt.Errorf("kafka connection check: %w", err)
		}
	}

	mq := newKafkaMQ(client, func(topic, group string) (kafkaConsumer, error) {
		options := append([]kgo.Opt(nil), commonOptions...)
		options = append(options,
			kgo.ConsumerGroup(group),
			kgo.ConsumeTopics(topic),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
		)
		return kgo.NewClient(options...)
	})
	mq.batchSize, mq.handlerTTL, mq.retryBase, mq.retryMax = kafkaRuntimeConfig(config.Kafka)
	return preparedClient{
		name:   name,
		module: "mq",
		typ:    string(config.Type),
		commit: func() error {
			return resource.Set[coremq.MQ](name, mq)
		},
		close: func(context.Context) error {
			return mq.Close()
		},
	}, nil
}

func kafkaRuntimeConfig(config *KafkaConfig) (int, time.Duration, time.Duration, time.Duration) {
	batchSize := config.ConsumerBatchSize
	if batchSize == 0 {
		batchSize = defaultKafkaConsumerBatchSize
	}
	handlerTimeout := config.HandlerTimeout
	if handlerTimeout == 0 {
		handlerTimeout = defaultKafkaHandlerTimeout
	}
	retryBackoff := config.RetryBackoff
	if retryBackoff == 0 {
		retryBackoff = defaultKafkaRetryBackoff
	}
	retryMaxBackoff := config.RetryMaxBackoff
	if retryMaxBackoff == 0 {
		retryMaxBackoff = defaultKafkaRetryMaxBackoff
	}
	return batchSize, handlerTimeout, retryBackoff, retryMaxBackoff
}

func kafkaOptions(config MQConfig) []kgo.Opt {
	options := []kgo.Opt{kgo.SeedBrokers(kafkaBrokers(config)...)}
	if kafkaConfig := config.Kafka; kafkaConfig != nil {
		if kafkaConfig.ClientID != "" {
			options = append(options, kgo.ClientID(kafkaConfig.ClientID))
		}
		if kafkaConfig.DialTimeout > 0 {
			options = append(options, kgo.DialTimeout(kafkaConfig.DialTimeout))
		}
		if kafkaConfig.TLS {
			options = append(options, kgo.DialTLS())
		}
		if kafkaConfig.SASL != nil {
			options = append(options, kgo.SASL(kafkaSASL(kafkaConfig.SASL)))
		}
	}
	return options
}

func kafkaBrokers(config MQConfig) []string {
	var values []string
	if config.Kafka != nil {
		values = config.Kafka.Brokers
	}
	brokers := make([]string, 0, len(values))
	for _, value := range values {
		brokers = append(brokers, strings.TrimSpace(value))
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

func messageFromKafkaRecord(record *kgo.Record) *coremq.Message {
	headers := make(map[string][]byte, len(record.Headers))
	for _, header := range record.Headers {
		headers[header.Key] = cloneBytes(header.Value)
	}
	return &coremq.Message{
		ID:        fmt.Sprintf("%d:%d", record.Partition, record.Offset),
		Key:       cloneBytes(record.Key),
		Body:      cloneBytes(record.Value),
		Headers:   headers,
		Timestamp: record.Timestamp,
	}
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}

func wrapKafkaError(operation string, err error) error {
	if errors.Is(err, kgo.ErrClientClosed) {
		return fmt.Errorf("%s: %w: %w", operation, coremq.ErrClosed, err)
	}
	return fmt.Errorf("%s: %w", operation, err)
}
