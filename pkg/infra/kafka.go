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

	"github.com/project-kgo/kc/internal/mqruntime"
	coremq "github.com/project-kgo/kc/pkg/mq"
	"github.com/project-kgo/kc/pkg/resource"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

var _ coremq.MQ = (*kafkaMQ)(nil)

const (
	defaultKafkaConsumerBatchSize     = 100
	defaultKafkaConcurrency           = 10
	defaultKafkaHandlerTimeout        = 30 * time.Second
	defaultKafkaShareHandlerTimeout   = 15 * time.Second
	defaultKafkaShareDeliveryAttempts = 4
	defaultKafkaShareConsumers        = 4
	defaultKafkaRebalanceTimeout      = 60 * time.Second
	defaultKafkaRetryBackoff          = time.Second
	defaultKafkaRetryMaxBackoff       = 30 * time.Second
	kafkaMaxConsumerBatchSize         = 10000
	kafkaMaxConcurrency               = 10000
	kafkaMaxRetries                   = 10000
	kafkaMaxShareConsumers            = 64
	// broker 投递上限为 5 时，应用最多在第 4 次投递写 DLQ，给 DLQ 故障保留一次重投机会。
	kafkaBrokerShareDeliveryCountLimit = 5
	kafkaMaxShareDeliveryAttempts      = kafkaBrokerShareDeliveryCountLimit - 1
	kafkaShareDeliveryLimitConfig      = "group.share.delivery.count.limit"
	kafkaDLQProduceTimeout             = 10 * time.Second
	kafkaCommitTimeout                 = 10 * time.Second
	kafkaFlushAcksTimeout              = 10 * time.Second
	kafkaTopicCheckTimeout             = 5 * time.Second
	kafkaConsumerFetchMaxWait          = 500 * time.Millisecond
	// broker 在发出响应前就开始计算锁期限，ack 回传也需要时间，处理截止时间要提前留出余量。
	kafkaShareLockMargin = 3 * time.Second
	// share 最后一次投递时为写 DLQ 预留的锁时间。
	kafkaShareDLQReserve          = 5 * time.Second
	kafkaDLQSuffix                = ".dlq"
	kafkaDLQHeaderSourceTopic     = "kc.dlq.source_topic"
	kafkaDLQHeaderSourceGroup     = "kc.dlq.source_group"
	kafkaDLQHeaderSourceMessageID = "kc.dlq.source_message_id"
	kafkaDLQHeaderDeliveryCount   = "kc.dlq.delivery_count"
	kafkaDLQHeaderError           = "kc.dlq.error"
	kafkaDLQHeaderFailedAt        = "kc.dlq.failed_at"
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
	AllowRebalance()
	PauseFetchPartitions(map[string][]int32) map[string][]int32
	ResumeFetchPartitions(map[string][]int32)
	MarkCommitRecords(...*kgo.Record)
	CommitMarkedOffsets(context.Context) error
	CloseAllowingRebalance()
	FlushAcks(context.Context) error
	Close()
}

// kafkaGroupHooks 把 consumer group 的分区分配回调转交给订阅内的分区 worker 管理器。
type kafkaGroupHooks struct {
	assigned func(map[string][]int32)
	revoked  func(map[string][]int32)
	lost     func(map[string][]int32)
}

type kafkaConsumerSetup struct {
	shareFetchLimit int
	hooks           kafkaGroupHooks
}

type kafkaConsumerFactory func(topic, group string, config kafkaSubscriptionConfig, setup kafkaConsumerSetup) (kafkaConsumer, error)

type kafkaTopicChecker func(ctx context.Context, topic string) (bool, error)

type kafkaRuntimeConfig struct {
	batchSize           int
	concurrency         int
	handlerTimeout      time.Duration
	maxRetries          int
	maxDeliveryAttempts int
	shareConsumers      int
	rebalanceTimeout    time.Duration
	retryBackoff        time.Duration
	retryMaxBackoff     time.Duration
	logger              *slog.Logger
}

type kafkaSubscriptionConfig struct {
	kafkaRuntimeConfig
	mode        kafkaGroupMode
	startOffset *coremq.KafkaStartOffset
}

type kafkaMQ struct {
	producer    kafkaProducer
	newConsumer kafkaConsumerFactory
	config      kafkaRuntimeConfig
	mode        kafkaGroupMode
	// topicExists 为空时跳过订阅前的 DLQ topic 检查。
	topicExists kafkaTopicChecker
	// shareDeliveryLimit 是 broker 的 group.share.delivery.count.limit，0 表示未知。
	shareDeliveryLimit int

	lifecycle *mqruntime.Lifecycle
}

type kafkaPartitionKey struct {
	topic     string
	partition int32
}

// kafkaSession 是一个 Kafka client 及绑定在它上面的消费循环。
type kafkaSession struct {
	consumer kafkaConsumer
	consume  func(pollCtx context.Context) (bool, error)
}

type kafkaSessionFactory func() (kafkaSession, error)

var (
	errKafkaPartitionRevoked  = errors.New("kafka partition revoked")
	errKafkaPartitionLost     = errors.New("kafka partition lost")
	errKafkaShareLockExpiring = errors.New("kafka share acquisition lock is expiring")
	errKafkaDeliveryExhausted = errors.New("kafka share delivery attempts exhausted")
)

func newKafkaMQ(
	producer kafkaProducer,
	newConsumer kafkaConsumerFactory,
	mode kafkaGroupMode,
	config kafkaRuntimeConfig,
) *kafkaMQ {
	return &kafkaMQ{
		producer:    producer,
		newConsumer: newConsumer,
		config:      config,
		mode:        mode,
		lifecycle:   mqruntime.NewLifecycle(),
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
	if !m.lifecycle.Begin() {
		return coremq.ErrClosed
	}
	defer m.lifecycle.End()

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
	_, err := m.subscribe(ctx, ctx, topic, group, handler, options...)
	return err
}

func (m *kafkaMQ) Consume(
	ctx context.Context,
	topic string,
	group string,
	handler coremq.Handler,
	options ...coremq.SubscribeOption,
) (coremq.Subscription, error) {
	subscription, err := m.subscribe(ctx, context.WithoutCancel(ctx), topic, group, handler, options...)
	if err != nil {
		return nil, err
	}
	return subscription, nil
}

// subscribe 建立订阅；lifetime 结束时立即停止拉取并取消 Handler。
func (m *kafkaMQ) subscribe(
	ctx context.Context,
	lifetime context.Context,
	topic string,
	group string,
	handler coremq.Handler,
	options ...coremq.SubscribeOption,
) (*mqruntime.Subscription, error) {
	if err := mqruntime.ValidateSubscribe(ctx, topic, group, handler); err != nil {
		return nil, fmt.Errorf("subscribe kafka: %w", err)
	}
	config, err := m.subscriptionConfig(options...)
	if err != nil {
		return nil, fmt.Errorf("subscribe kafka: %w", err)
	}
	if !m.lifecycle.Begin() {
		return nil, coremq.ErrClosed
	}
	defer m.lifecycle.End()

	if err := m.checkDeadLetterTopic(ctx, topic, config); err != nil {
		return nil, fmt.Errorf("subscribe kafka: %w", err)
	}

	subscription := m.lifecycle.NewSubscription(lifetime)
	factories := m.sessionFactories(subscription.WorkContext(), topic, group, handler, config)
	sessions := make([]kafkaSession, 0, len(factories))
	closeSessions := func(cause error) {
		subscription.Abort(cause)
		for _, session := range sessions {
			_ = closeKafkaConsumer(session.consumer, config.mode)
		}
	}
	for _, factory := range factories {
		session, err := factory()
		if err != nil {
			closeSessions(err)
			return nil, fmt.Errorf("create kafka consumer: %w", err)
		}
		sessions = append(sessions, session)
	}
	if !m.lifecycle.Track(subscription) {
		closeSessions(coremq.ErrClosed)
		return nil, coremq.ErrClosed
	}

	go m.runSubscription(topic, group, config, subscription, factories, sessions)
	return subscription, nil
}

func (m *kafkaMQ) Close() error {
	return m.closeContext(context.Background())
}

// closeContext 停止拉取并等待已经开始的 Handler 和确认完成；ctx 结束后取消仍在执行的 Handler 再继续等待。
func (m *kafkaMQ) closeContext(ctx context.Context) error {
	return m.lifecycle.Close(ctx, func() error {
		m.producer.Close()
		return nil
	})
}

// checkDeadLetterTopic 在订阅前确认 DLQ topic 存在。producer 不自动建 topic，
// DLQ 缺失时传统组会卡住毒消息所在分区，share group 会在投递次数耗尽后被 broker 丢弃。
func (m *kafkaMQ) checkDeadLetterTopic(ctx context.Context, topic string, config kafkaSubscriptionConfig) error {
	if m.topicExists == nil {
		return nil
	}
	dlqTopic := topic + kafkaDLQSuffix
	checkCtx, cancel := context.WithTimeout(ctx, kafkaTopicCheckTimeout)
	exists, err := m.topicExists(checkCtx, dlqTopic)
	cancel()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		// 网络或权限问题不阻断订阅，运行期写 DLQ 失败时仍按各模式的策略重试。
		kafkaLogger(config).WarnContext(ctx, "检查 Kafka DLQ topic 失败",
			"topic", topic, "dlq_topic", dlqTopic, "error", err)
		return nil
	}
	if !exists {
		return fmt.Errorf("%w: kafka dlq topic %q does not exist", coremq.ErrInvalidArgument, dlqTopic)
	}
	return nil
}

func (m *kafkaMQ) sessionFactories(
	workCtx context.Context,
	topic string,
	group string,
	handler coremq.Handler,
	config kafkaSubscriptionConfig,
) []kafkaSessionFactory {
	if config.mode == kafkaShareGroup {
		limits := kafkaShareConsumerLimits(config)
		factories := make([]kafkaSessionFactory, 0, len(limits))
		for _, limit := range limits {
			factories = append(factories, func() (kafkaSession, error) {
				consumer, err := m.newConsumer(topic, group, config, kafkaConsumerSetup{shareFetchLimit: limit})
				if err != nil {
					return kafkaSession{}, err
				}
				return kafkaSession{consumer: consumer, consume: func(pollCtx context.Context) (bool, error) {
					return m.consumeShareGroup(pollCtx, workCtx, topic, group, consumer, handler, config, limit)
				}}, nil
			})
		}
		return factories
	}

	// 并发槽位在 client 重建之间共享，保证同一订阅的 Handler 并发始终不超过 concurrency。
	slots := make(chan struct{}, config.concurrency)
	return []kafkaSessionFactory{func() (kafkaSession, error) {
		runner := newKafkaGroupRunner(m, workCtx, topic, group, handler, config, slots)
		consumer, err := m.newConsumer(topic, group, config, kafkaConsumerSetup{hooks: runner.hooks()})
		if err != nil {
			return kafkaSession{}, err
		}
		return kafkaSession{consumer: consumer, consume: func(pollCtx context.Context) (bool, error) {
			return runner.consume(pollCtx, consumer)
		}}, nil
	}}
}

func (m *kafkaMQ) runSubscription(
	topic string,
	group string,
	config kafkaSubscriptionConfig,
	subscription *mqruntime.Subscription,
	factories []kafkaSessionFactory,
	sessions []kafkaSession,
) {
	defer subscription.Finish()
	pollCtx := subscription.PollContext()
	var instances sync.WaitGroup
	for index, session := range sessions {
		instances.Add(1)
		go func() {
			defer instances.Done()
			m.runSession(pollCtx, topic, group, config, factories[index], session)
		}()
	}
	instances.Wait()
}

// runSession 只在 client 不可用时重建；单条消息失败、DLQ 失败或重平衡都在 client 内部处理，不会退出消费组。
func (m *kafkaMQ) runSession(
	pollCtx context.Context,
	topic string,
	group string,
	config kafkaSubscriptionConfig,
	factory kafkaSessionFactory,
	session kafkaSession,
) {
	logCtx := context.WithoutCancel(pollCtx)
	retryAttempt := 0
	for {
		stable, err := session.consume(pollCtx)
		if closeErr := closeKafkaConsumer(session.consumer, config.mode); closeErr != nil {
			kafkaLogger(config).ErrorContext(logCtx, "关闭 Kafka consumer 失败",
				"topic", topic, "group", group, "error", closeErr)
		}
		if pollCtx.Err() != nil {
			return
		}
		if err != nil {
			kafkaLogger(config).ErrorContext(logCtx, "Kafka 消费循环中断",
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
			session, err = factory()
			if err == nil {
				break
			}
			kafkaLogger(config).ErrorContext(logCtx, "重建 Kafka consumer 失败",
				"topic", topic, "group", group, "error", err)
		}
	}
}

// kafkaGroupRunner 为传统 consumer group 的每个已分配分区维护一个串行 worker。
// 拉取循环只负责分发，分区之间互不阻塞；分区积压达到 2*batchSize 时暂停拉取该分区，
// 剩余 batchSize 条时恢复，让恢复后的存量工作覆盖下一次拉取的往返时间。
// 处理完成的消息通过 MarkCommitRecords 交给后台自动提交；分区被回收时先停止对应 worker，
// 再同步提交，避免把不属于自己的分区提交回退。
type kafkaGroupRunner struct {
	m       *kafkaMQ
	workCtx context.Context
	topic   string
	group   string
	handler coremq.Handler
	config  kafkaSubscriptionConfig
	slots   chan struct{}

	mu       sync.Mutex
	consumer kafkaConsumer
	workers  map[kafkaPartitionKey]*kafkaPartitionWorker
	closed   bool
}

func newKafkaGroupRunner(
	m *kafkaMQ,
	workCtx context.Context,
	topic string,
	group string,
	handler coremq.Handler,
	config kafkaSubscriptionConfig,
	slots chan struct{},
) *kafkaGroupRunner {
	return &kafkaGroupRunner{
		m: m, workCtx: workCtx, topic: topic, group: group, handler: handler, config: config, slots: slots,
		workers: make(map[kafkaPartitionKey]*kafkaPartitionWorker),
	}
}

func (r *kafkaGroupRunner) hooks() kafkaGroupHooks {
	return kafkaGroupHooks{assigned: r.assign, revoked: r.revoke, lost: r.lose}
}

// client 返回绑定的 consumer。分配回调可能早于 consume 开始，此时还没有消息需要处理。
func (r *kafkaGroupRunner) client() kafkaConsumer {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.consumer
}

func (r *kafkaGroupRunner) consume(pollCtx context.Context, consumer kafkaConsumer) (bool, error) {
	r.mu.Lock()
	r.consumer = consumer
	r.mu.Unlock()
	defer r.shutdown()

	stable := false
	failures := 0
	for {
		fetches := consumer.PollRecords(pollCtx, r.config.batchSize)
		if err := pollCtx.Err(); err != nil {
			consumer.AllowRebalance()
			return stable, err
		}
		// BlockRebalanceOnPoll 只覆盖内存分发这一小段，保证旧会话拉到的消息不会投递给重新分配后的 worker。
		dispatched := r.dispatch(consumer, fetches)
		consumer.AllowRebalance()

		closed, failed := logKafkaPollErrors(r.config, r.topic, r.group, fetches)
		if closed {
			return stable, wrapKafkaError("poll kafka message", kgo.ErrClientClosed)
		}
		if dispatched > 0 {
			stable = true
			failures = 0
			continue
		}
		if failed {
			if !waitKafkaRetry(pollCtx, kafkaRetryDelay(r.config.retryBackoff, r.config.retryMaxBackoff, failures)) {
				return stable, pollCtx.Err()
			}
			failures++
		}
	}
}

func (r *kafkaGroupRunner) dispatch(consumer kafkaConsumer, fetches kgo.Fetches) int {
	dispatched := 0
	fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
		if len(partition.Records) == 0 {
			return
		}
		r.mu.Lock()
		worker := r.workers[kafkaPartitionKey{topic: partition.Topic, partition: partition.Partition}]
		r.mu.Unlock()
		if worker == nil {
			// 分区已被回收，未处理的消息会由新的持有者从已提交位置重新拉取。
			return
		}
		worker.enqueue(consumer, partition.Records, 2*r.config.batchSize)
		dispatched += len(partition.Records)
	})
	return dispatched
}

func (r *kafkaGroupRunner) assign(partitions map[string][]int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	if r.consumer != nil {
		r.consumer.ResumeFetchPartitions(partitions)
	}
	for topic, ids := range partitions {
		for _, id := range ids {
			key := kafkaPartitionKey{topic: topic, partition: id}
			if _, exists := r.workers[key]; exists {
				continue
			}
			worker := newKafkaPartitionWorker(r.workCtx, topic, id)
			r.workers[key] = worker
			go r.runWorker(worker)
		}
	}
}

// revoke 在 rebalanceTimeout 内交出分区：前一半时间等待进行中的消息自然完成，
// 随后取消 Handler，最后把已完成的位置同步提交。
func (r *kafkaGroupRunner) revoke(partitions map[string][]int32) {
	workers := r.detach(partitions)
	now := time.Now()
	budget := r.config.rebalanceTimeout
	r.stopWorkers(workers, now.Add(budget/2), now.Add(budget*3/4), errKafkaPartitionRevoked)
	r.resume(partitions)
	r.commit(min(kafkaCommitTimeout, budget/4))
}

// lose 处理成员已被踢出消费组的情况：此时提交必然失败，只需尽快停止处理。
func (r *kafkaGroupRunner) lose(partitions map[string][]int32) {
	workers := r.detach(partitions)
	for _, worker := range workers {
		worker.cancel(errKafkaPartitionLost)
	}
	deadline := time.Now().Add(r.config.rebalanceTimeout / 4)
	r.stopWorkers(workers, deadline, deadline, errKafkaPartitionLost)
	r.resume(partitions)
}

func (r *kafkaGroupRunner) shutdown() {
	r.mu.Lock()
	r.closed = true
	workers := make([]*kafkaPartitionWorker, 0, len(r.workers))
	for key, worker := range r.workers {
		workers = append(workers, worker)
		delete(r.workers, key)
	}
	r.mu.Unlock()

	// 关闭时不限时等待进行中的 Handler，超时由 closeContext 通过 workCtx 取消。
	r.stopWorkers(workers, time.Time{}, time.Time{}, coremq.ErrClosed)
	r.commit(kafkaCommitTimeout)
}

func (r *kafkaGroupRunner) detach(partitions map[string][]int32) []*kafkaPartitionWorker {
	r.mu.Lock()
	defer r.mu.Unlock()
	var workers []*kafkaPartitionWorker
	for topic, ids := range partitions {
		for _, id := range ids {
			key := kafkaPartitionKey{topic: topic, partition: id}
			if worker, exists := r.workers[key]; exists {
				workers = append(workers, worker)
				delete(r.workers, key)
			}
		}
	}
	return workers
}

func (r *kafkaGroupRunner) stopWorkers(workers []*kafkaPartitionWorker, graceful, forced time.Time, cause error) {
	for _, worker := range workers {
		worker.stop()
	}
	if !waitKafkaWorkers(workers, graceful) {
		for _, worker := range workers {
			worker.cancel(cause)
		}
		if !waitKafkaWorkers(workers, forced) {
			kafkaLogger(r.config).Warn("Kafka Handler 未及时响应取消，分区已交出，可能产生重复处理",
				"topic", r.topic, "group", r.group, "error", cause)
		}
	}
	// 交出后不再标记 offset，避免仍未返回的 Handler 回退其他成员的提交。
	for _, worker := range workers {
		worker.markRevoked()
	}
}

func (r *kafkaGroupRunner) resume(partitions map[string][]int32) {
	if len(partitions) == 0 {
		return
	}
	if consumer := r.client(); consumer != nil {
		consumer.ResumeFetchPartitions(partitions)
	}
}

func (r *kafkaGroupRunner) commit(timeout time.Duration) {
	consumer := r.client()
	if consumer == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	err := consumer.CommitMarkedOffsets(ctx)
	cancel()
	if err != nil {
		kafkaLogger(r.config).Error("提交 Kafka offset 失败",
			"topic", r.topic, "group", r.group, "error", err)
	}
}

func (r *kafkaGroupRunner) runWorker(worker *kafkaPartitionWorker) {
	defer close(worker.done)
	defer worker.cancel(context.Canceled)
	resumeAt := r.config.batchSize
	for {
		record, ok := worker.next(r.client(), resumeAt)
		if !ok || !worker.acquire(r.slots) {
			return
		}
		terminal := r.process(worker, record)
		<-r.slots
		if !terminal {
			return
		}
		worker.complete(r.client(), record)
	}
}

// process 返回 true 表示消息已成功处理或已写入 DLQ，可以提交；返回 false 表示 worker 被停止。
func (r *kafkaGroupRunner) process(worker *kafkaPartitionWorker, record *kgo.Record) bool {
	logger := kafkaLogger(r.config)
	logCtx := context.WithoutCancel(worker.ctx)
	message := messageFromKafkaRecord(record)
	var handlerErr error
	for attempt := 0; ; attempt++ {
		handlerErr = handleKafkaMessage(worker.ctx, r.handler, message, r.config.handlerTimeout)
		if handlerErr == nil {
			return true
		}
		if worker.ctx.Err() != nil {
			return false
		}
		logger.WarnContext(logCtx, "处理 Kafka 消息失败",
			"topic", r.topic, "group", r.group, "message_id", message.ID,
			"attempt", attempt+1, "error", handlerErr)
		if attempt >= r.config.maxRetries {
			break
		}
		if !worker.wait(kafkaRetryDelay(r.config.retryBackoff, r.config.retryMaxBackoff, attempt)) {
			return false
		}
	}

	deliveryCount := int64(r.config.maxRetries + 1)
	for attempt := 0; ; attempt++ {
		err := r.m.publishKafkaDLQ(worker.ctx, r.topic, r.group, record, deliveryCount, handlerErr, r.config)
		if err == nil {
			return true
		}
		if worker.ctx.Err() != nil {
			return false
		}
		// DLQ 不可用时只阻塞当前分区，其余分区照常消费；分区被回收后由新的持有者重新处理。
		logger.ErrorContext(logCtx, "写入 Kafka DLQ 失败，稍后重试",
			"topic", r.topic, "group", r.group, "message_id", message.ID,
			"attempt", attempt+1, "error", err)
		if !worker.wait(kafkaRetryDelay(r.config.retryBackoff, r.config.retryMaxBackoff, attempt)) {
			return false
		}
	}
}

// kafkaPartitionWorker 串行处理单个分区的消息，保证分区内顺序和连续提交。
type kafkaPartitionWorker struct {
	topic     string
	partition int32
	ctx       context.Context
	cancel    context.CancelCauseFunc
	stopping  chan struct{}
	stopOnce  sync.Once
	notify    chan struct{}
	done      chan struct{}

	// mu 同时保护队列和暂停状态，使暂停与恢复拉取的调用顺序与队列变化一致。
	mu      sync.Mutex
	queue   []*kgo.Record
	paused  bool
	revoked bool
}

func newKafkaPartitionWorker(parent context.Context, topic string, partition int32) *kafkaPartitionWorker {
	ctx, cancel := context.WithCancelCause(parent)
	return &kafkaPartitionWorker{
		topic: topic, partition: partition, ctx: ctx, cancel: cancel,
		stopping: make(chan struct{}), notify: make(chan struct{}, 1), done: make(chan struct{}),
	}
}

func (w *kafkaPartitionWorker) partitions() map[string][]int32 {
	return map[string][]int32{w.topic: {w.partition}}
}

func (w *kafkaPartitionWorker) enqueue(consumer kafkaConsumer, records []*kgo.Record, pauseAt int) {
	w.mu.Lock()
	if w.revoked {
		w.mu.Unlock()
		return
	}
	w.queue = append(w.queue, records...)
	if !w.paused && len(w.queue) >= pauseAt {
		// 暂停后 client 会丢弃该分区已缓冲的数据并保留拉取位置，恢复后从队尾之后继续拉取。
		w.paused = true
		consumer.PauseFetchPartitions(w.partitions())
	}
	w.mu.Unlock()
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

func (w *kafkaPartitionWorker) next(consumer kafkaConsumer, resumeAt int) (*kgo.Record, bool) {
	for {
		if w.stopped() {
			return nil, false
		}
		w.mu.Lock()
		if len(w.queue) > 0 {
			record := w.queue[0]
			w.queue[0] = nil
			w.queue = w.queue[1:]
			if w.paused && len(w.queue) <= resumeAt {
				w.paused = false
				consumer.ResumeFetchPartitions(w.partitions())
			}
			w.mu.Unlock()
			return record, true
		}
		w.queue = nil
		w.mu.Unlock()

		select {
		case <-w.notify:
		case <-w.stopping:
			return nil, false
		case <-w.ctx.Done():
			return nil, false
		}
	}
}

func (w *kafkaPartitionWorker) acquire(slots chan struct{}) bool {
	select {
	case slots <- struct{}{}:
		if w.stopped() || w.ctx.Err() != nil {
			<-slots
			return false
		}
		return true
	case <-w.stopping:
		return false
	case <-w.ctx.Done():
		return false
	}
}

func (w *kafkaPartitionWorker) complete(consumer kafkaConsumer, record *kgo.Record) {
	w.mu.Lock()
	if !w.revoked {
		consumer.MarkCommitRecords(record)
	}
	w.mu.Unlock()
}

func (w *kafkaPartitionWorker) wait(delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-w.stopping:
		return false
	case <-w.ctx.Done():
		return false
	}
}

// stop 让 worker 处理完当前消息后退出，不再开始新的消息或重试。
func (w *kafkaPartitionWorker) stop() {
	w.stopOnce.Do(func() { close(w.stopping) })
}

func (w *kafkaPartitionWorker) stopped() bool {
	select {
	case <-w.stopping:
		return true
	default:
		return false
	}
}

func (w *kafkaPartitionWorker) markRevoked() {
	w.mu.Lock()
	w.revoked = true
	w.queue = nil
	w.mu.Unlock()
}

func waitKafkaWorkers(workers []*kafkaPartitionWorker, deadline time.Time) bool {
	var timeout <-chan time.Time
	if !deadline.IsZero() {
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		timeout = timer.C
	}
	for _, worker := range workers {
		select {
		case <-worker.done:
		case <-timeout:
			return false
		}
	}
	return true
}

// consumeShareGroup 驱动一个 share consumer。franz-go 在下一次 PollRecords 时会自动接受上一批未确认的消息，
// 因此必须等本批全部确认后才能再次拉取；订阅通过多个 share consumer 并行来摊薄慢消息造成的空闲槽位。
func (m *kafkaMQ) consumeShareGroup(
	pollCtx context.Context,
	workCtx context.Context,
	topic string,
	group string,
	consumer kafkaConsumer,
	handler coremq.Handler,
	config kafkaSubscriptionConfig,
	limit int,
) (bool, error) {
	stable := false
	failures := 0
	for {
		fetches := consumer.PollRecords(pollCtx, limit)
		records := fetches.Records()
		if err := pollCtx.Err(); err != nil {
			for _, record := range records {
				record.Ack(kgo.AckRelease)
			}
			return stable, err
		}
		closed, failed := logKafkaPollErrors(config, topic, group, fetches)
		if closed {
			return stable, wrapKafkaError("poll kafka share message", kgo.ErrClientClosed)
		}
		if len(records) > 0 {
			m.processShareGroupBatch(pollCtx, workCtx, topic, group, records, handler, config)
			stable = true
			failures = 0
			continue
		}
		if failed {
			if !waitKafkaRetry(pollCtx, kafkaRetryDelay(config.retryBackoff, config.retryMaxBackoff, failures)) {
				return stable, pollCtx.Err()
			}
			failures++
		}
	}
}

func (m *kafkaMQ) processShareGroupBatch(
	pollCtx context.Context,
	workCtx context.Context,
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
			record.Ack(m.processShareGroupRecord(
				pollCtx, workCtx, topic, group, record,
				record.DeliveryCount(), record.AcquisitionDeadline(), handler, config,
			))
		}()
	}
	workers.Wait()
}

// processShareGroupRecord 在 broker 的获取锁期限内完成处理和确认。lockDeadline 为零表示期限未知。
func (m *kafkaMQ) processShareGroupRecord(
	pollCtx context.Context,
	workCtx context.Context,
	topic string,
	group string,
	record *kgo.Record,
	deliveryCount int32,
	lockDeadline time.Time,
	handler coremq.Handler,
	config kafkaSubscriptionConfig,
) kgo.AckStatus {
	logger := kafkaLogger(config)
	logCtx := context.WithoutCancel(workCtx)
	message := messageFromKafkaRecord(record)
	ctx := workCtx
	if !lockDeadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadlineCause(workCtx, lockDeadline.Add(-kafkaShareLockMargin), errKafkaShareLockExpiring)
		defer cancel()
	}
	if ctx.Err() != nil {
		if workCtx.Err() == nil {
			logger.WarnContext(logCtx, "Kafka share 消息锁即将过期，交还 broker 重新投递",
				"topic", topic, "group", group, "message_id", message.ID, "delivery_count", deliveryCount)
		}
		return kgo.AckRelease
	}

	maxAttempts := int32(config.maxDeliveryAttempts)
	var handlerErr error
	if deliveryCount > maxAttempts {
		// 上一次投递已用尽处理次数但 DLQ 写入失败，本次只补写 DLQ。
		handlerErr = errKafkaDeliveryExhausted
	} else {
		handlerCtx := ctx
		if deliveryCount == maxAttempts && !lockDeadline.IsZero() {
			var cancel context.CancelFunc
			handlerCtx, cancel = context.WithDeadlineCause(
				ctx, lockDeadline.Add(-kafkaShareLockMargin-kafkaShareDLQReserve), errKafkaShareLockExpiring,
			)
			defer cancel()
		}
		handlerErr = handleKafkaMessage(handlerCtx, handler, message, config.handlerTimeout)
		if handlerErr == nil {
			return kgo.AckAccept
		}
		if workCtx.Err() != nil {
			return kgo.AckRelease
		}
		logger.WarnContext(logCtx, "处理 Kafka share 消息失败",
			"topic", topic, "group", group, "message_id", message.ID,
			"delivery_count", deliveryCount, "error", handlerErr)
		if ctx.Err() != nil {
			return kgo.AckRelease
		}
		if deliveryCount < maxAttempts {
			// broker 释放后会立即重投，先持有一段退避时间，避免下游故障时瞬间耗尽投递次数。
			waitKafkaRetryUntil(pollCtx, ctx,
				kafkaRetryDelay(config.retryBackoff, config.retryMaxBackoff, int(deliveryCount)-1))
			return kgo.AckRelease
		}
	}

	if err := m.publishKafkaDLQ(ctx, topic, group, record, int64(deliveryCount), handlerErr, config); err != nil {
		logger.ErrorContext(logCtx, "写入 Kafka share DLQ 失败",
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
	produceCtx, cancel := context.WithTimeout(parent, kafkaDLQProduceTimeout)
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

// logKafkaPollErrors 记录拉取中的分区错误，返回 client 是否已关闭以及是否存在需要退避的错误。
func logKafkaPollErrors(config kafkaSubscriptionConfig, topic, group string, fetches kgo.Fetches) (closed, failed bool) {
	fetches.EachError(func(fetchTopic string, partition int32, err error) {
		switch {
		case errors.Is(err, kgo.ErrClientClosed):
			closed = true
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		default:
			failed = true
			kafkaLogger(config).Error("拉取 Kafka 消息失败",
				"topic", topic, "group", group, "fetch_topic", fetchTopic, "partition", partition, "error", err)
		}
	})
	return closed, failed
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
	if m.mode == kafkaConsumerGroup {
		// 新 consumer group 没有已提交 offset 时，默认只消费订阅后产生的消息。
		startOffset := coremq.KafkaStartOffsetLatest
		config.startOffset = &startOffset
	}
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
		if resolved.Kafka.ShareConsumers != nil {
			if m.mode != kafkaShareGroup {
				return kafkaSubscriptionConfig{}, fmt.Errorf("%w: share consumers option on traditional kafka", coremq.ErrUnsupportedSubscribeOption)
			}
			config.shareConsumers = *resolved.Kafka.ShareConsumers
		}
	}
	if config.batchSize > kafkaMaxConsumerBatchSize || config.concurrency > kafkaMaxConcurrency || config.maxRetries > kafkaMaxRetries ||
		config.maxDeliveryAttempts > kafkaMaxShareDeliveryAttempts || config.shareConsumers > kafkaMaxShareConsumers {
		return kafkaSubscriptionConfig{}, fmt.Errorf("%w: kafka subscription value exceeds limit", coremq.ErrInvalidSubscribeOption)
	}
	if config.mode == kafkaShareGroup {
		if config.shareConsumers > config.concurrency {
			return kafkaSubscriptionConfig{}, fmt.Errorf("%w: kafka share consumers exceed concurrency", coremq.ErrInvalidSubscribeOption)
		}
		if len(kafkaShareConsumerLimits(config)) > kafkaMaxShareConsumers {
			return kafkaSubscriptionConfig{}, fmt.Errorf("%w: kafka share batch size is too small for concurrency", coremq.ErrInvalidSubscribeOption)
		}
		if m.shareDeliveryLimit > 0 && config.maxDeliveryAttempts >= m.shareDeliveryLimit {
			return kafkaSubscriptionConfig{}, fmt.Errorf("%w: kafka share delivery attempts must be less than broker %s=%d",
				coremq.ErrInvalidSubscribeOption, kafkaShareDeliveryLimitConfig, m.shareDeliveryLimit)
		}
	}
	return config, nil
}

// kafkaShareConsumerLimits 把订阅并发平均分给多个 share consumer，返回每个 consumer 单次拉取的上限。
// 每个 consumer 至少要拿到一个槽位，单个 consumer 的上限不超过 batchSize。
func kafkaShareConsumerLimits(config kafkaSubscriptionConfig) []int {
	count := config.shareConsumers
	if count == 0 {
		count = min(config.concurrency, defaultKafkaShareConsumers)
	}
	count = max(count, (config.concurrency+config.batchSize-1)/config.batchSize)
	limits := make([]int, count)
	for index := range limits {
		limits[index] = config.concurrency / count
		if index < config.concurrency%count {
			limits[index]++
		}
	}
	return limits
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
	if kafkaConfig.DialTimeout < 0 || kafkaConfig.HandlerTimeout < 0 || kafkaConfig.RebalanceTimeout < 0 ||
		kafkaConfig.RetryBackoff < 0 || kafkaConfig.RetryMaxBackoff < 0 {
		return fmt.Errorf("%w: kafka duration is negative", ErrInvalidConfig)
	}
	if kafkaConfig.ConsumerBatchSize < 0 || kafkaConfig.ConsumerBatchSize > kafkaMaxConsumerBatchSize ||
		kafkaConfig.Concurrency < 0 || kafkaConfig.Concurrency > kafkaMaxConcurrency ||
		kafkaConfig.MaxRetries < 0 || kafkaConfig.MaxRetries > kafkaMaxRetries ||
		kafkaConfig.MaxDeliveryAttempts < 0 || kafkaConfig.MaxDeliveryAttempts > kafkaMaxShareDeliveryAttempts ||
		kafkaConfig.ShareConsumers < 0 || kafkaConfig.ShareConsumers > kafkaMaxShareConsumers {
		return fmt.Errorf("%w: kafka numeric value is invalid", ErrInvalidConfig)
	}
	if config.Type == MQTypeKafka && (kafkaConfig.MaxDeliveryAttempts != 0 || kafkaConfig.ShareConsumers != 0) {
		return fmt.Errorf("%w: share options on traditional kafka", ErrInvalidConfig)
	}
	if config.Type == MQTypeKafkaShare && kafkaConfig.MaxRetries != 0 {
		return fmt.Errorf("%w: traditional retries on kafka share", ErrInvalidConfig)
	}
	if config.Type == MQTypeKafkaShare && kafkaConfig.RebalanceTimeout != 0 {
		return fmt.Errorf("%w: rebalance timeout on kafka share", ErrInvalidConfig)
	}
	runtimeConfig := kafkaRuntimeConfigFrom(kafkaConfig, kafkaMode(config.Type))
	if runtimeConfig.retryMaxBackoff < runtimeConfig.retryBackoff {
		return fmt.Errorf("%w: kafka retry max backoff is less than retry backoff", ErrInvalidConfig)
	}
	if config.Type == MQTypeKafkaShare {
		shareConfig := kafkaSubscriptionConfig{kafkaRuntimeConfig: runtimeConfig, mode: kafkaShareGroup}
		if runtimeConfig.shareConsumers > runtimeConfig.concurrency || len(kafkaShareConsumerLimits(shareConfig)) > kafkaMaxShareConsumers {
			return fmt.Errorf("%w: kafka share consumers do not fit concurrency and batch size", ErrInvalidConfig)
		}
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
	mode := kafkaMode(config.Type)
	runtimeConfig := kafkaRuntimeConfigFrom(config.Kafka, mode)
	shareDeliveryLimit := 0
	if !config.SkipCheck {
		checkCtx, cancel := checkContext(ctx, config.CheckTimeout)
		err = producer.Ping(checkCtx)
		if err != nil {
			cancel()
			producer.Close()
			return preparedClient{}, fmt.Errorf("kafka connection check: %w", err)
		}
		if mode == kafkaShareGroup {
			shareDeliveryLimit, err = checkKafkaShareDeliveryLimit(checkCtx, producer, runtimeConfig)
		}
		cancel()
		if err != nil {
			producer.Close()
			return preparedClient{}, err
		}
	}

	newConsumer := func(topic, group string, subscriptionConfig kafkaSubscriptionConfig, setup kafkaConsumerSetup) (kafkaConsumer, error) {
		logger := kafkaLogger(subscriptionConfig)
		options := append([]kgo.Opt(nil), commonOptions...)
		options = append(options, kgo.ConsumeTopics(topic))
		if mode == kafkaShareGroup {
			options = append(options,
				kgo.ShareGroup(group),
				kgo.ShareMaxRecords(int32(setup.shareFetchLimit)),
				kgo.ShareMaxRecordsStrict(),
				// 不预取：预取到本地等待处理的消息同样在消耗 broker 的获取锁时间。
				kgo.MaxConcurrentFetches(0),
				kgo.ShareAckCallback(func(_ *kgo.Client, results kgo.ShareAckResults) {
					if err := results.Error(); err != nil {
						logger.Error("确认 Kafka share 消息失败", "topic", topic, "group", group, "error", err)
					}
				}),
			)
			return kgo.NewClient(options...)
		}

		if subscriptionConfig.startOffset != nil {
			offset := kafkaOffset(*subscriptionConfig.startOffset)
			options = append(options, kgo.ConsumeStartOffset(offset), kgo.ConsumeResetOffset(offset))
		}
		hooks := setup.hooks
		options = append(options,
			kgo.ConsumerGroup(group),
			// 恢复暂停的分区要等在途的长轮询返回，与 Java 客户端默认值对齐以缩短这段空等。
			kgo.FetchMaxWait(kafkaConsumerFetchMaxWait),
			kgo.AutoCommitMarks(),
			kgo.AutoCommitCallback(func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, response *kmsg.OffsetCommitResponse, err error) {
				if err == nil {
					err = kafkaOffsetCommitError(response)
				}
				if err != nil {
					logger.Error("自动提交 Kafka offset 失败", "topic", topic, "group", group, "error", err)
				}
			}),
			kgo.BlockRebalanceOnPoll(),
			kgo.RebalanceTimeout(subscriptionConfig.rebalanceTimeout),
			kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
				hooks.assigned(partitions)
			}),
			kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
				hooks.revoked(partitions)
			}),
			kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
				hooks.lost(partitions)
			}),
		)
		return kgo.NewClient(options...)
	}

	mq := newKafkaMQ(producer, newConsumer, mode, runtimeConfig)
	mq.shareDeliveryLimit = shareDeliveryLimit
	mq.topicExists = func(ctx context.Context, topic string) (bool, error) {
		return kafkaTopicExists(ctx, producer, topic)
	}
	return preparedClient{
		name: name, module: "mq", typ: string(config.Type),
		commit: func() error { return resource.Set[coremq.MQ](name, mq) },
		close:  mq.closeContext,
	}, nil
}

// checkKafkaShareDeliveryLimit 校验 broker 的 share 投递上限。上限不大于应用的最大投递次数时，
// 消息会在写入 DLQ 之前被 broker 直接归档丢弃。读取失败只记录警告，不阻断启动。
func checkKafkaShareDeliveryLimit(ctx context.Context, client *kgo.Client, config kafkaRuntimeConfig) (int, error) {
	limit, err := kafkaShareDeliveryLimit(ctx, client)
	if err != nil {
		config.logger.WarnContext(ctx, "读取 Kafka broker share 投递上限失败，按默认值 5 处理",
			"config", kafkaShareDeliveryLimitConfig, "error", err)
		return 0, nil
	}
	if limit > 0 && config.maxDeliveryAttempts >= limit {
		return 0, fmt.Errorf("%w: kafka max delivery attempts %d must be less than broker %s=%d",
			ErrInvalidConfig, config.maxDeliveryAttempts, kafkaShareDeliveryLimitConfig, limit)
	}
	return limit, nil
}

func kafkaShareDeliveryLimit(ctx context.Context, client *kgo.Client) (int, error) {
	metadata := kmsg.NewPtrMetadataRequest()
	metadata.Topics = []kmsg.MetadataRequestTopic{}
	metadataResponse, err := metadata.RequestWith(ctx, client)
	if err != nil {
		return 0, err
	}
	if len(metadataResponse.Brokers) == 0 {
		return 0, errors.New("kafka metadata has no broker")
	}

	describe := kmsg.NewPtrDescribeConfigsRequest()
	brokerResource := kmsg.NewDescribeConfigsRequestResource()
	brokerResource.ResourceType = kmsg.ConfigResourceTypeBroker
	brokerResource.ResourceName = strconv.Itoa(int(metadataResponse.Brokers[0].NodeID))
	brokerResource.ConfigNames = []string{kafkaShareDeliveryLimitConfig}
	describe.Resources = append(describe.Resources, brokerResource)
	describeResponse, err := describe.RequestWith(ctx, client)
	if err != nil {
		return 0, err
	}
	for _, described := range describeResponse.Resources {
		if err := kerr.ErrorForCode(described.ErrorCode); err != nil {
			return 0, err
		}
		for _, entry := range described.Configs {
			if entry.Name != kafkaShareDeliveryLimitConfig || entry.Value == nil {
				continue
			}
			return strconv.Atoi(*entry.Value)
		}
	}
	return 0, nil
}

func kafkaTopicExists(ctx context.Context, client *kgo.Client, topic string) (bool, error) {
	request := kmsg.NewPtrMetadataRequest()
	requestTopic := kmsg.NewMetadataRequestTopic()
	requestTopic.Topic = kmsg.StringPtr(topic)
	request.Topics = append(request.Topics, requestTopic)
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		return false, err
	}
	for _, responseTopic := range response.Topics {
		if responseTopic.Topic == nil || *responseTopic.Topic != topic {
			continue
		}
		err := kerr.ErrorForCode(responseTopic.ErrorCode)
		if errors.Is(err, kerr.UnknownTopicOrPartition) {
			return false, nil
		}
		return err == nil, err
	}
	return false, nil
}

func kafkaOffsetCommitError(response *kmsg.OffsetCommitResponse) error {
	if response == nil {
		return nil
	}
	for _, topic := range response.Topics {
		for _, partition := range topic.Partitions {
			if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
				return fmt.Errorf("topic %s partition %d: %w", topic.Topic, partition.Partition, err)
			}
		}
	}
	return nil
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
		maxDeliveryAttempts: config.MaxDeliveryAttempts, shareConsumers: config.ShareConsumers,
		rebalanceTimeout: config.RebalanceTimeout,
		retryBackoff:     config.RetryBackoff, retryMaxBackoff: config.RetryMaxBackoff,
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
	if mode == kafkaConsumerGroup && runtimeConfig.rebalanceTimeout == 0 {
		runtimeConfig.rebalanceTimeout = defaultKafkaRebalanceTimeout
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

func closeKafkaConsumer(consumer kafkaConsumer, mode kafkaGroupMode) error {
	if mode == kafkaShareGroup {
		ctx, cancel := context.WithTimeout(context.Background(), kafkaFlushAcksTimeout)
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
