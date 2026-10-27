package infra

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/project-kgo/kc/pkg/mq"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeKafkaProducer struct {
	mu       sync.Mutex
	records  []*kgo.Record
	err      error
	failures int
	closed   int
}

func (p *fakeKafkaProducer) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.records = append(p.records, records...)
	err := p.err
	if p.failures > 0 {
		p.failures--
		err = errors.New("kafka unavailable")
	}
	results := make(kgo.ProduceResults, len(records))
	for index, record := range records {
		results[index] = kgo.ProduceResult{Record: record, Err: err}
	}
	return results
}

func (p *fakeKafkaProducer) Close() {
	p.mu.Lock()
	p.closed++
	p.mu.Unlock()
}

func (p *fakeKafkaProducer) snapshot() ([]*kgo.Record, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*kgo.Record(nil), p.records...), p.closed
}

type fakeKafkaConsumer struct {
	mu          sync.Mutex
	polls       []kgo.Fetches
	pollWake    chan struct{}
	marks       []*kgo.Record
	commits     int
	paused      map[kafkaPartitionKey]bool
	pauseCount  int
	resumeCount int
	allowCount  int
	closeCount  int
	shareClose  int
	flushCount  int
	pollStarted chan struct{}
	pollOnce    sync.Once
}

func newFakeKafkaConsumer() *fakeKafkaConsumer {
	return &fakeKafkaConsumer{
		pollWake: make(chan struct{}, 1), paused: make(map[kafkaPartitionKey]bool),
		pollStarted: make(chan struct{}),
	}
}

func (c *fakeKafkaConsumer) push(fetches kgo.Fetches) {
	c.mu.Lock()
	c.polls = append(c.polls, fetches)
	c.mu.Unlock()
	select {
	case c.pollWake <- struct{}{}:
	default:
	}
}

func (c *fakeKafkaConsumer) PollRecords(ctx context.Context, _ int) kgo.Fetches {
	for {
		c.mu.Lock()
		if len(c.polls) > 0 {
			fetches := c.polls[0]
			c.polls = c.polls[1:]
			c.mu.Unlock()
			return fetches
		}
		c.mu.Unlock()
		c.pollOnce.Do(func() { close(c.pollStarted) })
		select {
		case <-ctx.Done():
			return kgo.NewErrFetch(ctx.Err())
		case <-c.pollWake:
		}
	}
}

func (c *fakeKafkaConsumer) AllowRebalance() {
	c.mu.Lock()
	c.allowCount++
	c.mu.Unlock()
}

func (c *fakeKafkaConsumer) PauseFetchPartitions(partitions map[string][]int32) map[string][]int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pauseCount++
	for topic, ids := range partitions {
		for _, id := range ids {
			c.paused[kafkaPartitionKey{topic: topic, partition: id}] = true
		}
	}
	return nil
}

func (c *fakeKafkaConsumer) ResumeFetchPartitions(partitions map[string][]int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumeCount++
	for topic, ids := range partitions {
		for _, id := range ids {
			delete(c.paused, kafkaPartitionKey{topic: topic, partition: id})
		}
	}
}

func (c *fakeKafkaConsumer) MarkCommitRecords(records ...*kgo.Record) {
	c.mu.Lock()
	c.marks = append(c.marks, records...)
	c.mu.Unlock()
}

func (c *fakeKafkaConsumer) CommitMarkedOffsets(context.Context) error {
	c.mu.Lock()
	c.commits++
	c.mu.Unlock()
	return nil
}

func (c *fakeKafkaConsumer) CloseAllowingRebalance() {
	c.mu.Lock()
	c.closeCount++
	c.mu.Unlock()
}

func (c *fakeKafkaConsumer) FlushAcks(context.Context) error {
	c.mu.Lock()
	c.flushCount++
	c.mu.Unlock()
	return nil
}

func (c *fakeKafkaConsumer) Close() {
	c.mu.Lock()
	c.shareClose++
	c.mu.Unlock()
}

func (c *fakeKafkaConsumer) markedIDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := make([]string, 0, len(c.marks))
	for _, record := range c.marks {
		ids = append(ids, kafkaMessageID(record))
	}
	return ids
}

func (c *fakeKafkaConsumer) isPaused(topic string, partition int32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paused[kafkaPartitionKey{topic: topic, partition: partition}]
}

func testKafkaRuntimeConfig(mode kafkaGroupMode) kafkaRuntimeConfig {
	config := kafkaRuntimeConfig{
		batchSize: 10, concurrency: 4, handlerTimeout: time.Second,
		retryBackoff: time.Millisecond, retryMaxBackoff: 10 * time.Millisecond,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if mode == kafkaShareGroup {
		config.maxDeliveryAttempts = defaultKafkaShareDeliveryAttempts
	} else {
		config.rebalanceTimeout = 2 * time.Second
	}
	return config
}

func testKafkaSubscriptionConfig(mode kafkaGroupMode) kafkaSubscriptionConfig {
	return kafkaSubscriptionConfig{mode: mode, kafkaRuntimeConfig: testKafkaRuntimeConfig(mode)}
}

type kafkaGroupTest struct {
	runner   *kafkaGroupRunner
	consumer *fakeKafkaConsumer
	producer *fakeKafkaProducer
	stop     context.CancelFunc
	done     chan struct{}
}

// startKafkaGroupTest 启动一个绑定假 consumer 的分区 worker 管理器，并分配给定分区。
func startKafkaGroupTest(
	t *testing.T,
	config kafkaSubscriptionConfig,
	producer *fakeKafkaProducer,
	handler Handler,
	partitions ...int32,
) *kafkaGroupTest {
	t.Helper()
	consumer := newFakeKafkaConsumer()
	mq := newKafkaMQ(producer, nil, kafkaConsumerGroup, config.kafkaRuntimeConfig)
	runner := newKafkaGroupRunner(mq, context.Background(), "orders", "billing", handler, config,
		make(chan struct{}, config.concurrency))
	runner.assign(map[string][]int32{"orders": partitions})
	pollCtx, stop := context.WithCancel(context.Background())
	test := &kafkaGroupTest{runner: runner, consumer: consumer, producer: producer, stop: stop, done: make(chan struct{})}
	go func() {
		defer close(test.done)
		_, _ = runner.consume(pollCtx, consumer)
	}()
	<-consumer.pollStarted
	t.Cleanup(test.close)
	return test
}

func (test *kafkaGroupTest) close() {
	test.stop()
	<-test.done
}

func waitKafkaTest(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not met in time")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestKafkaPublishDoesNotCopyPayload(t *testing.T) {
	producer := &fakeKafkaProducer{}
	mq := newKafkaMQ(producer, nil, kafkaConsumerGroup, testKafkaRuntimeConfig(kafkaConsumerGroup))
	message := &Message{
		Key: []byte("key"), Body: []byte("payload"),
		Headers: map[string][]byte{"trace": []byte("value")},
	}
	if err := mq.Publish(context.Background(), "orders", message); err != nil {
		t.Fatal(err)
	}
	records, _ := producer.snapshot()
	if len(records) != 1 || &records[0].Key[0] != &message.Key[0] || &records[0].Value[0] != &message.Body[0] ||
		&records[0].Headers[0].Value[0] != &message.Headers["trace"][0] {
		t.Fatal("Kafka publish unexpectedly copied payload data")
	}
}

func TestKafkaConsumerGroupRetriesSameMessageInPlace(t *testing.T) {
	producer := &fakeKafkaProducer{}
	config := testKafkaSubscriptionConfig(kafkaConsumerGroup)
	config.maxRetries = 2
	record := &kgo.Record{Topic: "orders", Partition: 1, Offset: 8, Value: []byte("payload")}
	var calls int
	var first *Message
	runner := newKafkaGroupRunner(mqForKafkaTest(producer, config), context.Background(), "orders", "billing",
		func(_ context.Context, message *Message) error {
			calls++
			if first == nil {
				first = message
			} else if first != message {
				t.Fatal("retry received a different Message instance")
			}
			if calls < 3 {
				return errors.New("temporary")
			}
			return nil
		}, config, make(chan struct{}, 1))
	if !runner.process(newKafkaPartitionWorker(context.Background(), "orders", 1), record) {
		t.Fatal("record was not completed")
	}
	if calls != 3 || first == nil || &first.Body[0] != &record.Value[0] {
		t.Fatalf("handler calls/message = %d/%#v", calls, first)
	}
	if records, _ := producer.snapshot(); len(records) != 0 {
		t.Fatalf("unexpected DLQ records = %d", len(records))
	}
}

func TestKafkaConsumerGroupMovesExhaustedMessageToDLQ(t *testing.T) {
	producer := &fakeKafkaProducer{}
	config := testKafkaSubscriptionConfig(kafkaConsumerGroup)
	config.maxRetries = 1
	record := &kgo.Record{
		Topic: "orders", Partition: 2, Offset: 12, Key: []byte("key"), Value: []byte("payload"),
		Headers: []kgo.RecordHeader{{Key: "trace", Value: []byte("abc")}},
	}
	var calls int
	runner := newKafkaGroupRunner(mqForKafkaTest(producer, config), context.Background(), "orders", "billing",
		func(context.Context, *Message) error { calls++; return errors.New("poison") }, config, make(chan struct{}, 1))
	if !runner.process(newKafkaPartitionWorker(context.Background(), "orders", 2), record) {
		t.Fatal("record was not completed")
	}
	records, _ := producer.snapshot()
	if calls != 2 || len(records) != 1 || records[0].Topic != "orders.dlq" {
		t.Fatalf("calls/DLQ = %d/%v", calls, records)
	}
	if &records[0].Key[0] != &record.Key[0] || &records[0].Value[0] != &record.Value[0] ||
		&records[0].Headers[0].Value[0] != &record.Headers[0].Value[0] {
		t.Fatal("DLQ unexpectedly copied source payload")
	}
	headers := kafkaHeaders(records[0])
	if string(headers[kafkaDLQHeaderSourceTopic]) != "orders" || string(headers[kafkaDLQHeaderSourceGroup]) != "billing" ||
		string(headers[kafkaDLQHeaderSourceMessageID]) != "2:12" || string(headers[kafkaDLQHeaderDeliveryCount]) != "2" ||
		string(headers[kafkaDLQHeaderError]) != "poison" || len(headers[kafkaDLQHeaderFailedAt]) == 0 {
		t.Fatalf("DLQ headers = %v", headers)
	}
}

func TestKafkaConsumerGroupRetriesDLQUntilAvailable(t *testing.T) {
	producer := &fakeKafkaProducer{failures: 3}
	config := testKafkaSubscriptionConfig(kafkaConsumerGroup)
	runner := newKafkaGroupRunner(mqForKafkaTest(producer, config), context.Background(), "orders", "billing",
		func(context.Context, *Message) error { return errors.New("poison") }, config, make(chan struct{}, 1))
	if !runner.process(newKafkaPartitionWorker(context.Background(), "orders", 0), &kgo.Record{Topic: "orders"}) {
		t.Fatal("record was not completed after DLQ recovered")
	}
	if records, _ := producer.snapshot(); len(records) != 4 {
		t.Fatalf("DLQ produce attempts = %d, want 4", len(records))
	}
}

func TestKafkaConsumerGroupProcessesPartitionsInParallelAndInOrder(t *testing.T) {
	config := testKafkaSubscriptionConfig(kafkaConsumerGroup)
	release := make(chan struct{})
	var mu sync.Mutex
	var order []string
	test := startKafkaGroupTest(t, config, &fakeKafkaProducer{}, func(_ context.Context, message *Message) error {
		if message.ID == "0:1" {
			<-release
		}
		mu.Lock()
		order = append(order, message.ID)
		mu.Unlock()
		return nil
	}, 0, 1)
	test.consumer.push(fetchKafkaRecords(
		&kgo.Record{Topic: "orders", Partition: 0, Offset: 1},
		&kgo.Record{Topic: "orders", Partition: 0, Offset: 2},
		&kgo.Record{Topic: "orders", Partition: 1, Offset: 1},
		&kgo.Record{Topic: "orders", Partition: 1, Offset: 2},
	))
	// 分区 0 的首条消息阻塞时，分区 1 仍然完成处理并标记提交。
	waitKafkaTest(t, func() bool { return len(test.consumer.markedIDs()) == 2 })
	if got := test.consumer.markedIDs(); !reflect.DeepEqual(got, []string{"1:1", "1:2"}) {
		t.Fatalf("marks while partition 0 blocked = %v", got)
	}
	close(release)
	waitKafkaTest(t, func() bool { return len(test.consumer.markedIDs()) == 4 })
	if got := test.consumer.markedIDs(); !reflect.DeepEqual(got[2:], []string{"0:1", "0:2"}) {
		t.Fatalf("partition 0 marks = %v", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(order, []string{"1:1", "1:2", "0:1", "0:2"}) {
		t.Fatalf("handler order = %v", order)
	}
}

func TestKafkaConsumerGroupDLQFailureOnlyBlocksItsPartition(t *testing.T) {
	config := testKafkaSubscriptionConfig(kafkaConsumerGroup)
	producer := &fakeKafkaProducer{err: errors.New("kafka unavailable")}
	test := startKafkaGroupTest(t, config, producer, func(_ context.Context, message *Message) error {
		if message.ID == "0:1" {
			return errors.New("poison")
		}
		return nil
	}, 0, 1)
	test.consumer.push(fetchKafkaRecords(
		&kgo.Record{Topic: "orders", Partition: 0, Offset: 1},
		&kgo.Record{Topic: "orders", Partition: 0, Offset: 2},
		&kgo.Record{Topic: "orders", Partition: 1, Offset: 1},
	))
	waitKafkaTest(t, func() bool {
		records, _ := producer.snapshot()
		return len(test.consumer.markedIDs()) == 1 && len(records) >= 3
	})

	test.runner.revoke(map[string][]int32{"orders": {0}})
	if got := test.consumer.markedIDs(); !reflect.DeepEqual(got, []string{"1:1"}) {
		t.Fatalf("marks = %v, poison partition must not advance", got)
	}
	test.consumer.mu.Lock()
	defer test.consumer.mu.Unlock()
	if test.consumer.commits != 1 || test.consumer.closeCount != 0 {
		t.Fatalf("commits/closes = %d/%d", test.consumer.commits, test.consumer.closeCount)
	}
}

func TestKafkaConsumerGroupPausesBackloggedPartition(t *testing.T) {
	config := testKafkaSubscriptionConfig(kafkaConsumerGroup)
	config.batchSize = 2
	release := make(chan struct{})
	test := startKafkaGroupTest(t, config, &fakeKafkaProducer{}, func(_ context.Context, message *Message) error {
		if message.ID == "0:1" {
			<-release
		}
		return nil
	}, 0)
	// 积压达到 2*batchSize 时暂停，剩余 batchSize 条时恢复。
	test.consumer.push(fetchKafkaRecords(
		&kgo.Record{Topic: "orders", Partition: 0, Offset: 1},
		&kgo.Record{Topic: "orders", Partition: 0, Offset: 2},
		&kgo.Record{Topic: "orders", Partition: 0, Offset: 3},
		&kgo.Record{Topic: "orders", Partition: 0, Offset: 4},
		&kgo.Record{Topic: "orders", Partition: 0, Offset: 5},
	))
	waitKafkaTest(t, func() bool { return test.consumer.isPaused("orders", 0) })
	close(release)
	waitKafkaTest(t, func() bool { return len(test.consumer.markedIDs()) == 5 })
	if test.consumer.isPaused("orders", 0) {
		t.Fatal("partition stayed paused after backlog drained")
	}
}

func TestKafkaConsumerGroupRevokeWaitsForInFlightRecordThenCommits(t *testing.T) {
	config := testKafkaSubscriptionConfig(kafkaConsumerGroup)
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	test := startKafkaGroupTest(t, config, &fakeKafkaProducer{}, func(_ context.Context, message *Message) error {
		calls.Add(1)
		if message.ID == "0:1" {
			close(started)
			<-release
		}
		return nil
	}, 0)
	test.consumer.push(fetchKafkaRecords(
		&kgo.Record{Topic: "orders", Partition: 0, Offset: 1},
		&kgo.Record{Topic: "orders", Partition: 0, Offset: 2},
	))
	<-started
	revoked := make(chan struct{})
	go func() {
		test.runner.revoke(map[string][]int32{"orders": {0}})
		close(revoked)
	}()
	select {
	case <-revoked:
		t.Fatal("revoke returned before the in-flight record finished")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	<-revoked
	if got := test.consumer.markedIDs(); !reflect.DeepEqual(got, []string{"0:1"}) || calls.Load() != 1 {
		t.Fatalf("marks/calls = %v/%d, queued record must not start after revoke", got, calls.Load())
	}
	test.consumer.mu.Lock()
	defer test.consumer.mu.Unlock()
	if test.consumer.commits != 1 {
		t.Fatalf("commits = %d", test.consumer.commits)
	}
}

func TestKafkaConsumerGroupRevokeCancelsSlowHandler(t *testing.T) {
	config := testKafkaSubscriptionConfig(kafkaConsumerGroup)
	config.rebalanceTimeout = 40 * time.Millisecond
	started := make(chan struct{})
	cause := make(chan error, 1)
	test := startKafkaGroupTest(t, config, &fakeKafkaProducer{}, func(ctx context.Context, _ *Message) error {
		close(started)
		<-ctx.Done()
		cause <- context.Cause(ctx)
		return ctx.Err()
	}, 0)
	test.consumer.push(fetchKafkaRecords(&kgo.Record{Topic: "orders", Partition: 0, Offset: 1}))
	<-started
	test.runner.revoke(map[string][]int32{"orders": {0}})
	if err := <-cause; !errors.Is(err, errKafkaPartitionRevoked) {
		t.Fatalf("handler cancel cause = %v", err)
	}
	if got := test.consumer.markedIDs(); len(got) != 0 {
		t.Fatalf("canceled record was marked: %v", got)
	}
}

func TestKafkaConsumerGroupDropsRecordsForUnassignedPartition(t *testing.T) {
	var calls atomic.Int32
	test := startKafkaGroupTest(t, testKafkaSubscriptionConfig(kafkaConsumerGroup), &fakeKafkaProducer{},
		func(context.Context, *Message) error { calls.Add(1); return nil }, 0)
	test.consumer.push(fetchKafkaRecords(
		&kgo.Record{Topic: "orders", Partition: 5, Offset: 1},
		&kgo.Record{Topic: "orders", Partition: 0, Offset: 1},
	))
	waitKafkaTest(t, func() bool { return len(test.consumer.markedIDs()) == 1 })
	if calls.Load() != 1 {
		t.Fatalf("handler calls = %d", calls.Load())
	}
}

func TestKafkaShareGroupAckDecisions(t *testing.T) {
	tests := []struct {
		name         string
		delivery     int32
		lockDeadline func() time.Time
		handlerErr   error
		produceErr   error
		want         kgo.AckStatus
		wantCalls    int
		wantDLQ      bool
	}{
		{name: "accept", delivery: 1, want: kgo.AckAccept, wantCalls: 1},
		{name: "release", delivery: 3, handlerErr: errors.New("retry"), want: kgo.AckRelease, wantCalls: 1},
		{name: "reject after dlq", delivery: 4, handlerErr: errors.New("poison"), want: kgo.AckReject, wantCalls: 1, wantDLQ: true},
		{name: "release after dlq failure", delivery: 4, handlerErr: errors.New("poison"), produceErr: errors.New("down"),
			want: kgo.AckRelease, wantCalls: 1, wantDLQ: true},
		{name: "exhausted delivery only writes dlq", delivery: 5, want: kgo.AckReject, wantDLQ: true},
		{name: "expiring lock releases without handling", delivery: 1,
			lockDeadline: func() time.Time { return time.Now().Add(kafkaShareLockMargin / 2) }, want: kgo.AckRelease},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			producer := &fakeKafkaProducer{err: test.produceErr}
			config := testKafkaSubscriptionConfig(kafkaShareGroup)
			record := &kgo.Record{Topic: "orders", Partition: 1, Offset: 9, Value: []byte("payload")}
			var lockDeadline time.Time
			if test.lockDeadline != nil {
				lockDeadline = test.lockDeadline()
			}
			var calls int
			status := mqForKafkaTest(producer, config).processShareGroupRecord(
				context.Background(), context.Background(), "orders", "workers", record, test.delivery, lockDeadline,
				func(context.Context, *Message) error { calls++; return test.handlerErr }, config,
			)
			if status != test.want || calls != test.wantCalls {
				t.Fatalf("status/calls = %s/%d, want %s/%d", status, calls, test.want, test.wantCalls)
			}
			records, _ := producer.snapshot()
			if (len(records) == 1) != test.wantDLQ {
				t.Fatalf("DLQ records = %d, wantDLQ %v", len(records), test.wantDLQ)
			}
		})
	}
}

func TestKafkaShareGroupBacksOffBeforeRelease(t *testing.T) {
	config := testKafkaSubscriptionConfig(kafkaShareGroup)
	config.retryBackoff = 30 * time.Millisecond
	config.retryMaxBackoff = time.Second
	started := time.Now()
	status := mqForKafkaTest(&fakeKafkaProducer{}, config).processShareGroupRecord(
		context.Background(), context.Background(), "orders", "workers", &kgo.Record{}, 2, time.Time{},
		func(context.Context, *Message) error { return errors.New("retry") }, config,
	)
	// 第 2 次投递失败按 retryBackoff * 2 退避后再交还。
	if elapsed := time.Since(started); status != kgo.AckRelease || elapsed < 60*time.Millisecond {
		t.Fatalf("status/elapsed = %s/%s", status, elapsed)
	}
}

func TestKafkaShareGroupFinalAttemptReservesDLQTime(t *testing.T) {
	producer := &fakeKafkaProducer{}
	config := testKafkaSubscriptionConfig(kafkaShareGroup)
	config.handlerTimeout = time.Minute
	lockDeadline := time.Now().Add(kafkaShareLockMargin + kafkaShareDLQReserve + 30*time.Millisecond)
	status := mqForKafkaTest(producer, config).processShareGroupRecord(
		context.Background(), context.Background(), "orders", "workers", &kgo.Record{}, 4, lockDeadline,
		func(ctx context.Context, _ *Message) error { <-ctx.Done(); return ctx.Err() }, config,
	)
	if records, _ := producer.snapshot(); status != kgo.AckReject || len(records) != 1 {
		t.Fatalf("status/DLQ = %s/%d", status, len(records))
	}
}

func TestKafkaShareGroupProcessesBatchConcurrently(t *testing.T) {
	config := testKafkaSubscriptionConfig(kafkaShareGroup)
	consumer := newFakeKafkaConsumer()
	records := []*kgo.Record{
		{Topic: "orders", Partition: 0, Offset: 1},
		{Topic: "orders", Partition: 0, Offset: 2},
		{Topic: "orders", Partition: 1, Offset: 1},
	}
	consumer.push(fetchKafkaRecords(records...))
	var arrived sync.WaitGroup
	arrived.Add(len(records))
	var handled atomic.Int32
	pollCtx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = mqForKafkaTest(&fakeKafkaProducer{}, config).consumeShareGroup(
			pollCtx, context.Background(), "orders", "workers", consumer,
			func(context.Context, *Message) error {
				arrived.Done()
				arrived.Wait()
				handled.Add(1)
				return nil
			}, config, len(records),
		)
	}()
	waitKafkaTest(t, func() bool { return handled.Load() == int32(len(records)) })
	stop()
	<-done
}

func TestKafkaShareConsumerLimits(t *testing.T) {
	tests := []struct {
		batchSize, concurrency, consumers int
		want                              []int
	}{
		{batchSize: 100, concurrency: 10, want: []int{3, 3, 2, 2}},
		{batchSize: 100, concurrency: 2, want: []int{1, 1}},
		{batchSize: 100, concurrency: 10, consumers: 1, want: []int{10}},
		{batchSize: 2, concurrency: 10, want: []int{2, 2, 2, 2, 2}},
	}
	for _, test := range tests {
		config := testKafkaSubscriptionConfig(kafkaShareGroup)
		config.batchSize, config.concurrency, config.shareConsumers = test.batchSize, test.concurrency, test.consumers
		if got := kafkaShareConsumerLimits(config); !reflect.DeepEqual(got, test.want) {
			t.Fatalf("limits(batch=%d, concurrency=%d, consumers=%d) = %v, want %v",
				test.batchSize, test.concurrency, test.consumers, got, test.want)
		}
	}
}

func TestKafkaSubscriptionOptionsAreModeSpecific(t *testing.T) {
	producer := &fakeKafkaProducer{}
	traditional := newKafkaMQ(producer, nil, kafkaConsumerGroup, testKafkaRuntimeConfig(kafkaConsumerGroup))
	share := newKafkaMQ(producer, nil, kafkaShareGroup, testKafkaRuntimeConfig(kafkaShareGroup))
	if _, err := traditional.subscriptionConfig(WithKafkaShareMaxDeliveryAttempts(3)); !errors.Is(err, ErrUnsupportedSubscribeOption) {
		t.Fatalf("traditional share option error = %v", err)
	}
	if _, err := traditional.subscriptionConfig(WithKafkaShareConsumers(2)); !errors.Is(err, ErrUnsupportedSubscribeOption) {
		t.Fatalf("traditional share consumers error = %v", err)
	}
	if _, err := share.subscriptionConfig(WithKafkaMaxRetries(2)); !errors.Is(err, ErrUnsupportedSubscribeOption) {
		t.Fatalf("share retry option error = %v", err)
	}
	if _, err := traditional.subscriptionConfig(WithRedisStreamQueueDepth(2)); !errors.Is(err, ErrUnsupportedSubscribeOption) {
		t.Fatalf("redis option error = %v", err)
	}
	resolved, err := share.subscriptionConfig(WithBatchSize(100), WithConcurrency(7), WithKafkaShareMaxDeliveryAttempts(4),
		WithKafkaShareConsumers(2))
	if err != nil || resolved.batchSize != 100 || resolved.concurrency != 7 || resolved.maxDeliveryAttempts != 4 ||
		!reflect.DeepEqual(kafkaShareConsumerLimits(resolved), []int{4, 3}) {
		t.Fatalf("resolved share config = (%+v, %v)", resolved, err)
	}
	if _, err := share.subscriptionConfig(WithKafkaShareMaxDeliveryAttempts(5)); !errors.Is(err, ErrInvalidSubscribeOption) {
		t.Fatalf("share attempts above broker-safe limit error = %v", err)
	}
	if _, err := share.subscriptionConfig(WithConcurrency(2), WithKafkaShareConsumers(3)); !errors.Is(err, ErrInvalidSubscribeOption) {
		t.Fatalf("share consumers above concurrency error = %v", err)
	}
	if _, err := share.subscriptionConfig(WithBatchSize(1), WithConcurrency(100)); !errors.Is(err, ErrInvalidSubscribeOption) {
		t.Fatalf("too many share consumers error = %v", err)
	}
	share.shareDeliveryLimit = 3
	if _, err := share.subscriptionConfig(); !errors.Is(err, ErrInvalidSubscribeOption) {
		t.Fatalf("delivery attempts above discovered broker limit error = %v", err)
	}
	if _, err := share.subscriptionConfig(WithKafkaShareMaxDeliveryAttempts(2)); err != nil {
		t.Fatalf("delivery attempts below discovered broker limit error = %v", err)
	}
	defaults, err := traditional.subscriptionConfig()
	if err != nil || defaults.startOffset == nil || *defaults.startOffset != KafkaStartOffsetLatest {
		t.Fatalf("traditional default start offset = (%v, %v), want latest", defaults.startOffset, err)
	}
	defaults, err = share.subscriptionConfig(WithKafkaShareMaxDeliveryAttempts(2))
	if err != nil || defaults.startOffset != nil {
		t.Fatalf("share default start offset = (%v, %v), want nil", defaults.startOffset, err)
	}
	resolved, err = traditional.subscriptionConfig(WithKafkaStartOffset(KafkaStartOffsetEarliest))
	if err != nil || resolved.startOffset == nil || *resolved.startOffset != KafkaStartOffsetEarliest {
		t.Fatalf("resolved traditional start offset = (%v, %v)", resolved.startOffset, err)
	}
	if _, err := share.subscriptionConfig(WithKafkaStartOffset(KafkaStartOffsetEarliest)); !errors.Is(err, ErrUnsupportedSubscribeOption) {
		t.Fatalf("share start offset error = %v, want ErrUnsupportedSubscribeOption", err)
	}
}

func TestKafkaSubscribeRequiresDeadLetterTopic(t *testing.T) {
	consumer := newFakeKafkaConsumer()
	var created atomic.Int32
	mq := newKafkaMQ(&fakeKafkaProducer{}, func(string, string, kafkaSubscriptionConfig, kafkaConsumerSetup) (kafkaConsumer, error) {
		created.Add(1)
		return consumer, nil
	}, kafkaConsumerGroup, testKafkaRuntimeConfig(kafkaConsumerGroup))
	defer mq.Close()
	handler := func(context.Context, *Message) error { return nil }

	var checked string
	mq.topicExists = func(_ context.Context, topic string) (bool, error) { checked = topic; return false, nil }
	if err := mq.Subscribe(context.Background(), "orders", "billing", handler); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("missing DLQ subscribe error = %v", err)
	}
	if checked != "orders.dlq" || created.Load() != 0 {
		t.Fatalf("checked/created = %q/%d", checked, created.Load())
	}

	// 无法确认 topic 状态时不阻断订阅。
	mq.topicExists = func(context.Context, string) (bool, error) { return false, errors.New("timeout") }
	if err := mq.Subscribe(context.Background(), "orders", "billing", handler); err != nil {
		t.Fatalf("unknown DLQ state subscribe error = %v", err)
	}
}

func TestKafkaConfigValidationAndDefaults(t *testing.T) {
	if err := validateMQConfig(MQConfig{Type: MQTypeKafka, Kafka: &KafkaConfig{Brokers: []string{"localhost:9092"}}}); err != nil {
		t.Fatal(err)
	}
	if err := validateMQConfig(MQConfig{Type: MQTypeKafkaShare, Kafka: &KafkaConfig{Brokers: []string{"localhost:9092"}}}); err != nil {
		t.Fatal(err)
	}
	for _, mechanism := range []SASLMechanism{SASLPlain, SASLSCRAMSHA256, SASLSCRAMSHA512} {
		config := MQConfig{Type: MQTypeKafka, Kafka: &KafkaConfig{
			Brokers: []string{"localhost:9092"}, TLS: true,
			SASL: &SASLConfig{Mechanism: mechanism, Username: "user", Password: "pass"},
		}}
		if err := validateMQConfig(config); err != nil {
			t.Fatalf("validate %s SASL: %v", mechanism, err)
		}
	}
	share := kafkaRuntimeConfigFrom(&KafkaConfig{}, kafkaShareGroup)
	if share.batchSize != 100 || share.concurrency != 10 || share.handlerTimeout != 15*time.Second ||
		share.maxDeliveryAttempts != 4 || share.shareConsumers != 0 || share.rebalanceTimeout != 0 {
		t.Fatalf("share defaults = %+v", share)
	}
	traditional := kafkaRuntimeConfigFrom(&KafkaConfig{}, kafkaConsumerGroup)
	if traditional.handlerTimeout != 30*time.Second || traditional.rebalanceTimeout != 60*time.Second {
		t.Fatalf("traditional defaults = %+v", traditional)
	}
	invalid := []MQConfig{
		{Type: MQTypeKafka, Kafka: &KafkaConfig{}},
		{Type: MQTypeKafka, Kafka: &KafkaConfig{Brokers: []string{"localhost:9092"}, MaxDeliveryAttempts: 2}},
		{Type: MQTypeKafka, Kafka: &KafkaConfig{Brokers: []string{"localhost:9092"}, ShareConsumers: 2}},
		{Type: MQTypeKafkaShare, Kafka: &KafkaConfig{Brokers: []string{"localhost:9092"}, MaxRetries: 1}},
		{Type: MQTypeKafkaShare, Kafka: &KafkaConfig{Brokers: []string{"localhost:9092"}, MaxDeliveryAttempts: 5}},
		{Type: MQTypeKafkaShare, Kafka: &KafkaConfig{Brokers: []string{"localhost:9092"}, RebalanceTimeout: time.Second}},
		{Type: MQTypeKafkaShare, Kafka: &KafkaConfig{Brokers: []string{"localhost:9092"}, Concurrency: 2, ShareConsumers: 3}},
		{Type: MQTypeKafkaShare, Kafka: &KafkaConfig{Brokers: []string{"localhost:9092"}, ShareConsumers: 65}},
		{Type: MQTypeKafka, Kafka: &KafkaConfig{Brokers: []string{"localhost:9092"}, SASL: &SASLConfig{Mechanism: SASLPlain}}},
	}
	for _, config := range invalid {
		if err := validateMQConfig(config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("validateMQConfig(%+v) error = %v", config, err)
		}
	}
}

func TestHandleKafkaMessageTreatsTimeoutAndPanicAsFailures(t *testing.T) {
	message := &Message{}
	if err := handleKafkaMessage(context.Background(), func(context.Context, *Message) error {
		panic("boom")
	}, message, time.Second); err == nil || err.Error() != "handler panic: boom" {
		t.Fatalf("panic error = %v", err)
	}
	if err := handleKafkaMessage(context.Background(), func(ctx context.Context, _ *Message) error {
		<-ctx.Done()
		return nil
	}, message, time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout error = %v", err)
	}
}

func TestKafkaCloseStopsSubscriptionAndUsesModeSpecificClose(t *testing.T) {
	for _, mode := range []kafkaGroupMode{kafkaConsumerGroup, kafkaShareGroup} {
		t.Run(map[kafkaGroupMode]string{kafkaConsumerGroup: "consumer", kafkaShareGroup: "share"}[mode], func(t *testing.T) {
			producer := &fakeKafkaProducer{}
			var mu sync.Mutex
			var consumers []*fakeKafkaConsumer
			var limits []int
			config := testKafkaRuntimeConfig(mode)
			mq := newKafkaMQ(producer, func(_, _ string, _ kafkaSubscriptionConfig, setup kafkaConsumerSetup) (kafkaConsumer, error) {
				mu.Lock()
				defer mu.Unlock()
				consumer := newFakeKafkaConsumer()
				consumers = append(consumers, consumer)
				limits = append(limits, setup.shareFetchLimit)
				return consumer, nil
			}, mode, config)
			if err := mq.Subscribe(context.Background(), "orders", "billing", func(context.Context, *Message) error { return nil }); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			started := append([]*fakeKafkaConsumer(nil), consumers...)
			mu.Unlock()
			for _, consumer := range started {
				<-consumer.pollStarted
			}
			if err := mq.Close(); err != nil {
				t.Fatal(err)
			}
			if err := mq.Close(); err != nil {
				t.Fatal(err)
			}
			if mode == kafkaShareGroup && !reflect.DeepEqual(limits, []int{1, 1, 1, 1}) {
				t.Fatalf("share consumer limits = %v", limits)
			}
			if mode == kafkaConsumerGroup && len(consumers) != 1 {
				t.Fatalf("consumer group clients = %d", len(consumers))
			}
			for _, consumer := range consumers {
				consumer.mu.Lock()
				if mode == kafkaShareGroup && (consumer.flushCount != 1 || consumer.shareClose != 1) {
					t.Fatalf("share flush/close = %d/%d", consumer.flushCount, consumer.shareClose)
				}
				if mode == kafkaConsumerGroup && (consumer.closeCount != 1 || consumer.commits != 1) {
					t.Fatalf("consumer close/commit = %d/%d", consumer.closeCount, consumer.commits)
				}
				consumer.mu.Unlock()
			}
			if _, producerClose := producer.snapshot(); producerClose != 1 {
				t.Fatalf("producer close = %d", producerClose)
			}
		})
	}
}

func TestKafkaCloseWaitsForInFlightHandlerAndCommit(t *testing.T) {
	producer := &fakeKafkaProducer{}
	consumer := newFakeKafkaConsumer()
	var hooks kafkaGroupHooks
	mq := newKafkaMQ(producer, func(_, _ string, _ kafkaSubscriptionConfig, setup kafkaConsumerSetup) (kafkaConsumer, error) {
		hooks = setup.hooks
		return consumer, nil
	}, kafkaConsumerGroup, testKafkaRuntimeConfig(kafkaConsumerGroup))
	started := make(chan struct{})
	release := make(chan struct{})
	if err := mq.Subscribe(context.Background(), "orders", "billing", func(context.Context, *Message) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	hooks.assigned(map[string][]int32{"orders": {0}})
	consumer.push(fetchKafkaRecords(&kgo.Record{Topic: "orders", Partition: 0, Offset: 4}))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- mq.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before handler: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish")
	}
	if got := consumer.markedIDs(); !reflect.DeepEqual(got, []string{"0:4"}) {
		t.Fatalf("marks = %v", got)
	}
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if consumer.commits != 1 {
		t.Fatalf("commits = %d", consumer.commits)
	}
}

func TestKafkaCloseContextCancelsHandlersAfterDeadline(t *testing.T) {
	consumer := newFakeKafkaConsumer()
	var hooks kafkaGroupHooks
	mq := newKafkaMQ(&fakeKafkaProducer{}, func(_, _ string, _ kafkaSubscriptionConfig, setup kafkaConsumerSetup) (kafkaConsumer, error) {
		hooks = setup.hooks
		return consumer, nil
	}, kafkaConsumerGroup, testKafkaRuntimeConfig(kafkaConsumerGroup))
	started := make(chan struct{})
	cause := make(chan error, 1)
	if err := mq.Subscribe(context.Background(), "orders", "billing", func(ctx context.Context, _ *Message) error {
		close(started)
		<-ctx.Done()
		cause <- context.Cause(ctx)
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	hooks.assigned(map[string][]int32{"orders": {0}})
	consumer.push(fetchKafkaRecords(&kgo.Record{Topic: "orders", Partition: 0, Offset: 1}))
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := mq.closeContext(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-cause; !errors.Is(err, ErrClosed) {
		t.Fatalf("handler cancel cause = %v", err)
	}
	if got := consumer.markedIDs(); len(got) != 0 {
		t.Fatalf("canceled record was marked: %v", got)
	}
}

func mqForKafkaTest(producer kafkaProducer, config kafkaSubscriptionConfig) *kafkaMQ {
	return newKafkaMQ(producer, nil, config.mode, config.kafkaRuntimeConfig)
}

func kafkaHeaders(record *kgo.Record) map[string][]byte {
	headers := make(map[string][]byte, len(record.Headers))
	for _, header := range record.Headers {
		headers[header.Key] = header.Value
	}
	return headers
}

func fetchKafkaRecords(records ...*kgo.Record) kgo.Fetches {
	grouped := make(map[kafkaPartitionKey][]*kgo.Record)
	var order []kafkaPartitionKey
	for _, record := range records {
		key := kafkaPartitionKey{topic: record.Topic, partition: record.Partition}
		if _, exists := grouped[key]; !exists {
			order = append(order, key)
		}
		grouped[key] = append(grouped[key], record)
	}
	var fetchedTopics []kgo.FetchTopic
	for _, key := range order {
		if len(fetchedTopics) == 0 || fetchedTopics[len(fetchedTopics)-1].Topic != key.topic {
			fetchedTopics = append(fetchedTopics, kgo.FetchTopic{Topic: key.topic})
		}
		topic := &fetchedTopics[len(fetchedTopics)-1]
		topic.Partitions = append(topic.Partitions, kgo.FetchPartition{Partition: key.partition, Records: grouped[key]})
	}
	return kgo.Fetches{{Topics: fetchedTopics}}
}

func TestKafkaMessageDuplicateHeaderUsesLastValueWithoutCopy(t *testing.T) {
	last := []byte("last")
	message := messageFromKafkaRecord(&kgo.Record{Headers: []kgo.RecordHeader{
		{Key: "trace", Value: []byte("first")}, {Key: "trace", Value: last},
	}})
	if !reflect.DeepEqual(message.Headers, map[string][]byte{"trace": last}) || &message.Headers["trace"][0] != &last[0] {
		t.Fatalf("headers = %v", message.Headers)
	}
}
