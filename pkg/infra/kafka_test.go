package infra

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	. "github.com/project-kgo/kc/pkg/mq"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeKafkaProducer struct {
	mu      sync.Mutex
	records []*kgo.Record
	err     error
	closed  int
}

func (p *fakeKafkaProducer) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.records = append(p.records, records...)
	results := make(kgo.ProduceResults, len(records))
	for index, record := range records {
		results[index] = kgo.ProduceResult{Record: record, Err: p.err}
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
	mu              sync.Mutex
	polls           []kgo.Fetches
	commits         [][]*kgo.Record
	commitErr       error
	allowCount      int
	closeCount      int
	shareClose      int
	flushCount      int
	pollStarted     chan struct{}
	pollStartedOnce sync.Once
}

func (c *fakeKafkaConsumer) PollRecords(ctx context.Context, _ int) kgo.Fetches {
	c.mu.Lock()
	if len(c.polls) > 0 {
		fetches := c.polls[0]
		c.polls = c.polls[1:]
		c.mu.Unlock()
		return fetches
	}
	started := c.pollStarted
	c.mu.Unlock()
	if started != nil {
		c.pollStartedOnce.Do(func() { close(started) })
	}
	<-ctx.Done()
	return kgo.NewErrFetch(ctx.Err())
}

func (c *fakeKafkaConsumer) CommitRecords(_ context.Context, records ...*kgo.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commits = append(c.commits, append([]*kgo.Record(nil), records...))
	return c.commitErr
}

func (c *fakeKafkaConsumer) AllowRebalance() {
	c.mu.Lock()
	c.allowCount++
	c.mu.Unlock()
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

func testKafkaRuntimeConfig(mode kafkaGroupMode) kafkaRuntimeConfig {
	return kafkaRuntimeConfig{
		batchSize: 10, concurrency: 4, handlerTimeout: time.Second,
		maxDeliveryAttempts:    defaultKafkaShareDeliveryAttempts,
		batchProcessingTimeout: time.Second, rebalanceTimeout: 2 * time.Second,
		retryBackoff: time.Millisecond, retryMaxBackoff: 10 * time.Millisecond,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
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
	config := kafkaSubscriptionConfig{mode: kafkaConsumerGroup, kafkaRuntimeConfig: testKafkaRuntimeConfig(kafkaConsumerGroup)}
	config.maxRetries = 2
	record := &kgo.Record{Topic: "orders", Partition: 1, Offset: 8, Value: []byte("payload")}
	var calls int
	var first *Message
	handler := func(_ context.Context, message *Message) error {
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
	}
	if err := mqForKafkaTest(producer, config).processConsumerGroupRecord(
		context.Background(), context.Background(), "orders", "billing", record, handler, config,
	); err != nil {
		t.Fatal(err)
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
	config := kafkaSubscriptionConfig{mode: kafkaConsumerGroup, kafkaRuntimeConfig: testKafkaRuntimeConfig(kafkaConsumerGroup)}
	config.maxRetries = 1
	record := &kgo.Record{
		Topic: "orders", Partition: 2, Offset: 12, Key: []byte("key"), Value: []byte("payload"),
		Headers: []kgo.RecordHeader{{Key: "trace", Value: []byte("abc")}},
	}
	var calls int
	err := mqForKafkaTest(producer, config).processConsumerGroupRecord(
		context.Background(), context.Background(), "orders", "billing", record,
		func(context.Context, *Message) error { calls++; return errors.New("poison") }, config,
	)
	if err != nil {
		t.Fatal(err)
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

func TestKafkaConsumerGroupDoesNotAdvancePastDLQFailure(t *testing.T) {
	producer := &fakeKafkaProducer{err: errors.New("kafka unavailable")}
	config := kafkaSubscriptionConfig{mode: kafkaConsumerGroup, kafkaRuntimeConfig: testKafkaRuntimeConfig(kafkaConsumerGroup)}
	records := []*kgo.Record{
		{Topic: "orders", Partition: 0, Offset: 1},
		{Topic: "orders", Partition: 0, Offset: 2},
		{Topic: "orders", Partition: 0, Offset: 3},
	}
	mq := mqForKafkaTest(producer, config)
	results := mq.processConsumerGroupBatch(
		context.Background(), context.Background(), "orders", "billing", records,
		func(_ context.Context, message *Message) error {
			if message.ID == "0:2" {
				return errors.New("poison")
			}
			return nil
		}, config,
	)
	if len(results) != 1 || results[0].lastTerminal != records[0] || results[0].err == nil {
		t.Fatalf("partition result = %+v", results)
	}
}

func TestKafkaConsumerGroupBatchTimeoutCancelsHandlerAndStopsRemaining(t *testing.T) {
	producer := &fakeKafkaProducer{}
	config := kafkaSubscriptionConfig{mode: kafkaConsumerGroup, kafkaRuntimeConfig: testKafkaRuntimeConfig(kafkaConsumerGroup)}
	config.batchProcessingTimeout = 20 * time.Millisecond
	config.handlerTimeout = time.Second
	records := []*kgo.Record{
		{Topic: "orders", Partition: 0, Offset: 1},
		{Topic: "orders", Partition: 0, Offset: 2},
	}
	var calls int
	results := mqForKafkaTest(producer, config).processConsumerGroupBatch(
		context.Background(), context.Background(), "orders", "billing", records,
		func(ctx context.Context, _ *Message) error {
			calls++
			<-ctx.Done()
			return ctx.Err()
		}, config,
	)
	if calls != 1 || len(results) != 1 || results[0].lastTerminal != nil ||
		!errors.Is(results[0].err, errKafkaBatchProcessingTimeout) {
		t.Fatalf("calls/results = %d/%+v", calls, results)
	}
	if dlq, _ := producer.snapshot(); len(dlq) != 0 {
		t.Fatalf("batch cancellation unexpectedly wrote %d DLQ records", len(dlq))
	}
}

func TestKafkaConsumerGroupDoesNotLoseEarlyRebalanceSignal(t *testing.T) {
	producer := &fakeKafkaProducer{}
	controller := &kafkaBatchController{}
	controller.interrupt(errKafkaRebalanceBlocked)
	config := kafkaSubscriptionConfig{
		mode: kafkaConsumerGroup, kafkaRuntimeConfig: testKafkaRuntimeConfig(kafkaConsumerGroup),
		batchController: controller,
	}
	var calls int
	results := mqForKafkaTest(producer, config).processConsumerGroupBatch(
		context.Background(), context.Background(), "orders", "billing",
		[]*kgo.Record{{Topic: "orders", Partition: 0, Offset: 1}},
		func(context.Context, *Message) error { calls++; return nil }, config,
	)
	if calls != 0 || len(results) != 1 || !errors.Is(results[0].err, errKafkaRebalanceBlocked) {
		t.Fatalf("calls/results = %d/%+v", calls, results)
	}
}

func TestKafkaConsumerGroupRebalanceSignalCommitsOnlySuccessfulPrefix(t *testing.T) {
	producer := &fakeKafkaProducer{}
	controller := &kafkaBatchController{}
	config := kafkaSubscriptionConfig{
		mode: kafkaConsumerGroup, kafkaRuntimeConfig: testKafkaRuntimeConfig(kafkaConsumerGroup),
		batchController: controller,
	}
	records := []*kgo.Record{
		{Topic: "orders", Partition: 0, Offset: 1},
		{Topic: "orders", Partition: 0, Offset: 2},
		{Topic: "orders", Partition: 0, Offset: 3},
	}
	consumer := &fakeKafkaConsumer{polls: []kgo.Fetches{fetchKafkaRecords(records...)}}
	var calls int
	_, err := mqForKafkaTest(producer, config).consumeConsumerGroup(
		context.Background(), context.Background(), "orders", "billing", consumer,
		func(ctx context.Context, message *Message) error {
			calls++
			if message.ID == "0:1" {
				return nil
			}
			if message.ID == "0:2" {
				controller.interrupt(errKafkaRebalanceBlocked)
				<-ctx.Done()
				return ctx.Err()
			}
			return errors.New("remaining record must not start")
		}, config,
	)
	if !errors.Is(err, errKafkaRebalanceBlocked) || calls != 2 {
		t.Fatalf("consume error/calls = %v/%d", err, calls)
	}
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if consumer.allowCount != 1 || len(consumer.commits) != 1 || len(consumer.commits[0]) != 1 ||
		consumer.commits[0][0] != records[0] {
		t.Fatalf("allow/commits = %d/%v", consumer.allowCount, consumer.commits)
	}
}

func TestKafkaShareGroupAckDecisions(t *testing.T) {
	tests := []struct {
		name       string
		delivery   int32
		handlerErr error
		produceErr error
		want       kgo.AckStatus
		wantDLQ    bool
	}{
		{name: "accept", delivery: 1, want: kgo.AckAccept},
		{name: "release", delivery: 3, handlerErr: errors.New("retry"), want: kgo.AckRelease},
		{name: "reject after dlq", delivery: 4, handlerErr: errors.New("poison"), want: kgo.AckReject, wantDLQ: true},
		{name: "release after dlq failure", delivery: 4, handlerErr: errors.New("poison"), produceErr: errors.New("down"), want: kgo.AckRelease, wantDLQ: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			producer := &fakeKafkaProducer{err: test.produceErr}
			config := kafkaSubscriptionConfig{mode: kafkaShareGroup, kafkaRuntimeConfig: testKafkaRuntimeConfig(kafkaShareGroup)}
			record := &kgo.Record{Topic: "orders", Partition: 1, Offset: 9, Value: []byte("payload")}
			status := mqForKafkaTest(producer, config).processShareGroupRecordWithDelivery(
				context.Background(), context.Background(), "orders", "workers", record, test.delivery,
				func(context.Context, *Message) error { return test.handlerErr }, config,
			)
			if status != test.want {
				t.Fatalf("status = %s, want %s", status, test.want)
			}
			records, _ := producer.snapshot()
			if (len(records) == 1) != test.wantDLQ {
				t.Fatalf("DLQ records = %d, wantDLQ %v", len(records), test.wantDLQ)
			}
		})
	}
}

func TestKafkaSubscriptionOptionsAreModeSpecific(t *testing.T) {
	producer := &fakeKafkaProducer{}
	traditional := newKafkaMQ(producer, nil, kafkaConsumerGroup, testKafkaRuntimeConfig(kafkaConsumerGroup))
	share := newKafkaMQ(producer, nil, kafkaShareGroup, testKafkaRuntimeConfig(kafkaShareGroup))
	if _, err := traditional.subscriptionConfig(WithKafkaShareMaxDeliveryAttempts(3)); !errors.Is(err, ErrUnsupportedSubscribeOption) {
		t.Fatalf("traditional share option error = %v", err)
	}
	if _, err := share.subscriptionConfig(WithKafkaMaxRetries(2)); !errors.Is(err, ErrUnsupportedSubscribeOption) {
		t.Fatalf("share retry option error = %v", err)
	}
	if _, err := traditional.subscriptionConfig(WithRedisStreamQueueDepth(2)); !errors.Is(err, ErrUnsupportedSubscribeOption) {
		t.Fatalf("redis option error = %v", err)
	}
	resolved, err := share.subscriptionConfig(WithBatchSize(100), WithConcurrency(7), WithKafkaShareMaxDeliveryAttempts(4))
	if err != nil || resolved.batchSize != 100 || resolved.concurrency != 7 || resolved.maxDeliveryAttempts != 4 ||
		kafkaShareFetchLimit(resolved) != 7 {
		t.Fatalf("resolved share config = (%+v, %v)", resolved, err)
	}
	if _, err := share.subscriptionConfig(WithKafkaShareMaxDeliveryAttempts(5)); !errors.Is(err, ErrInvalidSubscribeOption) {
		t.Fatalf("share attempts above broker-safe limit error = %v", err)
	}
	for _, mq := range []*kafkaMQ{traditional, share} {
		defaults, err := mq.subscriptionConfig()
		if err != nil || defaults.startOffset != nil {
			t.Fatalf("default start offset = (%v, %v), want nil", defaults.startOffset, err)
		}
	}
	resolved, err = traditional.subscriptionConfig(WithKafkaStartOffset(KafkaStartOffsetEarliest))
	if err != nil || resolved.startOffset == nil || *resolved.startOffset != KafkaStartOffsetEarliest {
		t.Fatalf("resolved traditional start offset = (%v, %v)", resolved.startOffset, err)
	}
	if _, err := share.subscriptionConfig(WithKafkaStartOffset(KafkaStartOffsetEarliest)); !errors.Is(err, ErrUnsupportedSubscribeOption) {
		t.Fatalf("share start offset error = %v, want ErrUnsupportedSubscribeOption", err)
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
		share.maxDeliveryAttempts != 4 || share.batchProcessingTimeout != 0 || share.rebalanceTimeout != 0 {
		t.Fatalf("share defaults = %+v", share)
	}
	traditional := kafkaRuntimeConfigFrom(&KafkaConfig{}, kafkaConsumerGroup)
	if traditional.handlerTimeout != 30*time.Second || traditional.batchProcessingTimeout != 45*time.Second ||
		traditional.rebalanceTimeout != 60*time.Second || kafkaConsumerGroupCommitTimeout(kafkaSubscriptionConfig{
		mode: kafkaConsumerGroup, kafkaRuntimeConfig: traditional,
	}) != 15*time.Second {
		t.Fatalf("traditional defaults = %+v", traditional)
	}
	invalid := []MQConfig{
		{Type: MQTypeKafka, Kafka: &KafkaConfig{}},
		{Type: MQTypeKafka, Kafka: &KafkaConfig{Brokers: []string{"localhost:9092"}, MaxDeliveryAttempts: 2}},
		{Type: MQTypeKafkaShare, Kafka: &KafkaConfig{Brokers: []string{"localhost:9092"}, MaxRetries: 1}},
		{Type: MQTypeKafkaShare, Kafka: &KafkaConfig{Brokers: []string{"localhost:9092"}, MaxDeliveryAttempts: 5}},
		{Type: MQTypeKafkaShare, Kafka: &KafkaConfig{Brokers: []string{"localhost:9092"}, BatchProcessingTimeout: time.Second}},
		{Type: MQTypeKafka, Kafka: &KafkaConfig{Brokers: []string{"localhost:9092"}, BatchProcessingTimeout: time.Minute, RebalanceTimeout: time.Minute}},
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
			consumer := &fakeKafkaConsumer{pollStarted: make(chan struct{})}
			config := testKafkaRuntimeConfig(mode)
			mq := newKafkaMQ(producer, func(string, string, kafkaSubscriptionConfig) (kafkaConsumer, error) {
				return consumer, nil
			}, mode, config)
			if err := mq.Subscribe(context.Background(), "orders", "billing", func(context.Context, *Message) error { return nil }); err != nil {
				t.Fatal(err)
			}
			select {
			case <-consumer.pollStarted:
			case <-time.After(time.Second):
				t.Fatal("consumer did not start polling")
			}
			if err := mq.Close(); err != nil {
				t.Fatal(err)
			}
			if err := mq.Close(); err != nil {
				t.Fatal(err)
			}
			consumer.mu.Lock()
			if mode == kafkaShareGroup && (consumer.flushCount != 1 || consumer.shareClose != 1) {
				t.Fatalf("share flush/close = %d/%d", consumer.flushCount, consumer.shareClose)
			}
			if mode == kafkaConsumerGroup && consumer.closeCount != 1 {
				t.Fatalf("consumer close = %d", consumer.closeCount)
			}
			consumer.mu.Unlock()
			_, producerClose := producer.snapshot()
			if producerClose != 1 {
				t.Fatalf("producer close = %d", producerClose)
			}
		})
	}
}

func TestKafkaCloseWaitsForInFlightHandlerAndCommit(t *testing.T) {
	producer := &fakeKafkaProducer{}
	record := &kgo.Record{Topic: "orders", Partition: 0, Offset: 4}
	consumer := &fakeKafkaConsumer{polls: []kgo.Fetches{fetchKafkaRecords(record)}}
	config := testKafkaRuntimeConfig(kafkaConsumerGroup)
	mq := newKafkaMQ(producer, func(string, string, kafkaSubscriptionConfig) (kafkaConsumer, error) {
		return consumer, nil
	}, kafkaConsumerGroup, config)
	started := make(chan struct{})
	release := make(chan struct{})
	if err := mq.Subscribe(context.Background(), "orders", "billing", func(context.Context, *Message) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
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
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if len(consumer.commits) != 1 || len(consumer.commits[0]) != 1 || consumer.commits[0][0] != record {
		t.Fatalf("commits = %v", consumer.commits)
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
	for _, record := range records {
		key := kafkaPartitionKey{topic: record.Topic, partition: record.Partition}
		grouped[key] = append(grouped[key], record)
	}
	topics := make(map[string][]kgo.FetchPartition)
	for key, partitionRecords := range grouped {
		topics[key.topic] = append(topics[key.topic], kgo.FetchPartition{Partition: key.partition, Records: partitionRecords})
	}
	fetchedTopics := make([]kgo.FetchTopic, 0, len(topics))
	for topic, partitions := range topics {
		fetchedTopics = append(fetchedTopics, kgo.FetchTopic{Topic: topic, Partitions: partitions})
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
