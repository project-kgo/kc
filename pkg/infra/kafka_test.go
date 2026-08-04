package infra

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/project-kgo/kc/pkg/mq"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeKafkaProducer struct {
	mu         sync.Mutex
	records    []*kgo.Record
	produceErr error
	closeCount int
}

func (p *fakeKafkaProducer) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	p.mu.Lock()
	p.records = append(p.records, records...)
	err := p.produceErr
	p.mu.Unlock()

	results := make(kgo.ProduceResults, 0, len(records))
	for _, record := range records {
		results = append(results, kgo.ProduceResult{Record: record, Err: err})
	}
	return results
}

func (p *fakeKafkaProducer) Close() {
	p.mu.Lock()
	p.closeCount++
	p.mu.Unlock()
}

func (p *fakeKafkaProducer) snapshot() ([]*kgo.Record, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*kgo.Record(nil), p.records...), p.closeCount
}

type fakeKafkaConsumer struct {
	mu          sync.Mutex
	polls       []kgo.Fetches
	poll        func(context.Context, int) kgo.Fetches
	pollLimits  []int
	commits     [][]*kgo.Record
	commitErr   error
	allowCount  int
	closeCount  int
	closeSignal chan struct{}
	started     <-chan struct{}
}

func (c *fakeKafkaConsumer) PollRecords(ctx context.Context, maxRecords int) kgo.Fetches {
	if c.poll != nil {
		return c.poll(ctx, maxRecords)
	}
	c.mu.Lock()
	c.pollLimits = append(c.pollLimits, maxRecords)
	if len(c.polls) > 0 {
		fetches := c.polls[0]
		c.polls = c.polls[1:]
		c.mu.Unlock()
		return fetches
	}
	closeSignal := c.closeSignal
	c.mu.Unlock()

	if closeSignal == nil {
		<-ctx.Done()
		return kgo.NewErrFetch(ctx.Err())
	}
	select {
	case <-ctx.Done():
		return kgo.NewErrFetch(ctx.Err())
	case <-closeSignal:
		return kgo.NewErrFetch(kgo.ErrClientClosed)
	}
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
	if c.closeSignal != nil && c.closeCount == 1 {
		close(c.closeSignal)
	}
	c.mu.Unlock()
}

func (c *fakeKafkaConsumer) snapshot() ([][]*kgo.Record, int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	commits := make([][]*kgo.Record, len(c.commits))
	for i, records := range c.commits {
		commits[i] = append([]*kgo.Record(nil), records...)
	}
	return commits, c.allowCount, c.closeCount
}

func (c *fakeKafkaConsumer) pollLimitsSnapshot() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.pollLimits...)
}

func TestKafkaMQPublishConvertsPortableMessage(t *testing.T) {
	producer := &fakeKafkaProducer{}
	mq := newKafkaMQ(producer, nil)
	timestamp := time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC)
	message := &Message{
		ID:        "receive-only-id",
		Key:       []byte("order-1"),
		Body:      []byte("created"),
		Headers:   map[string][]byte{"trace-id": []byte("trace-1")},
		Timestamp: timestamp,
	}

	if err := mq.Publish(context.Background(), "orders", message); err != nil {
		t.Fatal(err)
	}
	records, _ := producer.snapshot()
	if len(records) != 1 {
		t.Fatalf("produced records = %d, want 1", len(records))
	}
	record := records[0]
	if record.Topic != "orders" || string(record.Key) != "order-1" || string(record.Value) != "created" {
		t.Fatalf("unexpected Kafka record: %+v", record)
	}
	if !record.Timestamp.Equal(timestamp) {
		t.Fatalf("record timestamp = %v, want %v", record.Timestamp, timestamp)
	}
	if len(record.Headers) != 1 || record.Headers[0].Key != "trace-id" || string(record.Headers[0].Value) != "trace-1" {
		t.Fatalf("record headers = %v", record.Headers)
	}

	// 发布记录不能继续引用调用方可变的消息缓冲区。
	message.Key[0] = 'X'
	message.Body[0] = 'X'
	message.Headers["trace-id"][0] = 'X'
	if string(record.Key) != "order-1" || string(record.Value) != "created" || string(record.Headers[0].Value) != "trace-1" {
		t.Fatal("Kafka record unexpectedly shares message buffers")
	}
}

func TestKafkaMQPublishValidatesArgumentsAndPreservesErrors(t *testing.T) {
	tests := []struct {
		name    string
		ctx     context.Context
		topic   string
		message *Message
	}{
		{name: "nil context", topic: "orders", message: &Message{}},
		{name: "empty topic", ctx: context.Background(), topic: "  ", message: &Message{}},
		{name: "nil message", ctx: context.Background(), topic: "orders"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mq := newKafkaMQ(&fakeKafkaProducer{}, nil)
			if err := mq.Publish(test.ctx, test.topic, test.message); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("Publish() error = %v, want ErrInvalidArgument", err)
			}
		})
	}

	produceErr := errors.New("produce failed")
	producer := &fakeKafkaProducer{produceErr: produceErr}
	mq := newKafkaMQ(producer, nil)
	if err := mq.Publish(context.Background(), "orders", &Message{}); !errors.Is(err, produceErr) {
		t.Fatalf("Publish() error = %v, want wrapped producer error", err)
	}

	producer.produceErr = kgo.ErrClientClosed
	if err := mq.Publish(context.Background(), "orders", &Message{}); !errors.Is(err, ErrClosed) || !errors.Is(err, kgo.ErrClientClosed) {
		t.Fatalf("Publish() error = %v, want ErrClosed and kgo.ErrClientClosed", err)
	}
}

func TestKafkaMQSubscribeReturnsImmediately(t *testing.T) {
	consumer := blockingKafkaConsumer()
	mq := newKafkaMQ(&fakeKafkaProducer{}, func(string, string) (kafkaConsumer, error) {
		return consumer, nil
	})

	returned := make(chan error, 1)
	go func() {
		returned <- mq.Subscribe(context.Background(), "orders", "billing", func(context.Context, *Message) error { return nil })
	}()
	select {
	case err := <-returned:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe() did not return after starting the background consumer")
	}
	waitForSignal(t, consumer.pollStarted())
	if err := mq.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestKafkaMQSubscribeCommitsSuccessfulBatch(t *testing.T) {
	timestamp := time.Date(2026, time.August, 1, 13, 0, 0, 0, time.UTC)
	record := &kgo.Record{
		Topic:     "orders",
		Partition: 2,
		Offset:    41,
		Key:       []byte("order-1"),
		Value:     []byte("created"),
		Headers:   []kgo.RecordHeader{{Key: "trace-id", Value: []byte("trace-1")}},
		Timestamp: timestamp,
	}
	consumer := &fakeKafkaConsumer{polls: []kgo.Fetches{fetchWithRecord(record)}}
	producer := &fakeKafkaProducer{}
	var gotTopic, gotGroup string
	mq := newKafkaMQ(producer, func(topic, group string) (kafkaConsumer, error) {
		gotTopic, gotGroup = topic, group
		return consumer, nil
	})

	var handled *Message
	err := mq.Subscribe(context.Background(), "orders", "billing", func(_ context.Context, message *Message) error {
		handled = message
		message.Key[0] = 'X'
		message.Body[0] = 'X'
		message.Headers["trace-id"][0] = 'X'
		return nil
	}, WithBatchSize(25))
	if err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, func() bool {
		commits, _, _ := consumer.snapshot()
		return len(commits) == 1
	}, "successful batch was not committed")
	if err := mq.Close(); err != nil {
		t.Fatal(err)
	}
	if gotTopic != "orders" || gotGroup != "billing" {
		t.Fatalf("consumer factory args = (%q, %q)", gotTopic, gotGroup)
	}
	if handled == nil || handled.ID != "2:41" || !handled.Timestamp.Equal(timestamp) {
		t.Fatalf("handled message = %+v", handled)
	}
	if string(record.Key) != "order-1" || string(record.Value) != "created" || string(record.Headers[0].Value) != "trace-1" {
		t.Fatal("portable message unexpectedly shares Kafka record buffers")
	}
	commits, allowCount, closeCount := consumer.snapshot()
	if len(commits) != 1 || len(commits[0]) != 1 || commits[0][0] != record {
		t.Fatalf("commits = %v, want original record once", commits)
	}
	if allowCount != 1 || closeCount != 1 {
		t.Fatalf("allow/close counts = (%d, %d), want (1, 1)", allowCount, closeCount)
	}
	limits := consumer.pollLimitsSnapshot()
	if len(limits) == 0 || limits[0] != 25 {
		t.Fatalf("poll limits = %v, want first limit 25", limits)
	}
}

func TestKafkaMQSubscribeProcessesPartitionsConcurrently(t *testing.T) {
	records := []*kgo.Record{
		{Topic: "orders", Partition: 0, Offset: 1},
		{Topic: "orders", Partition: 1, Offset: 4},
	}
	consumer := &fakeKafkaConsumer{polls: []kgo.Fetches{fetchWithRecords(records...)}}
	mq := newKafkaMQ(&fakeKafkaProducer{}, func(string, string) (kafkaConsumer, error) {
		return consumer, nil
	})

	started := make(chan struct{}, len(records))
	release := make(chan struct{})
	if err := mq.Subscribe(context.Background(), "orders", "billing", func(context.Context, *Message) error {
		started <- struct{}{}
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for range records {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("records from different partitions were not processed concurrently")
		}
	}
	close(release)
	waitForCondition(t, func() bool {
		commits, _, _ := consumer.snapshot()
		return len(commits) == 1
	}, "concurrently processed partitions were not committed")
	if err := mq.Close(); err != nil {
		t.Fatal(err)
	}
	commits, _, _ := consumer.snapshot()
	assertCommittedOffsets(t, commits, map[int32]int64{0: 1, 1: 4})
}

func TestKafkaMQSubscribeConcurrencyOptionLimitsPartitions(t *testing.T) {
	records := []*kgo.Record{
		{Topic: "orders", Partition: 0, Offset: 1},
		{Topic: "orders", Partition: 1, Offset: 1},
		{Topic: "orders", Partition: 2, Offset: 1},
	}
	consumer := &fakeKafkaConsumer{polls: []kgo.Fetches{fetchWithRecords(records...)}}
	mq := newKafkaMQ(&fakeKafkaProducer{}, func(string, string) (kafkaConsumer, error) {
		return consumer, nil
	})

	started := make(chan struct{}, len(records))
	release := make(chan struct{})
	if err := mq.Subscribe(context.Background(), "orders", "billing", func(context.Context, *Message) error {
		started <- struct{}{}
		<-release
		return nil
	}, WithConcurrency(2)); err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, started)
	waitForSignal(t, started)
	select {
	case <-started:
		t.Fatal("handler concurrency exceeded subscription option")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	waitForCondition(t, func() bool {
		commits, _, _ := consumer.snapshot()
		return len(commits) == 1
	}, "limited partitions were not committed")
	if err := mq.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestKafkaMQSubscribeRetriesFailedPartitionAndCommitsSuccessfulPrefixes(t *testing.T) {
	handlerErr := errors.New("handler failed")
	partition0 := []*kgo.Record{
		{Topic: "orders", Partition: 0, Offset: 1},
		{Topic: "orders", Partition: 0, Offset: 2},
		{Topic: "orders", Partition: 0, Offset: 3},
	}
	partition1 := []*kgo.Record{
		{Topic: "orders", Partition: 1, Offset: 5},
		{Topic: "orders", Partition: 1, Offset: 6},
	}
	consumers := []*fakeKafkaConsumer{
		{polls: []kgo.Fetches{fetchWithRecords(append(append([]*kgo.Record{}, partition0...), partition1...)...)}},
		{polls: []kgo.Fetches{fetchWithRecords(partition0[1:]...)}},
	}
	var factoryMu sync.Mutex
	nextConsumer := 0
	mq := newKafkaMQ(&fakeKafkaProducer{}, func(string, string) (kafkaConsumer, error) {
		factoryMu.Lock()
		defer factoryMu.Unlock()
		if nextConsumer >= len(consumers) {
			return nil, errors.New("unexpected extra consumer")
		}
		consumer := consumers[nextConsumer]
		nextConsumer++
		return consumer, nil
	})
	mq.retryBase = time.Millisecond
	mq.retryMax = time.Millisecond

	var handlerMu sync.Mutex
	handled := map[string][]string{"0": {}, "1": {}}
	failedOnce := false
	err := mq.Subscribe(context.Background(), "orders", "billing", func(_ context.Context, message *Message) error {
		partition := strings.SplitN(message.ID, ":", 2)[0]
		handlerMu.Lock()
		handled[partition] = append(handled[partition], message.ID)
		if message.ID == "0:2" && !failedOnce {
			failedOnce = true
			handlerMu.Unlock()
			return handlerErr
		}
		handlerMu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, func() bool {
		commits, _, _ := consumers[1].snapshot()
		return len(commits) == 1
	}, "failed partition was not consumed again")
	if err := mq.Close(); err != nil {
		t.Fatal(err)
	}

	firstCommits, firstAllow, firstClose := consumers[0].snapshot()
	assertCommittedOffsets(t, firstCommits, map[int32]int64{0: 1, 1: 6})
	if firstAllow != 1 || firstClose != 1 {
		t.Fatalf("first consumer allow/close counts = (%d, %d), want (1, 1)", firstAllow, firstClose)
	}
	secondCommits, _, secondClose := consumers[1].snapshot()
	assertCommittedOffsets(t, secondCommits, map[int32]int64{0: 3})
	if secondClose != 1 {
		t.Fatalf("second consumer close count = %d, want 1", secondClose)
	}
	handlerMu.Lock()
	defer handlerMu.Unlock()
	if !reflect.DeepEqual(handled["0"], []string{"0:1", "0:2", "0:2", "0:3"}) {
		t.Fatalf("partition 0 handling order = %v", handled["0"])
	}
	if !reflect.DeepEqual(handled["1"], []string{"1:5", "1:6"}) {
		t.Fatalf("partition 1 handling order = %v", handled["1"])
	}
}

func TestKafkaMQSubscribeRetriesAfterCommitFailure(t *testing.T) {
	commitErr := errors.New("commit failed")
	firstRecord := &kgo.Record{Topic: "orders", Partition: 1, Offset: 7}
	secondRecord := &kgo.Record{Topic: "orders", Partition: 1, Offset: 7}
	consumers := []*fakeKafkaConsumer{
		{polls: []kgo.Fetches{fetchWithRecord(firstRecord)}, commitErr: commitErr},
		{polls: []kgo.Fetches{fetchWithRecord(secondRecord)}},
	}
	var factoryMu sync.Mutex
	nextConsumer := 0
	mq := newKafkaMQ(&fakeKafkaProducer{}, func(string, string) (kafkaConsumer, error) {
		factoryMu.Lock()
		defer factoryMu.Unlock()
		consumer := consumers[nextConsumer]
		nextConsumer++
		return consumer, nil
	})
	mq.retryBase = time.Millisecond
	mq.retryMax = time.Millisecond

	var handled int
	var handlerMu sync.Mutex
	err := mq.Subscribe(context.Background(), "orders", "billing", func(context.Context, *Message) error {
		handlerMu.Lock()
		handled++
		handlerMu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, func() bool {
		commits, _, _ := consumers[1].snapshot()
		return len(commits) == 1
	}, "record was not retried after commit failure")
	if err := mq.Close(); err != nil {
		t.Fatal(err)
	}
	handlerMu.Lock()
	defer handlerMu.Unlock()
	if handled != 2 {
		t.Fatalf("handler calls = %d, want 2", handled)
	}
}

func TestKafkaMQSubscribeRetriesTimedOutHandlerWithoutCommit(t *testing.T) {
	records := []*kgo.Record{
		{Topic: "orders", Partition: 0, Offset: 9},
		{Topic: "orders", Partition: 0, Offset: 9},
	}
	consumers := []*fakeKafkaConsumer{
		{polls: []kgo.Fetches{fetchWithRecord(records[0])}},
		{polls: []kgo.Fetches{fetchWithRecord(records[1])}},
	}
	var factoryMu sync.Mutex
	nextConsumer := 0
	mq := newKafkaMQ(&fakeKafkaProducer{}, func(string, string) (kafkaConsumer, error) {
		factoryMu.Lock()
		defer factoryMu.Unlock()
		consumer := consumers[nextConsumer]
		nextConsumer++
		return consumer, nil
	})
	var handlerMu sync.Mutex
	handled := 0
	err := mq.Subscribe(context.Background(), "orders", "billing", func(ctx context.Context, _ *Message) error {
		handlerMu.Lock()
		handled++
		attempt := handled
		handlerMu.Unlock()
		if attempt == 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}, WithHandlerTimeout(20*time.Millisecond), WithRetryBackoff(time.Millisecond, time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, func() bool {
		commits, _, _ := consumers[1].snapshot()
		return len(commits) == 1
	}, "timed out record was not retried")
	if err := mq.Close(); err != nil {
		t.Fatal(err)
	}
	firstCommits, _, _ := consumers[0].snapshot()
	if len(firstCommits) != 0 {
		t.Fatalf("timed out record commits = %v, want none", firstCommits)
	}
}

func TestKafkaMQSubscribeRejectsRedisStreamOption(t *testing.T) {
	var factoryCalls int
	mq := newKafkaMQ(&fakeKafkaProducer{}, func(string, string) (kafkaConsumer, error) {
		factoryCalls++
		return blockingKafkaConsumer(), nil
	})

	err := mq.Subscribe(
		context.Background(), "orders", "billing", func(context.Context, *Message) error { return nil },
		WithRedisStreamQueueDepth(8),
	)
	if !errors.Is(err, ErrUnsupportedSubscribeOption) {
		t.Fatalf("Subscribe() error = %v, want ErrUnsupportedSubscribeOption", err)
	}
	if factoryCalls != 0 {
		t.Fatalf("consumer factory calls = %d, want 0", factoryCalls)
	}
}

func TestKafkaMQSubscribeValidatesArgumentsAndFactoryErrors(t *testing.T) {
	var factoryCalls int
	mq := newKafkaMQ(&fakeKafkaProducer{}, func(string, string) (kafkaConsumer, error) {
		factoryCalls++
		return &fakeKafkaConsumer{}, nil
	})
	tests := []struct {
		name    string
		ctx     context.Context
		topic   string
		group   string
		handler Handler
	}{
		{name: "nil context", topic: "orders", group: "billing", handler: func(context.Context, *Message) error { return nil }},
		{name: "empty topic", ctx: context.Background(), topic: " ", group: "billing", handler: func(context.Context, *Message) error { return nil }},
		{name: "empty group", ctx: context.Background(), topic: "orders", group: " ", handler: func(context.Context, *Message) error { return nil }},
		{name: "nil handler", ctx: context.Background(), topic: "orders", group: "billing"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := mq.Subscribe(test.ctx, test.topic, test.group, test.handler); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("Subscribe() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
	if factoryCalls != 0 {
		t.Fatalf("consumer factory calls = %d, want 0", factoryCalls)
	}

	factoryErr := errors.New("factory failed")
	mq = newKafkaMQ(&fakeKafkaProducer{}, func(string, string) (kafkaConsumer, error) {
		return nil, factoryErr
	})
	if err := mq.Subscribe(context.Background(), "orders", "billing", func(context.Context, *Message) error { return nil }); !errors.Is(err, factoryErr) {
		t.Fatalf("Subscribe() error = %v, want wrapped factory error", err)
	}
}

func TestKafkaMQCloseStopsConcurrentSubscriptionsAndIsIdempotent(t *testing.T) {
	producer := &fakeKafkaProducer{}
	consumers := []*fakeKafkaConsumer{
		blockingKafkaConsumer(),
		blockingKafkaConsumer(),
	}
	var factoryMu sync.Mutex
	var nextConsumer int
	mq := newKafkaMQ(producer, func(string, string) (kafkaConsumer, error) {
		factoryMu.Lock()
		defer factoryMu.Unlock()
		consumer := consumers[nextConsumer]
		nextConsumer++
		return consumer, nil
	})

	for range consumers {
		if err := mq.Subscribe(context.Background(), "topic", "group", func(context.Context, *Message) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	for _, consumer := range consumers {
		waitForSignal(t, consumer.pollStarted())
	}

	if err := mq.Close(); err != nil {
		t.Fatal(err)
	}
	if err := mq.Close(); err != nil {
		t.Fatal(err)
	}
	for _, consumer := range consumers {
		_, _, closeCount := consumer.snapshot()
		if closeCount != 1 {
			t.Fatalf("consumer close count = %d, want 1", closeCount)
		}
	}
	_, producerCloseCount := producer.snapshot()
	if producerCloseCount != 1 {
		t.Fatalf("producer close count = %d, want 1", producerCloseCount)
	}
	if err := mq.Publish(context.Background(), "topic", &Message{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish() after Close error = %v, want ErrClosed", err)
	}
	if err := mq.Subscribe(context.Background(), "topic", "group", func(context.Context, *Message) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("Subscribe() after Close error = %v, want ErrClosed", err)
	}
}

func TestKafkaMQCloseWaitsForInFlightHandlerAndCommits(t *testing.T) {
	records := []*kgo.Record{
		{Topic: "orders", Partition: 0, Offset: 12},
		{Topic: "orders", Partition: 0, Offset: 13},
	}
	consumer := &fakeKafkaConsumer{polls: []kgo.Fetches{fetchWithRecords(records...)}}
	mq := newKafkaMQ(&fakeKafkaProducer{}, func(string, string) (kafkaConsumer, error) {
		return consumer, nil
	})

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	var handlerMu sync.Mutex
	handlerCalls := 0
	if err := mq.Subscribe(context.Background(), "orders", "billing", func(context.Context, *Message) error {
		handlerMu.Lock()
		handlerCalls++
		attempt := handlerCalls
		handlerMu.Unlock()
		if attempt == 1 {
			close(handlerStarted)
			<-releaseHandler
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, handlerStarted)

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- mq.Close()
	}()
	waitForCondition(t, mq.isClosed, "Close() did not start")
	select {
	case err := <-closeDone:
		t.Fatalf("Close() returned before the in-flight handler completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseHandler)
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close() did not return after the in-flight handler completed")
	}

	commits, allowCount, closeCount := consumer.snapshot()
	if len(commits) != 1 || len(commits[0]) != 1 || commits[0][0] != records[0] {
		t.Fatalf("commits = %v, want the drained record", commits)
	}
	handlerMu.Lock()
	defer handlerMu.Unlock()
	if handlerCalls != 1 {
		t.Fatalf("handler calls = %d, want only the in-flight record", handlerCalls)
	}
	if allowCount != 1 || closeCount != 1 {
		t.Fatalf("allow/close counts = (%d, %d), want (1, 1)", allowCount, closeCount)
	}
}

func TestKafkaMQSubscribeReturnsContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	consumer := &fakeKafkaConsumer{polls: []kgo.Fetches{kgo.NewErrFetch(context.Canceled)}}
	mq := newKafkaMQ(&fakeKafkaProducer{}, func(string, string) (kafkaConsumer, error) {
		return consumer, nil
	})

	if err := mq.Subscribe(ctx, "orders", "billing", func(context.Context, *Message) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe() error = %v, want context.Canceled", err)
	}
}

func fetchWithRecord(record *kgo.Record) kgo.Fetches {
	return fetchWithRecords(record)
}

func fetchWithRecords(records ...*kgo.Record) kgo.Fetches {
	type topicPartition struct {
		topic     string
		partition int32
	}
	grouped := make(map[topicPartition][]*kgo.Record)
	order := make([]topicPartition, 0)
	for _, record := range records {
		key := topicPartition{topic: record.Topic, partition: record.Partition}
		if _, ok := grouped[key]; !ok {
			order = append(order, key)
		}
		grouped[key] = append(grouped[key], record)
	}

	topics := make(map[string]*kgo.FetchTopic)
	topicOrder := make([]string, 0)
	for _, key := range order {
		topic := topics[key.topic]
		if topic == nil {
			topic = &kgo.FetchTopic{Topic: key.topic}
			topics[key.topic] = topic
			topicOrder = append(topicOrder, key.topic)
		}
		topic.Partitions = append(topic.Partitions, kgo.FetchPartition{
			Partition: key.partition,
			Records:   grouped[key],
		})
	}
	fetchTopics := make([]kgo.FetchTopic, 0, len(topicOrder))
	for _, topic := range topicOrder {
		fetchTopics = append(fetchTopics, *topics[topic])
	}
	return kgo.Fetches{{Topics: fetchTopics}}
}

func blockingKafkaConsumer() *fakeKafkaConsumer {
	started := make(chan struct{})
	closed := make(chan struct{})
	var startOnce sync.Once
	consumer := &fakeKafkaConsumer{closeSignal: closed, started: started}
	consumer.poll = func(ctx context.Context, _ int) kgo.Fetches {
		startOnce.Do(func() { close(started) })
		select {
		case <-ctx.Done():
			return kgo.NewErrFetch(ctx.Err())
		case <-closed:
			return kgo.NewErrFetch(kgo.ErrClientClosed)
		}
	}
	return consumer
}

func (c *fakeKafkaConsumer) pollStarted() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.started
}

func waitForSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for subscription to start")
	}
}

func waitForCondition(t *testing.T, condition func() bool, failureMessage string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(failureMessage)
}

func assertCommittedOffsets(t *testing.T, commits [][]*kgo.Record, want map[int32]int64) {
	t.Helper()
	if len(commits) != 1 {
		t.Fatalf("commit calls = %d, want 1", len(commits))
	}
	got := make(map[int32]int64, len(commits[0]))
	for _, record := range commits[0] {
		got[record.Partition] = record.Offset
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("committed offsets = %v, want %v", got, want)
	}
}

func TestMessageFromKafkaRecordUsesLastDuplicateHeader(t *testing.T) {
	record := &kgo.Record{Headers: []kgo.RecordHeader{
		{Key: "key", Value: []byte("first")},
		{Key: "key", Value: []byte("last")},
	}}
	message := messageFromKafkaRecord(record)
	if !reflect.DeepEqual(message.Headers, map[string][]byte{"key": []byte("last")}) {
		t.Fatalf("headers = %v, want last value", message.Headers)
	}
}
