package infra

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/project-kgo/kc/pkg/mq"
	"github.com/redis/go-redis/v9"
)

type fakeRedisStreamAdd struct {
	stream string
	values []interface{}
	maxLen int64
}

type fakeRedisStreamStore struct {
	mu sync.Mutex

	adds        []fakeRedisStreamAdd
	acks        [][]string
	ensureStart []string
	closeCalls  int

	closeFunc     func() error
	addFunc       func(context.Context, string, []interface{}, int64) (string, error)
	ensureFunc    func(context.Context, string, string, string) error
	readFunc      func(context.Context, string, string, string, int64, time.Duration) ([]redis.XMessage, error)
	autoClaimFunc func(context.Context, string, string, string, time.Duration, string, int64) ([]redis.XMessage, string, error)
	ackFunc       func(context.Context, string, string, ...string) (int64, error)
}

func (s *fakeRedisStreamStore) Close() error {
	s.mu.Lock()
	s.closeCalls++
	fn := s.closeFunc
	s.mu.Unlock()
	if fn != nil {
		return fn()
	}
	return nil
}

func (s *fakeRedisStreamStore) Add(
	ctx context.Context,
	stream string,
	values []interface{},
	maxLen int64,
) (string, error) {
	s.mu.Lock()
	s.adds = append(s.adds, fakeRedisStreamAdd{
		stream: stream, values: append([]interface{}(nil), values...), maxLen: maxLen,
	})
	fn := s.addFunc
	s.mu.Unlock()
	if fn != nil {
		return fn(ctx, stream, values, maxLen)
	}
	return "1-0", nil
}

func (s *fakeRedisStreamStore) EnsureGroup(ctx context.Context, stream, group, start string) error {
	s.mu.Lock()
	s.ensureStart = append(s.ensureStart, start)
	fn := s.ensureFunc
	s.mu.Unlock()
	if fn != nil {
		return fn(ctx, stream, group, start)
	}
	return nil
}

func (s *fakeRedisStreamStore) ReadGroup(
	ctx context.Context,
	stream string,
	group string,
	consumer string,
	count int64,
	block time.Duration,
) ([]redis.XMessage, error) {
	if s.readFunc != nil {
		return s.readFunc(ctx, stream, group, consumer, count, block)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *fakeRedisStreamStore) AutoClaim(
	ctx context.Context,
	stream string,
	group string,
	consumer string,
	minIdle time.Duration,
	start string,
	count int64,
) ([]redis.XMessage, string, error) {
	if s.autoClaimFunc != nil {
		return s.autoClaimFunc(ctx, stream, group, consumer, minIdle, start, count)
	}
	return nil, "0-0", nil
}

func (s *fakeRedisStreamStore) Ack(ctx context.Context, stream, group string, ids ...string) (int64, error) {
	s.mu.Lock()
	s.acks = append(s.acks, append([]string(nil), ids...))
	fn := s.ackFunc
	s.mu.Unlock()
	if fn != nil {
		return fn(ctx, stream, group, ids...)
	}
	return int64(len(ids)), nil
}

func (s *fakeRedisStreamStore) ackedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for _, call := range s.acks {
		ids = append(ids, call...)
	}
	return ids
}

func (s *fakeRedisStreamStore) capturedAdds() []fakeRedisStreamAdd {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]fakeRedisStreamAdd(nil), s.adds...)
}

func (s *fakeRedisStreamStore) capturedEnsureStarts() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ensureStart...)
}

func (s *fakeRedisStreamStore) capturedCloseCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCalls
}

func testRedisStreamRuntimeConfig() redisStreamRuntimeConfig {
	return redisStreamRuntimeConfig{
		keyPrefix:          "test:mq",
		batchSize:          64,
		queueDepth:         64,
		concurrency:        10,
		consumerID:         "test-consumer",
		groupStartID:       "0",
		readBlock:          5 * time.Millisecond,
		handlerTimeout:     100 * time.Millisecond,
		pendingIdleTimeout: 100 * time.Millisecond,
		redeliverInterval:  10 * time.Millisecond,
		retryBackoff:       time.Millisecond,
		retryMaxBackoff:    20 * time.Millisecond,
	}
}

func testRedisStreamRecord(t *testing.T, id string, message *Message) redis.XMessage {
	t.Helper()
	values, err := marshalRedisStreamMessage(message)
	if err != nil {
		t.Fatal(err)
	}
	valueMap := make(map[string]interface{}, len(values)/2)
	for index := 0; index < len(values); index += 2 {
		valueMap[values[index].(string)] = values[index+1]
	}
	return redis.XMessage{ID: id, Values: valueMap}
}

func TestRedisStreamMessageRoundTrip(t *testing.T) {
	timestamp := time.Unix(123, 456)
	original := &Message{
		Key:       []byte{0, 1, 2},
		Body:      []byte("payload"),
		Headers:   map[string][]byte{"trace": {3, 4, 5}},
		Timestamp: timestamp,
	}
	record := testRedisStreamRecord(t, "123000-7", original)

	decoded, err := messageFromRedisStreamRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ID != record.ID || !reflect.DeepEqual(decoded.Key, original.Key) ||
		!reflect.DeepEqual(decoded.Body, original.Body) || !reflect.DeepEqual(decoded.Headers, original.Headers) ||
		!decoded.Timestamp.Equal(timestamp) {
		t.Fatalf("decoded message = %#v", decoded)
	}

	decoded.Key[0] = 9
	if original.Key[0] == 9 {
		t.Fatal("decoded key aliases the published message")
	}
}

func TestRedisStreamPublishUsesSingleStream(t *testing.T) {
	store := &fakeRedisStreamStore{}
	config := testRedisStreamRuntimeConfig()
	config.maxLen = 123
	mq := newRedisStreamMQ(store, config)

	for _, key := range [][]byte{[]byte("first"), []byte("second"), nil} {
		if err := mq.Publish(context.Background(), "orders", &Message{Key: key, Body: []byte("body")}); err != nil {
			t.Fatal(err)
		}
	}
	adds := store.capturedAdds()
	if len(adds) != 3 {
		t.Fatalf("adds = %d, want 3", len(adds))
	}
	for _, add := range adds {
		if add.stream != mq.streamKey("orders") || add.maxLen != 123 {
			t.Fatalf("add = %#v", add)
		}
	}
	if err := mq.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRedisStreamBatchUsesBoundedConcurrency(t *testing.T) {
	config := testRedisStreamRuntimeConfig()
	const wantedConcurrency = 4
	const wantedBatchSize = int64(12)
	const messageCount = 24
	records := make([]redis.XMessage, 0, messageCount)
	for index := range messageCount {
		records = append(records, testRedisStreamRecord(t, redisStreamTestID(index+1), &Message{Body: []byte("body")}))
	}

	var delivered atomic.Bool
	store := &fakeRedisStreamStore{}
	store.readFunc = func(ctx context.Context, _ string, _ string, _ string, count int64, _ time.Duration) ([]redis.XMessage, error) {
		if count != wantedBatchSize {
			t.Errorf("read count = %d, want %d", count, wantedBatchSize)
		}
		if delivered.CompareAndSwap(false, true) {
			return records, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	var active atomic.Int32
	var maximum atomic.Int32
	started := make(chan struct{}, messageCount)
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	mq := newRedisStreamMQ(store, config)
	if err := mq.Subscribe(ctx, "orders", "billing", func(context.Context, *Message) error {
		current := active.Add(1)
		for {
			old := maximum.Load()
			if current <= old || maximum.CompareAndSwap(old, current) {
				break
			}
		}
		started <- struct{}{}
		<-release
		active.Add(-1)
		return nil
	}, WithConcurrency(wantedConcurrency), WithBatchSize(int(wantedBatchSize)), WithRedisStreamQueueDepth(8)); err != nil {
		t.Fatal(err)
	}

	for range wantedConcurrency {
		waitForSignal(t, started)
	}
	select {
	case <-started:
		t.Fatal("handler concurrency exceeded configured worker count")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	waitForCondition(t, func() bool { return len(store.ackedIDs()) == messageCount }, "batch was not fully acknowledged")
	cancel()
	if err := mq.Close(); err != nil {
		t.Fatal(err)
	}
	if maximum.Load() != int32(wantedConcurrency) {
		t.Fatalf("maximum concurrency = %d, want %d", maximum.Load(), wantedConcurrency)
	}
}

func TestRedisStreamSubscriptionOptionsOverrideDefaults(t *testing.T) {
	mq := newRedisStreamMQ(&fakeRedisStreamStore{}, testRedisStreamRuntimeConfig())
	config, err := mq.subscriptionConfig(
		WithBatchSize(32),
		WithConcurrency(3),
		WithHandlerTimeout(2*time.Second),
		WithRetryBackoff(25*time.Millisecond, 2*time.Second),
		WithRedisStreamQueueDepth(96),
		WithRedisStreamRedelivery(3*time.Second, 250*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	if config.batchSize != 32 || config.concurrency != 3 || config.queueDepth != 96 {
		t.Fatalf("subscription limits = batch %d, concurrency %d, queue %d", config.batchSize, config.concurrency, config.queueDepth)
	}
	if config.handlerTimeout != 2*time.Second || config.pendingIdleTimeout != 3*time.Second || config.redeliverInterval != 250*time.Millisecond {
		t.Fatalf("subscription timeouts = handler %v, pending %v, interval %v", config.handlerTimeout, config.pendingIdleTimeout, config.redeliverInterval)
	}
	if config.retryBackoff != 25*time.Millisecond || config.retryMaxBackoff != 2*time.Second {
		t.Fatalf("subscription retry = %v/%v", config.retryBackoff, config.retryMaxBackoff)
	}
}

func TestRedisStreamSubscriptionOptionsValidateCombinedTimeouts(t *testing.T) {
	mq := newRedisStreamMQ(&fakeRedisStreamStore{}, testRedisStreamRuntimeConfig())
	_, err := mq.subscriptionConfig(
		WithHandlerTimeout(2*time.Second),
		WithRedisStreamPendingIdleTimeout(time.Second),
	)
	if !errors.Is(err, ErrInvalidSubscribeOption) {
		t.Fatalf("subscriptionConfig() error = %v, want ErrInvalidSubscribeOption", err)
	}
}

func TestRedisStreamFailureModesDoNotAck(t *testing.T) {
	errHandler := errors.New("handler failed")
	tests := []struct {
		name    string
		record  func(*testing.T) redis.XMessage
		handler Handler
	}{
		{
			name:   "error",
			record: func(t *testing.T) redis.XMessage { return testRedisStreamRecord(t, "2-0", &Message{}) },
			handler: func(context.Context, *Message) error {
				return errHandler
			},
		},
		{
			name:   "panic",
			record: func(t *testing.T) redis.XMessage { return testRedisStreamRecord(t, "3-0", &Message{}) },
			handler: func(context.Context, *Message) error {
				panic("boom")
			},
		},
		{
			name:   "timeout swallowed",
			record: func(t *testing.T) redis.XMessage { return testRedisStreamRecord(t, "4-0", &Message{}) },
			handler: func(ctx context.Context, _ *Message) error {
				<-ctx.Done()
				return nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeRedisStreamStore{}
			config := testRedisStreamRuntimeConfig()
			config.handlerTimeout = 5 * time.Millisecond
			mq := newRedisStreamMQ(store, config)
			mq.processRedisStreamMessage(context.Background(), "stream", "group", test.handler, test.record(t), config)
			if ids := store.ackedIDs(); len(ids) != 0 {
				t.Fatalf("acked IDs = %v", ids)
			}
		})
	}
}

func TestRedisStreamDecodeFailureLogsAndAcknowledges(t *testing.T) {
	var logs bytes.Buffer
	store := &fakeRedisStreamStore{}
	config := testRedisStreamRuntimeConfig()
	config.logger = slog.New(slog.NewTextHandler(&logs, nil))
	mq := newRedisStreamMQ(store, config)

	mq.processRedisStreamMessage(
		context.Background(),
		"stream",
		"group",
		func(context.Context, *Message) error {
			t.Fatal("无法解码的消息不应调用 handler")
			return nil
		},
		redis.XMessage{ID: "1-0"},
		config,
	)

	if ids := store.ackedIDs(); !reflect.DeepEqual(ids, []string{"1-0"}) {
		t.Fatalf("acked IDs = %v, want [1-0]", ids)
	}
	if !bytes.Contains(logs.Bytes(), []byte("丢弃无法解码的 Redis Stream 消息")) ||
		!bytes.Contains(logs.Bytes(), []byte("message_id=1-0")) {
		t.Fatalf("decode failure log = %q", logs.String())
	}
}

func TestRedisStreamDecodeFailureAckErrorIsLogged(t *testing.T) {
	var logs bytes.Buffer
	errAck := errors.New("ack unavailable")
	store := &fakeRedisStreamStore{}
	store.ackFunc = func(context.Context, string, string, ...string) (int64, error) {
		return 0, errAck
	}
	config := testRedisStreamRuntimeConfig()
	config.logger = slog.New(slog.NewTextHandler(&logs, nil))
	mq := newRedisStreamMQ(store, config)

	mq.processRedisStreamMessage(
		context.Background(), "stream", "group",
		func(context.Context, *Message) error { return nil },
		redis.XMessage{ID: "1-0"}, config,
	)

	if ids := store.ackedIDs(); !reflect.DeepEqual(ids, []string{"1-0"}) {
		t.Fatalf("ack attempts = %v, want [1-0]", ids)
	}
	if !bytes.Contains(logs.Bytes(), []byte("确认无法解码的 Redis Stream 消息失败")) ||
		!bytes.Contains(logs.Bytes(), []byte(errAck.Error())) {
		t.Fatalf("ack failure log = %q", logs.String())
	}
}

func TestRedisStreamSuccessfulMessageAckUsesDetachedContext(t *testing.T) {
	type contextKey string
	const key contextKey = "trace"
	parent, cancelParent := context.WithCancel(context.WithValue(context.Background(), key, "value"))
	store := &fakeRedisStreamStore{}
	store.ackFunc = func(ctx context.Context, _ string, _ string, _ ...string) (int64, error) {
		if err := ctx.Err(); err != nil {
			t.Errorf("ack context is canceled: %v", err)
		}
		if got := ctx.Value(key); got != "value" {
			t.Errorf("ack context value = %v", got)
		}
		return 1, nil
	}
	mq := newRedisStreamMQ(store, testRedisStreamRuntimeConfig())
	record := testRedisStreamRecord(t, "1-0", &Message{})
	mq.processRedisStreamMessage(parent, "stream", "group", func(context.Context, *Message) error {
		return nil
	}, record, mq.config)
	cancelParent()

	if ids := store.ackedIDs(); !reflect.DeepEqual(ids, []string{"1-0"}) {
		t.Fatalf("acked IDs = %v", ids)
	}
}

func TestRedisStreamAckFailureLeavesMessageForPendingRecovery(t *testing.T) {
	errAck := errors.New("ack unavailable")
	store := &fakeRedisStreamStore{}
	store.ackFunc = func(context.Context, string, string, ...string) (int64, error) {
		return 0, errAck
	}
	mq := newRedisStreamMQ(store, testRedisStreamRuntimeConfig())
	record := testRedisStreamRecord(t, "1-0", &Message{})

	mq.processRedisStreamMessage(context.Background(), "stream", "group", func(context.Context, *Message) error {
		return nil
	}, record, mq.config)
	if ids := store.ackedIDs(); !reflect.DeepEqual(ids, []string{"1-0"}) {
		t.Fatalf("ack attempts = %v", ids)
	}
	// 没有第二次本地 handler 调用；Redis PEL 仍是唯一真相，后续由 reclaim 重新投递。
}

func TestRedisStreamSubscriptionCancelDoesNotAck(t *testing.T) {
	record := testRedisStreamRecord(t, "1-0", &Message{})
	var delivered atomic.Bool
	store := &fakeRedisStreamStore{}
	store.readFunc = func(ctx context.Context, _ string, _ string, _ string, _ int64, _ time.Duration) ([]redis.XMessage, error) {
		if delivered.CompareAndSwap(false, true) {
			return []redis.XMessage{record}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	handlerStarted := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	mq := newRedisStreamMQ(store, testRedisStreamRuntimeConfig())
	if err := mq.Subscribe(ctx, "orders", "billing", func(ctx context.Context, _ *Message) error {
		close(handlerStarted)
		<-ctx.Done()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, handlerStarted)
	cancel()
	waitForCondition(t, func() bool {
		mq.mu.Lock()
		defer mq.mu.Unlock()
		return len(mq.subscriptions) == 0
	}, "subscription did not stop after context cancellation")
	if ids := store.ackedIDs(); len(ids) != 0 {
		t.Fatalf("acked IDs = %v", ids)
	}
	if err := mq.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRedisStreamCloseDrainsStartedHandlerButLeavesQueuedPending(t *testing.T) {
	records := []redis.XMessage{
		testRedisStreamRecord(t, "1-0", &Message{}),
		testRedisStreamRecord(t, "2-0", &Message{}),
	}
	var delivered atomic.Bool
	ackContextOK := atomic.Bool{}
	store := &fakeRedisStreamStore{}
	store.readFunc = func(ctx context.Context, _ string, _ string, _ string, _ int64, _ time.Duration) ([]redis.XMessage, error) {
		if delivered.CompareAndSwap(false, true) {
			return records, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	store.ackFunc = func(ctx context.Context, _ string, _ string, _ ...string) (int64, error) {
		ackContextOK.Store(ctx.Err() == nil)
		return 1, nil
	}

	config := testRedisStreamRuntimeConfig()
	config.concurrency = 1
	started := make(chan struct{})
	release := make(chan struct{})
	var handlerCalls atomic.Int32
	mq := newRedisStreamMQ(store, config)
	if err := mq.Subscribe(context.Background(), "orders", "billing", func(context.Context, *Message) error {
		handlerCalls.Add(1)
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, started)
	waitForCondition(t, func() bool { return redisStreamQueuedMessages(mq) == 1 }, "second message was not queued")

	closed := make(chan error, 1)
	go func() { closed <- mq.Close() }()
	waitForCondition(t, mq.isClosed, "Close did not transition to closing")
	close(release)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if handlerCalls.Load() != 1 {
		t.Fatalf("handler calls = %d, want 1", handlerCalls.Load())
	}
	if ids := store.ackedIDs(); !reflect.DeepEqual(ids, []string{"1-0"}) {
		t.Fatalf("acked IDs = %v", ids)
	}
	if !ackContextOK.Load() {
		t.Fatal("ack did not use a live detached context during Close")
	}
}

func TestRedisStreamCloseWaitsForPublishAndIsConcurrentSafe(t *testing.T) {
	addStarted := make(chan struct{})
	releaseAdd := make(chan struct{})
	closeErr := errors.New("close failed")
	store := &fakeRedisStreamStore{}
	store.addFunc = func(context.Context, string, []interface{}, int64) (string, error) {
		close(addStarted)
		<-releaseAdd
		return "1-0", nil
	}
	store.closeFunc = func() error { return closeErr }
	mq := newRedisStreamMQ(store, testRedisStreamRuntimeConfig())

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- mq.Publish(context.Background(), "orders", &Message{})
	}()
	waitForSignal(t, addStarted)

	const callers = 5
	closeResults := make(chan error, callers)
	for range callers {
		go func() { closeResults <- mq.Close() }()
	}
	time.Sleep(20 * time.Millisecond)
	if calls := store.capturedCloseCalls(); calls != 0 {
		t.Fatalf("store Close calls before Publish completed = %d", calls)
	}
	close(releaseAdd)
	if err := <-publishDone; err != nil {
		t.Fatal(err)
	}
	for range callers {
		if err := <-closeResults; !errors.Is(err, closeErr) {
			t.Fatalf("Close() error = %v, want %v", err, closeErr)
		}
	}
	if calls := store.capturedCloseCalls(); calls != 1 {
		t.Fatalf("store Close calls = %d, want 1", calls)
	}
	if err := mq.Publish(context.Background(), "orders", &Message{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish after Close error = %v", err)
	}
	if err := mq.Subscribe(context.Background(), "orders", "billing", func(context.Context, *Message) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("Subscribe after Close error = %v", err)
	}
}

func TestRedisStreamCloseWaitsForSubscribeInitialization(t *testing.T) {
	ensureStarted := make(chan struct{})
	releaseEnsure := make(chan struct{})
	store := &fakeRedisStreamStore{}
	store.ensureFunc = func(context.Context, string, string, string) error {
		close(ensureStarted)
		<-releaseEnsure
		return nil
	}
	mq := newRedisStreamMQ(store, testRedisStreamRuntimeConfig())

	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- mq.Subscribe(context.Background(), "orders", "billing", func(context.Context, *Message) error {
			return nil
		})
	}()
	waitForSignal(t, ensureStarted)
	closeDone := make(chan error, 1)
	go func() { closeDone <- mq.Close() }()
	time.Sleep(20 * time.Millisecond)
	if calls := store.capturedCloseCalls(); calls != 0 {
		t.Fatalf("store Close calls during Subscribe initialization = %d", calls)
	}

	close(releaseEnsure)
	if err := <-subscribeDone; !errors.Is(err, ErrClosed) {
		t.Fatalf("Subscribe() error = %v, want ErrClosed", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if calls := store.capturedCloseCalls(); calls != 1 {
		t.Fatalf("store Close calls = %d, want 1", calls)
	}
}

func TestRedisStreamPendingRecoveryUsesAutoClaimCursor(t *testing.T) {
	recordOne := testRedisStreamRecord(t, "1-0", &Message{Body: []byte("one")})
	recordThree := testRedisStreamRecord(t, "3-0", &Message{Body: []byte("three")})
	store := &fakeRedisStreamStore{}
	var starts []string
	store.autoClaimFunc = func(
		_ context.Context,
		stream string,
		group string,
		consumer string,
		minIdle time.Duration,
		start string,
		count int64,
	) ([]redis.XMessage, string, error) {
		starts = append(starts, start)
		if stream != "stream" || group != "group" || consumer != "consumer" {
			t.Errorf("AutoClaim identifiers = %q/%q/%q", stream, group, consumer)
		}
		if minIdle != 100*time.Millisecond || count != 2 {
			t.Errorf("AutoClaim limits = %v/%d", minIdle, count)
		}
		switch start {
		case "0-0":
			return []redis.XMessage{recordOne}, "2-0", nil
		case "2-0":
			// XAUTOCLAIM 可能只扫描到尚未超时的条目，此时消息为空但游标仍需继续。
			return nil, "4-0", nil
		case "4-0":
			return []redis.XMessage{recordThree}, "0-0", nil
		default:
			return nil, "0-0", nil
		}
	}

	config := testRedisStreamRuntimeConfig()
	config.batchSize = 2
	mq := newRedisStreamMQ(store, config)
	subscription := &redisStreamSubscription{queue: make(chan redis.XMessage, 4), inFlight: make(map[string]struct{})}
	if err := mq.reclaimRedisStreamPending(context.Background(), "stream", "group", "consumer", config, subscription); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(starts, []string{"0-0", "2-0", "4-0"}) {
		t.Fatalf("XAUTOCLAIM starts = %v", starts)
	}
	if len(subscription.queue) != 2 {
		t.Fatalf("queued messages = %d, want 2", len(subscription.queue))
	}
	if ids := store.ackedIDs(); len(ids) != 0 {
		t.Fatalf("reclaim acknowledgements = %v, want none", ids)
	}
}

func TestRedisStreamPendingRecoveryRejectsInvalidCursor(t *testing.T) {
	store := &fakeRedisStreamStore{}
	store.autoClaimFunc = func(
		context.Context, string, string, string, time.Duration, string, int64,
	) ([]redis.XMessage, string, error) {
		return nil, "", nil
	}

	mq := newRedisStreamMQ(store, testRedisStreamRuntimeConfig())
	subscription := &redisStreamSubscription{queue: make(chan redis.XMessage, 1), inFlight: make(map[string]struct{})}
	if err := mq.reclaimRedisStreamPending(context.Background(), "stream", "group", "consumer", mq.config, subscription); err == nil {
		t.Fatal("reclaimRedisStreamPending() error = nil")
	}
}

func TestRedisStreamInflightPreventsDuplicateQueueing(t *testing.T) {
	record := testRedisStreamRecord(t, "1-0", &Message{})
	store := &fakeRedisStreamStore{}
	store.autoClaimFunc = func(
		context.Context, string, string, string, time.Duration, string, int64,
	) ([]redis.XMessage, string, error) {
		return []redis.XMessage{record}, "0-0", nil
	}

	mq := newRedisStreamMQ(store, testRedisStreamRuntimeConfig())
	subscription := &redisStreamSubscription{queue: make(chan redis.XMessage, 2), inFlight: make(map[string]struct{})}
	if !subscription.enqueue(context.Background(), record) {
		t.Fatal("initial enqueue failed")
	}
	if err := mq.reclaimRedisStreamPending(context.Background(), "stream", "group", "consumer", mq.config, subscription); err != nil {
		t.Fatal(err)
	}
	if len(subscription.queue) != 1 {
		t.Fatalf("queued messages = %d, want 1", len(subscription.queue))
	}
}

func TestRedisStreamFailedMessageIsRedeliveredFromPending(t *testing.T) {
	record := testRedisStreamRecord(t, "1-0", &Message{})
	var delivered atomic.Bool
	var failed atomic.Bool
	var claimed atomic.Bool
	store := &fakeRedisStreamStore{}
	store.readFunc = func(ctx context.Context, _ string, _ string, _ string, _ int64, _ time.Duration) ([]redis.XMessage, error) {
		if delivered.CompareAndSwap(false, true) {
			return []redis.XMessage{record}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	store.autoClaimFunc = func(
		context.Context, string, string, string, time.Duration, string, int64,
	) ([]redis.XMessage, string, error) {
		if failed.Load() && claimed.CompareAndSwap(false, true) {
			return []redis.XMessage{record}, "0-0", nil
		}
		return nil, "0-0", nil
	}

	config := testRedisStreamRuntimeConfig()
	config.redeliverInterval = 20 * time.Millisecond
	var attempts atomic.Int32
	acked := make(chan struct{})
	store.ackFunc = func(context.Context, string, string, ...string) (int64, error) {
		close(acked)
		return 1, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	mq := newRedisStreamMQ(store, config)
	if err := mq.Subscribe(ctx, "orders", "billing", func(context.Context, *Message) error {
		if attempts.Add(1) == 1 {
			failed.Store(true)
			return errors.New("retry")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, acked)
	if attempts.Load() != 2 {
		t.Fatalf("handler attempts = %d, want 2", attempts.Load())
	}
	cancel()
	if err := mq.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRedisStreamPollRecreatesMissingGroupAtZero(t *testing.T) {
	record := testRedisStreamRecord(t, "1-0", &Message{})
	var reads atomic.Int32
	store := &fakeRedisStreamStore{}
	store.readFunc = func(ctx context.Context, _ string, _ string, _ string, _ int64, _ time.Duration) ([]redis.XMessage, error) {
		switch reads.Add(1) {
		case 1:
			return nil, errors.New("NOGROUP consumer group is missing")
		case 2:
			return []redis.XMessage{record}, nil
		default:
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	acked := make(chan struct{})
	store.ackFunc = func(context.Context, string, string, ...string) (int64, error) {
		close(acked)
		return 1, nil
	}

	config := testRedisStreamRuntimeConfig()
	config.groupStartID = "$"
	ctx, cancel := context.WithCancel(context.Background())
	mq := newRedisStreamMQ(store, config)
	if err := mq.Subscribe(ctx, "orders", "billing", func(context.Context, *Message) error { return nil }); err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, acked)
	cancel()
	if err := mq.Close(); err != nil {
		t.Fatal(err)
	}
	starts := store.capturedEnsureStarts()
	if len(starts) < 2 || starts[0] != "$" || starts[1] != "0" {
		t.Fatalf("group creation starts = %v", starts)
	}
}

func TestRedisStreamCancellationStopsErrorBackoff(t *testing.T) {
	readStarted := make(chan struct{}, 1)
	store := &fakeRedisStreamStore{}
	store.readFunc = func(context.Context, string, string, string, int64, time.Duration) ([]redis.XMessage, error) {
		select {
		case readStarted <- struct{}{}:
		default:
		}
		return nil, errors.New("redis unavailable")
	}
	config := testRedisStreamRuntimeConfig()
	config.retryBackoff = time.Second
	config.retryMaxBackoff = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	mq := newRedisStreamMQ(store, config)
	if err := mq.Subscribe(ctx, "orders", "billing", func(context.Context, *Message) error { return nil }); err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, readStarted)
	cancel()
	started := time.Now()
	if err := mq.Close(); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 200*time.Millisecond {
		t.Fatal("context cancellation did not interrupt retry backoff")
	}
}

func TestRedisStreamRuntimeDefaultsAndValidation(t *testing.T) {
	config := redisStreamRuntimeConfigFrom(&RedisStreamConfig{})
	if config.keyPrefix != "kc:mq" || config.batchSize != 64 || config.queueDepth != 64 || config.concurrency != 10 || config.maxLen != 0 {
		t.Fatalf("runtime defaults = %#v", config)
	}
	if config.handlerTimeout != 30*time.Second || config.pendingIdleTimeout != time.Minute ||
		config.redeliverInterval != 15*time.Second {
		t.Fatalf("runtime timeout defaults = %#v", config)
	}

	valid := MQConfig{
		Type: MQTypeRedisStream, DSN: "redis://localhost:6379",
		RedisStream: &RedisStreamConfig{},
	}
	if err := validateMQConfig(valid); err != nil {
		t.Fatalf("valid redis stream config: %v", err)
	}

	invalid := []MQConfig{
		{Type: MQTypeRedisStream, DSN: "redis://localhost:6379"},
		{Type: MQTypeRedisStream, DSN: "redis://localhost:6379", RedisStream: &RedisStreamConfig{ConsumerBatchSize: -1}},
		{Type: MQTypeRedisStream, DSN: "redis://localhost:6379", RedisStream: &RedisStreamConfig{QueueDepth: -1}},
		{Type: MQTypeRedisStream, DSN: "redis://localhost:6379", RedisStream: &RedisStreamConfig{Concurrency: -1}},
		{Type: MQTypeRedisStream, DSN: "redis://localhost:6379", RedisStream: &RedisStreamConfig{GroupStartID: "invalid"}},
		{Type: MQTypeRedisStream, DSN: "redis://localhost:6379", RedisStream: &RedisStreamConfig{HandlerTimeout: time.Minute, PendingIdleTimeout: time.Second}},
		{Type: MQTypeRedisStream, DSN: "redis://localhost:6379", RedisStream: &RedisStreamConfig{RedeliverInterval: -time.Second}},
		{Type: MQTypeRedisStream, DSN: "redis://localhost:6379", RedisStream: &RedisStreamConfig{MaxLen: -1}},
		{Type: MQTypeRedisStreamCluster, DSN: "redis://localhost:6379/1", RedisStream: &RedisStreamConfig{}},
	}
	for index, item := range invalid {
		if err := validateMQConfig(item); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("invalid config %d error = %v", index, err)
		}
	}
}

func TestRedisStreamPublicArgumentValidation(t *testing.T) {
	mq := newRedisStreamMQ(&fakeRedisStreamStore{}, testRedisStreamRuntimeConfig())
	if err := mq.Publish(nil, "topic", &Message{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Publish(nil context) error = %v", err)
	}
	if err := mq.Publish(context.Background(), "", &Message{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Publish(empty topic) error = %v", err)
	}
	if err := mq.Subscribe(context.Background(), "topic", "", func(context.Context, *Message) error { return nil }); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Subscribe(empty group) error = %v", err)
	}
	if err := mq.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRedisStreamConsumerIDsAreUnique(t *testing.T) {
	mq := newRedisStreamMQ(&fakeRedisStreamStore{}, testRedisStreamRuntimeConfig())
	first := mq.subscriptionConsumerID()
	second := mq.subscriptionConsumerID()
	if first == second || len(first) <= len("test-consumer:") {
		t.Fatalf("consumer IDs = %q, %q", first, second)
	}
}

func redisStreamQueuedMessages(mq *redisStreamMQ) int {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	for subscription := range mq.subscriptions {
		return len(subscription.queue)
	}
	return 0
}

func redisStreamTestID(value int) string {
	return strconv.Itoa(value) + "-0"
}

func waitForSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("等待 Redis Stream 测试信号超时")
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
