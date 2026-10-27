package infra

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coremq "github.com/project-kgo/kc/pkg/mq"
	"github.com/project-kgo/kc/pkg/resource"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestKafkaIntegration 仅在显式提供 Kafka 测试集群时运行。
// KC_KAFKA_TEST_BROKERS 用于传统组；KC_KAFKA_SHARE_TEST_BROKERS 必须指向已启用 share group 的 Kafka 4.2+ 集群。
func TestKafkaIntegration(t *testing.T) {
	tests := []struct {
		name string
		env  string
		typ  MQType
	}{
		{name: "consumer-group", env: "KC_KAFKA_TEST_BROKERS", typ: MQTypeKafka},
		{name: "share-group", env: "KC_KAFKA_SHARE_TEST_BROKERS", typ: MQTypeKafkaShare},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			brokers := splitKafkaBrokers(os.Getenv(test.env))
			if len(brokers) == 0 {
				t.Skipf("%s is not set", test.env)
			}
			testKafkaDLQIntegration(t, brokers, test.typ)
		})
	}
}

func testKafkaDLQIntegration(t *testing.T, brokers []string, typ MQType) {
	t.Helper()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	name := "infra-kafka-integration-" + suffix
	topic := "kc-integration-" + suffix
	sourceGroup := "kc-source-" + suffix
	dlqGroup := "kc-dlq-" + suffix
	kafkaConfig := &KafkaConfig{
		Brokers: brokers, ConsumerBatchSize: 8, Concurrency: 2,
		HandlerTimeout: 5 * time.Second, RetryBackoff: 10 * time.Millisecond, RetryMaxBackoff: 5 * time.Second,
	}
	if typ == MQTypeKafka {
		kafkaConfig.MaxRetries = 1
	} else {
		kafkaConfig.MaxDeliveryAttempts = 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// 订阅要求 "<topic>.dlq" 已存在；DLQ 本身也被订阅，因此还需要它的 DLQ。
	createKafkaTestTopics(ctx, t, brokers, 1, topic, topic+kafkaDLQSuffix, topic+kafkaDLQSuffix+kafkaDLQSuffix)
	if err := Init(ctx, Config{MQ: map[string]MQConfig{name: {Type: typ, Kafka: kafkaConfig}}}); err != nil {
		t.Fatal(err)
	}
	mq, ok := resource.Get[coremq.MQ](name)
	if !ok {
		t.Fatal("Kafka MQ resource is missing")
	}
	t.Cleanup(func() { _ = mq.Close() })

	noDLQTopic := "kc-integration-no-dlq-" + suffix
	createKafkaTestTopics(ctx, t, brokers, 1, noDLQTopic)
	if err := mq.Subscribe(ctx, noDLQTopic, sourceGroup, func(context.Context, *coremq.Message) error { return nil }); !errors.Is(err, coremq.ErrInvalidArgument) {
		t.Fatalf("subscribe without DLQ topic error = %v, want ErrInvalidArgument", err)
	}

	// 传统组在 Subscribe 返回后才异步加入消费组，新 topic 从最早位置消费，避免分区分配前发布的消息被跳过。
	var options []coremq.SubscribeOption
	if typ == MQTypeKafka {
		options = append(options, coremq.WithKafkaStartOffset(coremq.KafkaStartOffsetEarliest))
	} else {
		// share group 的起始位置只能通过 group 配置设置，默认 latest。
		setKafkaShareGroupsEarliest(ctx, t, brokers, sourceGroup, dlqGroup)
	}
	dlqBody := make(chan []byte, 1)
	if err := mq.Subscribe(ctx, topic+kafkaDLQSuffix, dlqGroup, func(_ context.Context, message *coremq.Message) error {
		dlqBody <- append([]byte(nil), message.Body...)
		return nil
	}, options...); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	if err := mq.Subscribe(ctx, topic, sourceGroup, func(context.Context, *coremq.Message) error {
		attempts.Add(1)
		return fmt.Errorf("integration poison")
	}, options...); err != nil {
		t.Fatal(err)
	}
	if err := mq.Publish(ctx, topic, &coremq.Message{Key: []byte("key"), Body: []byte("payload")}); err != nil {
		t.Fatal(err)
	}

	select {
	case body := <-dlqBody:
		if string(body) != "payload" {
			t.Fatalf("DLQ body = %q", body)
		}
	case <-ctx.Done():
		t.Fatalf("DLQ not received, handler attempts = %d: %v", attempts.Load(), ctx.Err())
	}
	if got := attempts.Load(); got < 2 {
		t.Fatalf("handler attempts = %d, want at least 2", got)
	}
}

// TestKafkaConsumerGroupRebalanceIntegration 在消费途中加入第二个成员触发重平衡，
// 校验分区交接后消息不丢失，并且关闭时已处理的位置全部提交。
func TestKafkaConsumerGroupRebalanceIntegration(t *testing.T) {
	brokers := splitKafkaBrokers(os.Getenv("KC_KAFKA_TEST_BROKERS"))
	if len(brokers) == 0 {
		t.Skip("KC_KAFKA_TEST_BROKERS is not set")
	}
	const total = 400
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	topic := "kc-rebalance-" + suffix
	group := "kc-rebalance-" + suffix
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	createKafkaTestTopics(ctx, t, brokers, 4, topic, topic+kafkaDLQSuffix)
	kafkaConfig := &KafkaConfig{
		Brokers: brokers, ConsumerBatchSize: 16, Concurrency: 8, HandlerTimeout: 5 * time.Second,
		RebalanceTimeout: 20 * time.Second, RetryBackoff: 10 * time.Millisecond, RetryMaxBackoff: time.Second,
	}
	newMQ := func(name string) coremq.MQ {
		t.Helper()
		if err := Init(ctx, Config{MQ: map[string]MQConfig{name: {Type: MQTypeKafka, Kafka: kafkaConfig}}}); err != nil {
			t.Fatal(err)
		}
		mq, ok := resource.Get[coremq.MQ](name)
		if !ok {
			t.Fatal("Kafka MQ resource is missing")
		}
		t.Cleanup(func() { _ = mq.Close() })
		return mq
	}
	earliest := coremq.WithKafkaStartOffset(coremq.KafkaStartOffsetEarliest)

	var mu sync.Mutex
	seen := make(map[string]int)
	var memberHandled [2]atomic.Int32
	handler := func(member int) coremq.Handler {
		return func(_ context.Context, message *coremq.Message) error {
			time.Sleep(50 * time.Millisecond)
			mu.Lock()
			seen[string(message.Body)]++
			mu.Unlock()
			memberHandled[member].Add(1)
			return nil
		}
	}
	progress := func() (unique, handled int) {
		mu.Lock()
		defer mu.Unlock()
		for _, count := range seen {
			handled += count
		}
		return len(seen), handled
	}
	waitFor := func(description string, condition func() bool) {
		t.Helper()
		for !condition() {
			select {
			case <-ctx.Done():
				unique, handled := progress()
				t.Fatalf("%s: unique/handled = %d/%d: %v", description, unique, handled, ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
	}

	mq := newMQ("infra-kafka-rebalance-" + suffix)
	started := time.Now()
	if err := mq.Subscribe(ctx, topic, group, handler(0), earliest); err != nil {
		t.Fatal(err)
	}
	// 并发发布让 producer 合并成批，避免逐条往返拖慢测试。
	var publishers sync.WaitGroup
	publishErrs := make(chan error, total)
	for index := range total {
		publishers.Add(1)
		go func() {
			defer publishers.Done()
			message := &coremq.Message{Key: []byte(fmt.Sprintf("key-%d", index%32)), Body: []byte(fmt.Sprintf("message-%d", index))}
			if err := mq.Publish(ctx, topic, message); err != nil {
				publishErrs <- err
			}
		}()
	}
	publishers.Wait()
	close(publishErrs)
	for err := range publishErrs {
		t.Fatal(err)
	}
	t.Logf("published %d messages at %s", total, time.Since(started))
	waitFor("first member did not start consuming", func() bool { _, handled := progress(); return handled >= total/4 })
	// 同一订阅客户端再加入一个成员，触发 cooperative 重平衡把一部分分区交接过去。
	t.Logf("second member joins at %s", time.Since(started))
	if err := mq.Subscribe(ctx, topic, group, handler(1), earliest); err != nil {
		t.Fatal(err)
	}
	waitFor("messages lost across rebalance", func() bool { unique, _ := progress(); return unique == total })
	unique, handled := progress()
	t.Logf("all consumed at %s, unique/handled = %d/%d, per member = %d/%d", time.Since(started),
		unique, handled, memberHandled[0].Load(), memberHandled[1].Load())
	if memberHandled[1].Load() == 0 {
		t.Fatal("second member never received partitions; rebalance was not exercised")
	}
	closeStarted := time.Now()
	if err := mq.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("close took %s", time.Since(closeStarted))

	// 关闭时已提交全部位置：新成员只应收到关闭后发布的哨兵消息。
	verifier := newMQ("infra-kafka-rebalance-verify-" + suffix)
	sentinel := make(chan struct{})
	var replayed atomic.Int32
	if err := verifier.Subscribe(ctx, topic, group, func(_ context.Context, message *coremq.Message) error {
		if string(message.Body) == "sentinel" {
			close(sentinel)
		} else {
			replayed.Add(1)
		}
		return nil
	}, earliest); err != nil {
		t.Fatal(err)
	}
	if err := verifier.Publish(ctx, topic, &coremq.Message{Key: []byte("sentinel"), Body: []byte("sentinel")}); err != nil {
		t.Fatal(err)
	}
	verifyStarted := time.Now()
	select {
	case <-sentinel:
	case <-ctx.Done():
		t.Fatalf("sentinel not received: %v", ctx.Err())
	}
	t.Logf("verifier received sentinel after %s", time.Since(verifyStarted))
	time.Sleep(2 * time.Second)
	if got := replayed.Load(); got != 0 {
		t.Fatalf("replayed %d already processed messages after close", got)
	}
}

func createKafkaTestTopics(ctx context.Context, t *testing.T, brokers []string, partitions int32, topics ...string) {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	request := kmsg.NewPtrCreateTopicsRequest()
	request.TimeoutMillis = 10000
	for _, topic := range topics {
		requestTopic := kmsg.NewCreateTopicsRequestTopic()
		requestTopic.Topic = topic
		requestTopic.NumPartitions = partitions
		requestTopic.ReplicationFactor = -1
		request.Topics = append(request.Topics, requestTopic)
	}
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	for _, responseTopic := range response.Topics {
		if err := kerr.ErrorForCode(responseTopic.ErrorCode); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			t.Fatalf("create topic %s: %v", responseTopic.Topic, err)
		}
	}
}

func setKafkaShareGroupsEarliest(ctx context.Context, t *testing.T, brokers []string, groups ...string) {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	request := kmsg.NewPtrIncrementalAlterConfigsRequest()
	for _, group := range groups {
		groupResource := kmsg.NewIncrementalAlterConfigsRequestResource()
		groupResource.ResourceType = kmsg.ConfigResourceTypeGroupConfig
		groupResource.ResourceName = group
		config := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		config.Name = "share.auto.offset.reset"
		config.Value = kmsg.StringPtr("earliest")
		groupResource.Configs = append(groupResource.Configs, config)
		request.Resources = append(request.Resources, groupResource)
	}
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	for _, groupResource := range response.Resources {
		if err := kerr.ErrorForCode(groupResource.ErrorCode); err != nil {
			t.Fatalf("set share group %s offset reset: %v", groupResource.ResourceName, err)
		}
	}
}

func splitKafkaBrokers(value string) []string {
	var brokers []string
	for _, broker := range strings.Split(value, ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}
