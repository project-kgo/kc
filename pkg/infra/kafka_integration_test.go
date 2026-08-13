package infra

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	coremq "github.com/project-kgo/kc/pkg/mq"
	"github.com/project-kgo/kc/pkg/resource"
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
	if err := Init(ctx, Config{MQ: map[string]MQConfig{name: {Type: typ, Kafka: kafkaConfig}}}); err != nil {
		t.Fatal(err)
	}
	mq, ok := resource.Get[coremq.MQ](name)
	if !ok {
		t.Fatal("Kafka MQ resource is missing")
	}
	t.Cleanup(func() { _ = mq.Close() })

	dlqBody := make(chan []byte, 1)
	if err := mq.Subscribe(ctx, topic+kafkaDLQSuffix, dlqGroup, func(_ context.Context, message *coremq.Message) error {
		dlqBody <- append([]byte(nil), message.Body...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	if err := mq.Subscribe(ctx, topic, sourceGroup, func(context.Context, *coremq.Message) error {
		attempts.Add(1)
		return fmt.Errorf("integration poison")
	}); err != nil {
		t.Fatal(err)
	}
	if typ == MQTypeKafkaShare {
		// 新 share group 默认从 latest 开始，先让订阅完成首轮心跳和 fetch。
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			t.Fatal(ctx.Err())
		case <-timer.C:
		}
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
		t.Fatal(ctx.Err())
	}
	if got := attempts.Load(); got < 2 {
		t.Fatalf("handler attempts = %d, want at least 2", got)
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
