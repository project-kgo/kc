package infra

import (
	"errors"
	"testing"
	"time"
)

func TestValidateConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name   string
		config Config
		want   error
	}{
		{
			name:   "empty data name",
			config: Config{Data: map[string]DataConfig{" ": {Type: DataTypeMySQL, DSN: "user:pass@tcp(localhost:3306)/db"}}},
			want:   ErrInvalidConfig,
		},
		{
			name:   "unsupported data type",
			config: Config{Data: map[string]DataConfig{"main": {Type: DataType("unknown"), DSN: "value"}}},
			want:   ErrUnsupportedType,
		},
		{
			name:   "unsupported mq type",
			config: Config{MQ: map[string]MQConfig{"events": {Type: MQType("unknown")}}},
			want:   ErrUnsupportedType,
		},
		{
			name:   "negative data check timeout",
			config: Config{Data: map[string]DataConfig{"main": {Type: DataTypeMySQL, DSN: "user:pass@tcp(localhost:3306)/db", CheckTimeout: -time.Second}}},
			want:   ErrInvalidConfig,
		},
		{
			name:   "negative mq check timeout",
			config: Config{MQ: map[string]MQConfig{"events": {Type: MQTypeKafka, CheckTimeout: -time.Second, Kafka: &KafkaConfig{Brokers: []string{"localhost:9092"}}}}},
			want:   ErrInvalidConfig,
		},
		{
			name:   "missing sql dsn",
			config: Config{Data: map[string]DataConfig{"main": {Type: DataTypeMySQL}}},
			want:   ErrInvalidConfig,
		},
		{
			name: "negative sql pool",
			config: Config{Data: map[string]DataConfig{"main": {
				Type: DataTypePostgreSQL, DSN: "postgres://localhost/db", SQL: &SQLConfig{MaxOpenConns: -1},
			}}},
			want: ErrInvalidConfig,
		},
		{
			name: "standalone redis multiple addresses",
			config: Config{Data: map[string]DataConfig{"main": {
				Type: DataTypeRedis, Redis: &RedisConfig{Addrs: []string{"localhost:6379", "localhost:6380"}},
			}}},
			want: ErrInvalidConfig,
		},
		{
			name: "redis cluster db",
			config: Config{Data: map[string]DataConfig{"main": {
				Type: DataTypeRedisCluster, Redis: &RedisConfig{Addrs: []string{"localhost:6379"}, DB: 1},
			}}},
			want: ErrInvalidConfig,
		},
		{
			name:   "redis cluster dsn db",
			config: Config{Data: map[string]DataConfig{"main": {Type: DataTypeRedisCluster, DSN: "redis://localhost:6379/1"}}},
			want:   ErrInvalidConfig,
		},
		{
			name:   "missing elasticsearch endpoint",
			config: Config{Data: map[string]DataConfig{"main": {Type: DataTypeElasticsearch, Elasticsearch: &ElasticsearchConfig{}}}},
			want:   ErrInvalidConfig,
		},
		{
			name: "elasticsearch auth conflict",
			config: Config{Data: map[string]DataConfig{"main": {
				Type: DataTypeElasticsearch, DSN: "http://localhost:9200", Elasticsearch: &ElasticsearchConfig{Username: "user", APIKey: "key"},
			}}},
			want: ErrInvalidConfig,
		},
		{
			name: "elasticsearch password without username",
			config: Config{Data: map[string]DataConfig{"main": {
				Type: DataTypeElasticsearch, DSN: "http://localhost:9200", Elasticsearch: &ElasticsearchConfig{Password: "pass"},
			}}},
			want: ErrInvalidConfig,
		},
		{
			name:   "missing kafka config",
			config: Config{MQ: map[string]MQConfig{"events": {Type: MQTypeKafka}}},
			want:   ErrInvalidConfig,
		},
		{
			name:   "missing kafka broker",
			config: Config{MQ: map[string]MQConfig{"events": {Type: MQTypeKafka, Kafka: &KafkaConfig{}}}},
			want:   ErrInvalidConfig,
		},
		{
			name: "negative kafka consumer batch size",
			config: Config{MQ: map[string]MQConfig{"events": {
				Type: MQTypeKafka, Kafka: &KafkaConfig{Brokers: []string{"localhost:9092"}, ConsumerBatchSize: -1},
			}}},
			want: ErrInvalidConfig,
		},
		{
			name: "negative kafka handler timeout",
			config: Config{MQ: map[string]MQConfig{"events": {
				Type: MQTypeKafka, Kafka: &KafkaConfig{Brokers: []string{"localhost:9092"}, HandlerTimeout: -time.Second},
			}}},
			want: ErrInvalidConfig,
		},
		{
			name: "kafka retry max less than base",
			config: Config{MQ: map[string]MQConfig{"events": {
				Type: MQTypeKafka,
				Kafka: &KafkaConfig{
					Brokers:         []string{"localhost:9092"},
					RetryBackoff:    2 * time.Second,
					RetryMaxBackoff: time.Second,
				},
			}}},
			want: ErrInvalidConfig,
		},
		{
			name: "unsupported kafka sasl",
			config: Config{MQ: map[string]MQConfig{"events": {
				Type: MQTypeKafka,
				Kafka: &KafkaConfig{
					Brokers: []string{"localhost:9092"},
					SASL:    &SASLConfig{Mechanism: SASLMechanism("unknown"), Username: "user", Password: "pass"},
				},
			}}},
			want: ErrInvalidConfig,
		},
		{
			name: "mismatched nested data config",
			config: Config{Data: map[string]DataConfig{"main": {
				Type: DataTypeMySQL, DSN: "user:pass@tcp(localhost:3306)/db", Redis: &RedisConfig{},
			}}},
			want: ErrInvalidConfig,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateConfig(test.config); !errors.Is(err, test.want) {
				t.Fatalf("validateConfig() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestValidateConfigAcceptsDataAndMQTogether(t *testing.T) {
	config := Config{
		Data: map[string]DataConfig{
			"mysql":         {Type: DataTypeMySQL, DSN: "user:pass@tcp(localhost:3306)/db"},
			"postgres":      {Type: DataTypePostgreSQL, DSN: "postgres://localhost/db"},
			"redis":         {Type: DataTypeRedis, DSN: "redis://localhost:6379/0"},
			"redis-cluster": {Type: DataTypeRedisCluster, DSN: "redis://localhost:6379?addr=localhost:6380"},
			"elasticsearch": {Type: DataTypeElasticsearch, DSN: "http://localhost:9200"},
			"redis-structured": {
				Type: DataTypeRedis, Redis: &RedisConfig{Addrs: []string{"localhost:6379"}},
			},
			"es-structured": {
				Type: DataTypeElasticsearch, Elasticsearch: &ElasticsearchConfig{Addresses: []string{"http://localhost:9200"}},
			},
		},
		MQ: map[string]MQConfig{
			"events": {Type: MQTypeKafka, Kafka: &KafkaConfig{Brokers: []string{"localhost:9092", "localhost:9093"}}},
			"redis-events": {
				Type: MQTypeRedisStream, DSN: "redis://localhost:6379/0", RedisStream: &RedisStreamConfig{},
			},
			"redis-cluster-events": {
				Type:        MQTypeRedisStreamCluster,
				Redis:       &RedisConfig{Addrs: []string{"localhost:6379", "localhost:6380"}},
				RedisStream: &RedisStreamConfig{ConsumerBatchSize: 128, QueueDepth: 256, Concurrency: 16},
			},
		},
	}

	if err := validateConfig(config); err != nil {
		t.Fatalf("validateConfig() error: %v", err)
	}
}
