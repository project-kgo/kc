// Package infra 提供基础设施客户端的统一配置、初始化与资源注册能力。
package infra

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"
)

// DataType 表示数据资源的具体实现类型。
type DataType string

const (
	DataTypeMySQL         DataType = "mysql"
	DataTypePostgreSQL    DataType = "postgres"
	DataTypeRedis         DataType = "redis"
	DataTypeRedisCluster  DataType = "redis-cluster"
	DataTypeElasticsearch DataType = "elasticsearch"
)

// MQType 表示消息队列的具体实现类型。
type MQType string

const (
	MQTypeKafka              MQType = "kafka"
	MQTypeKafkaShare         MQType = "kafka-share"
	MQTypeRedisStream        MQType = "redis-stream"
	MQTypeRedisStreamCluster MQType = "redis-stream-cluster"
)

// SASLMechanism 表示 Kafka 使用的 SASL 认证机制。
type SASLMechanism string

const (
	SASLPlain       SASLMechanism = "plain"
	SASLSCRAMSHA256 SASLMechanism = "scram-sha-256"
	SASLSCRAMSHA512 SASLMechanism = "scram-sha-512"
)

var (
	// ErrInvalidConfig 表示配置字段缺失、冲突或取值非法。
	ErrInvalidConfig = errors.New("infra: invalid config")
	// ErrUnsupportedType 表示客户端类型尚未被组件支持。
	ErrUnsupportedType = errors.New("infra: unsupported type")
	// ErrAlreadyInitialized 表示同模块的同名资源已经存在。
	ErrAlreadyInitialized = errors.New("infra: resource already initialized")
)

// Config 按模块描述需要初始化的数据资源和消息队列。
// map key 同时作为资源注册和获取时使用的名称。
type Config struct {
	Data map[string]DataConfig `json:"data,omitempty" yaml:"data,omitempty"`
	MQ   map[string]MQConfig   `json:"mq,omitempty" yaml:"mq,omitempty"`
}

// DataConfig 描述一个数据资源客户端。
type DataConfig struct {
	Type         DataType      `json:"type" yaml:"type"`
	DSN          string        `json:"dsn,omitempty" yaml:"dsn,omitempty"`
	SkipCheck    bool          `json:"skip_check,omitempty" yaml:"skip_check,omitempty"`
	CheckTimeout time.Duration `json:"check_timeout,omitempty" yaml:"check_timeout,omitempty"`

	SQL           *SQLConfig           `json:"sql,omitempty" yaml:"sql,omitempty"`
	Redis         *RedisConfig         `json:"redis,omitempty" yaml:"redis,omitempty"`
	Elasticsearch *ElasticsearchConfig `json:"elasticsearch,omitempty" yaml:"elasticsearch,omitempty"`
}

// MQConfig 描述一个消息队列客户端。
type MQConfig struct {
	Type         MQType        `json:"type" yaml:"type"`
	DSN          string        `json:"dsn,omitempty" yaml:"dsn,omitempty"`
	SkipCheck    bool          `json:"skip_check,omitempty" yaml:"skip_check,omitempty"`
	CheckTimeout time.Duration `json:"check_timeout,omitempty" yaml:"check_timeout,omitempty"`

	Kafka       *KafkaConfig       `json:"kafka,omitempty" yaml:"kafka,omitempty"`
	Redis       *RedisConfig       `json:"redis,omitempty" yaml:"redis,omitempty"`
	RedisStream *RedisStreamConfig `json:"redis_stream,omitempty" yaml:"redis_stream,omitempty"`
}

// SQLConfig 配置 database/sql 连接池。零值保留 database/sql 默认行为。
type SQLConfig struct {
	MaxOpenConns    int           `json:"max_open_conns,omitempty" yaml:"max_open_conns,omitempty"`
	MaxIdleConns    int           `json:"max_idle_conns,omitempty" yaml:"max_idle_conns,omitempty"`
	ConnMaxLifetime time.Duration `json:"conn_max_lifetime,omitempty" yaml:"conn_max_lifetime,omitempty"`
	ConnMaxIdleTime time.Duration `json:"conn_max_idle_time,omitempty" yaml:"conn_max_idle_time,omitempty"`
}

// RedisConfig 配置普通 Redis 或 Redis Cluster 客户端。
type RedisConfig struct {
	Addrs          []string      `json:"addrs,omitempty" yaml:"addrs,omitempty"`
	Username       string        `json:"username,omitempty" yaml:"username,omitempty"`
	Password       string        `json:"password,omitempty" yaml:"password,omitempty"`
	DB             int           `json:"db,omitempty" yaml:"db,omitempty"`
	ClientName     string        `json:"client_name,omitempty" yaml:"client_name,omitempty"`
	Protocol       int           `json:"protocol,omitempty" yaml:"protocol,omitempty"`
	PoolSize       int           `json:"pool_size,omitempty" yaml:"pool_size,omitempty"`
	MinIdleConns   int           `json:"min_idle_conns,omitempty" yaml:"min_idle_conns,omitempty"`
	DialTimeout    time.Duration `json:"dial_timeout,omitempty" yaml:"dial_timeout,omitempty"`
	ReadTimeout    time.Duration `json:"read_timeout,omitempty" yaml:"read_timeout,omitempty"`
	WriteTimeout   time.Duration `json:"write_timeout,omitempty" yaml:"write_timeout,omitempty"`
	TLS            bool          `json:"tls,omitempty" yaml:"tls,omitempty"`
	ReadOnly       bool          `json:"read_only,omitempty" yaml:"read_only,omitempty"`
	RouteByLatency bool          `json:"route_by_latency,omitempty" yaml:"route_by_latency,omitempty"`
	RouteRandomly  bool          `json:"route_randomly,omitempty" yaml:"route_randomly,omitempty"`
}

// ElasticsearchConfig 配置 Elasticsearch 地址、认证和常用传输选项。
type ElasticsearchConfig struct {
	Addresses              []string `json:"addresses,omitempty" yaml:"addresses,omitempty"`
	CloudID                string   `json:"cloud_id,omitempty" yaml:"cloud_id,omitempty"`
	Username               string   `json:"username,omitempty" yaml:"username,omitempty"`
	Password               string   `json:"password,omitempty" yaml:"password,omitempty"`
	APIKey                 string   `json:"api_key,omitempty" yaml:"api_key,omitempty"`
	CertificateFingerprint string   `json:"certificate_fingerprint,omitempty" yaml:"certificate_fingerprint,omitempty"`
	EnableCompression      bool     `json:"enable_compression,omitempty" yaml:"enable_compression,omitempty"`
	DiscoverNodesOnStart   bool     `json:"discover_nodes_on_start,omitempty" yaml:"discover_nodes_on_start,omitempty"`
}

// KafkaConfig 配置 Kafka broker、认证、消费并发和失败重试策略。
// MaxRetries 仅用于传统 consumer group，表示首次失败后的额外原地重试次数。
// MaxDeliveryAttempts 仅用于 share group，包含首次投递；零值默认为 4，上限为 4。
// 部署时 broker 的 group.share.delivery.count.limit 应配置为 5。
type KafkaConfig struct {
	Brokers     []string      `json:"brokers,omitempty" yaml:"brokers,omitempty"`
	ClientID    string        `json:"client_id,omitempty" yaml:"client_id,omitempty"`
	DialTimeout time.Duration `json:"dial_timeout,omitempty" yaml:"dial_timeout,omitempty"`
	TLS         bool          `json:"tls,omitempty" yaml:"tls,omitempty"`
	SASL        *SASLConfig   `json:"sasl,omitempty" yaml:"sasl,omitempty"`

	// ConsumerBatchSize 和 Concurrency 的零值分别默认为 100 和 10。
	ConsumerBatchSize int `json:"consumer_batch_size,omitempty" yaml:"consumer_batch_size,omitempty"`
	Concurrency       int `json:"concurrency,omitempty" yaml:"concurrency,omitempty"`
	// HandlerTimeout 的零值在传统组中为 30 秒，在 share group 中为 15 秒；
	// share group 中应小于 broker 的 share.record.lock.duration.ms。
	HandlerTimeout      time.Duration `json:"handler_timeout,omitempty" yaml:"handler_timeout,omitempty"`
	MaxRetries          int           `json:"max_retries,omitempty" yaml:"max_retries,omitempty"`
	MaxDeliveryAttempts int           `json:"max_delivery_attempts,omitempty" yaml:"max_delivery_attempts,omitempty"`
	// BatchProcessingTimeout 和 RebalanceTimeout 仅用于传统组，零值分别默认为 45 秒和 60 秒。
	// 前者限制一批消息从 Handler 到 DLQ 的总处理时间，必须小于后者。
	BatchProcessingTimeout time.Duration `json:"batch_processing_timeout,omitempty" yaml:"batch_processing_timeout,omitempty"`
	RebalanceTimeout       time.Duration `json:"rebalance_timeout,omitempty" yaml:"rebalance_timeout,omitempty"`
	// RetryBackoff 和 RetryMaxBackoff 的零值分别默认为 1 秒和 30 秒。
	RetryBackoff    time.Duration `json:"retry_backoff,omitempty" yaml:"retry_backoff,omitempty"`
	RetryMaxBackoff time.Duration `json:"retry_max_backoff,omitempty" yaml:"retry_max_backoff,omitempty"`
	// Logger 用于记录后台消费、重投、提交和 DLQ 异常；为空时使用 slog.Default()。
	Logger *slog.Logger `json:"-" yaml:"-"`
}

// SASLConfig 配置 Kafka SASL 用户名、密码和认证机制。
type SASLConfig struct {
	Mechanism SASLMechanism `json:"mechanism" yaml:"mechanism"`
	Username  string        `json:"username" yaml:"username"`
	Password  string        `json:"password" yaml:"password"`
}

// RedisStreamConfig 配置 Redis 7.0+ Streams 的批量消费、并发、重试和裁剪策略。
// 消费相关字段是订阅默认值，可由 pkg/mq 的 SubscribeOption 按订阅覆盖。
type RedisStreamConfig struct {
	KeyPrefix string `json:"key_prefix,omitempty" yaml:"key_prefix,omitempty"`
	// ConsumerBatchSize、QueueDepth 和 Concurrency 分别限制单次拉取、本地积压和并发 Handler 数量。
	ConsumerBatchSize int64 `json:"consumer_batch_size,omitempty" yaml:"consumer_batch_size,omitempty"`
	QueueDepth        int   `json:"queue_depth,omitempty" yaml:"queue_depth,omitempty"`
	Concurrency       int   `json:"concurrency,omitempty" yaml:"concurrency,omitempty"`
	// ACK 使用有界异步队列，达到批量大小或刷新间隔时提交。
	AckBatchSize     int           `json:"ack_batch_size,omitempty" yaml:"ack_batch_size,omitempty"`
	AckFlushInterval time.Duration `json:"ack_flush_interval,omitempty" yaml:"ack_flush_interval,omitempty"`
	// ReclaimMaxBatches 限制单轮 XAUTOCLAIM 调用次数，游标会在多轮之间延续。
	ReclaimMaxBatches int `json:"reclaim_max_batches,omitempty" yaml:"reclaim_max_batches,omitempty"`
	// MaxDeliveryAttempts 包含首次投递；达到上限后消息会进入 <topic>.dlq。
	MaxDeliveryAttempts int `json:"max_delivery_attempts,omitempty" yaml:"max_delivery_attempts,omitempty"`
	// ConsumerID 是 Redis consumer 名称的可选前缀，运行时总会追加唯一随机后缀。
	ConsumerID         string        `json:"consumer_id,omitempty" yaml:"consumer_id,omitempty"`
	GroupStartID       string        `json:"group_start_id,omitempty" yaml:"group_start_id,omitempty"`
	ReadBlock          time.Duration `json:"read_block,omitempty" yaml:"read_block,omitempty"`
	HandlerTimeout     time.Duration `json:"handler_timeout,omitempty" yaml:"handler_timeout,omitempty"`
	PendingIdleTimeout time.Duration `json:"pending_idle_timeout,omitempty" yaml:"pending_idle_timeout,omitempty"`
	RedeliverInterval  time.Duration `json:"redeliver_interval,omitempty" yaml:"redeliver_interval,omitempty"`
	RetryBackoff       time.Duration `json:"retry_backoff,omitempty" yaml:"retry_backoff,omitempty"`
	RetryMaxBackoff    time.Duration `json:"retry_max_backoff,omitempty" yaml:"retry_max_backoff,omitempty"`
	// ConsumerCleanupInterval 和 ConsumerCleanupIdleTimeout 控制零 Pending 遗留 consumer 的安全清理。
	ConsumerCleanupInterval    time.Duration `json:"consumer_cleanup_interval,omitempty" yaml:"consumer_cleanup_interval,omitempty"`
	ConsumerCleanupIdleTimeout time.Duration `json:"consumer_cleanup_idle_timeout,omitempty" yaml:"consumer_cleanup_idle_timeout,omitempty"`
	// MaxLen 使用近似 MAXLEN 裁剪 Stream；正值会将至少一次保证限制在消息保留窗口内。
	MaxLen int64 `json:"max_len,omitempty" yaml:"max_len,omitempty"`
	// Logger 用于记录处理、重投、DLQ 与清理异常；为空时使用 slog.Default()。
	Logger *slog.Logger `json:"-" yaml:"-"`
}

func validateConfig(config Config) error {
	for _, name := range sortedMapKeys(config.Data) {
		dataConfig := config.Data[name]
		if err := validateResourceName(name); err != nil {
			return fmt.Errorf("infra: validate data %q (%s): %w", name, dataConfig.Type, err)
		}
		if err := validateDataConfig(dataConfig); err != nil {
			return fmt.Errorf("infra: validate data %q (%s): %w", name, dataConfig.Type, err)
		}
	}
	for _, name := range sortedMapKeys(config.MQ) {
		mqConfig := config.MQ[name]
		if err := validateResourceName(name); err != nil {
			return fmt.Errorf("infra: validate mq %q (%s): %w", name, mqConfig.Type, err)
		}
		if err := validateMQConfig(mqConfig); err != nil {
			return fmt.Errorf("infra: validate mq %q (%s): %w", name, mqConfig.Type, err)
		}
	}
	return nil
}

func validateResourceName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%w: resource name is empty", ErrInvalidConfig)
	}
	return nil
}

func validateDataConfig(config DataConfig) error {
	if config.CheckTimeout < 0 {
		return fmt.Errorf("%w: check timeout is negative", ErrInvalidConfig)
	}
	if err := validateDataNestedConfig(config); err != nil {
		return err
	}
	switch config.Type {
	case DataTypeMySQL, DataTypePostgreSQL:
		return validateSQLConfig(config)
	case DataTypeRedis, DataTypeRedisCluster:
		return validateRedisConfig(config)
	case DataTypeElasticsearch:
		return validateElasticsearchConfig(config)
	default:
		return fmt.Errorf("%w: data %q", ErrUnsupportedType, config.Type)
	}
}

func validateMQConfig(config MQConfig) error {
	if config.CheckTimeout < 0 {
		return fmt.Errorf("%w: check timeout is negative", ErrInvalidConfig)
	}
	switch config.Type {
	case MQTypeKafka, MQTypeKafkaShare:
		if config.DSN != "" || config.Redis != nil || config.RedisStream != nil {
			return fmt.Errorf("%w: unexpected non-kafka config", ErrInvalidConfig)
		}
		return validateKafkaConfig(config)
	case MQTypeRedisStream, MQTypeRedisStreamCluster:
		if config.Kafka != nil {
			return fmt.Errorf("%w: unexpected kafka config", ErrInvalidConfig)
		}
		return validateRedisStreamConfig(config)
	default:
		return fmt.Errorf("%w: mq %q", ErrUnsupportedType, config.Type)
	}
}

func validateDataNestedConfig(config DataConfig) error {
	switch config.Type {
	case DataTypeMySQL, DataTypePostgreSQL:
		if config.Redis != nil || config.Elasticsearch != nil {
			return fmt.Errorf("%w: unexpected data config", ErrInvalidConfig)
		}
	case DataTypeRedis, DataTypeRedisCluster:
		if config.SQL != nil || config.Elasticsearch != nil {
			return fmt.Errorf("%w: unexpected data config", ErrInvalidConfig)
		}
	case DataTypeElasticsearch:
		if config.SQL != nil || config.Redis != nil {
			return fmt.Errorf("%w: unexpected data config", ErrInvalidConfig)
		}
	default:
		// 未知类型由 validateDataConfig 返回更准确的错误。
	}
	return nil
}

func sortedMapKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
