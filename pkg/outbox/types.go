// Package outbox 提供基于业务数据库、支持多节点补偿的本地消息表。
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bytedance/sonic"
)

const shardCount = 64

var (
	ErrInvalidConfig    = errors.New("outbox: invalid config")
	ErrInvalidArgument  = errors.New("outbox: invalid argument")
	ErrResourceNotFound = errors.New("outbox: SQL resource not found")
	ErrClosed           = errors.New("outbox: closed")
	ErrNotFound         = errors.New("outbox: message not found")
	ErrNotFailed        = errors.New("outbox: message is not failed")
	ErrLeaseLost        = errors.New("outbox: lease lost")
	ErrHandlerPanic     = errors.New("outbox: handler panic")
	ErrNoHandler        = errors.New("outbox: handler not registered")
)

// Status 是消息的持久化状态。
type Status int

const (
	StatusPending Status = iota
	StatusProcessing
	StatusSucceeded
	StatusFailed
)

// Message 的 Payload 始终是完整 JSON 文档；Attempts 包含当前投递。
// 回调收到的 ID 可用于业务幂等，Retry 不会改变该 ID。
type Message struct {
	ID       int64  `db:"id"`
	Type     string `db:"message_type"`
	Payload  string `db:"payload"`
	Attempts int    `db:"attempts"`
}

// NewMessage 只编码一次；已编码的 JSON 可直接赋值给 Message.Payload。
func NewMessage(messageType string, value any) (Message, error) {
	if err := validateType(messageType); err != nil {
		return Message{}, err
	}
	payload, err := sonic.MarshalString(value)
	if err != nil {
		return Message{}, fmt.Errorf("%w: encode payload: %w", ErrInvalidArgument, err)
	}
	return Message{Type: messageType, Payload: payload}, nil
}

// Decode 将 JSON 字符串解码到业务提供的指针，避免自动转换为 map 后丢失整数精度。
func (m Message) Decode(dst any) error {
	if dst == nil {
		return fmt.Errorf("%w: decode destination is nil", ErrInvalidArgument)
	}
	if err := sonic.UnmarshalString(m.Payload, dst); err != nil {
		return fmt.Errorf("%w: decode payload: %w", ErrInvalidArgument, err)
	}
	return nil
}

// Record 中所有时间字段均为数据库时钟的 Unix 毫秒，0 表示尚未设置。
type Record struct {
	Message
	ShardID       int    `db:"shard_id"`
	Status        Status `db:"status"`
	NextAttemptAt int64  `db:"next_attempt_at"`
	LeaseUntil    int64  `db:"lease_until"`
	LastError     string `db:"last_error"`
	CreatedAt     int64  `db:"created_at"`
	UpdatedAt     int64  `db:"updated_at"`
	CompletedAt   int64  `db:"completed_at"`
}

// Result 的 AttemptError 仅表示提交后的处理结果，不代表业务事务回滚。
type Result struct {
	ID           int64
	Status       Status
	AttemptError error
}

// Handler 必须并发安全、幂等，并及时响应 context 取消。
type Handler func(context.Context, *Message) error

// PostgresOptions 只在创建新消息表时应用，不覆盖已有表的运维设置。
// 自定义时可先调用 DefaultPostgresOptions，再修改所需字段。
type PostgresOptions struct {
	FillFactor         int
	VacuumScaleFactor  float64
	VacuumThreshold    int
	AnalyzeScaleFactor float64
	AnalyzeThreshold   int
	VacuumTruncate     bool
}

func DefaultPostgresOptions() PostgresOptions {
	return PostgresOptions{FillFactor: 90, VacuumScaleFactor: 0.01, VacuumThreshold: 1000, AnalyzeScaleFactor: 0.02, AnalyzeThreshold: 1000}
}

// Config 的时长和数量为 0 时采用默认值。MaxRetries 为 nil 时默认 20，
// 指向 0 时禁止重试；该数量不包含首次投递。SchemaName 必须已存在。
type Config struct {
	SchemaName        string
	TableSuffix       string
	DisableBackground bool
	WorkerCount       int
	ScanInterval      time.Duration
	BatchSize         int
	HandlerTimeout    time.Duration
	TaskLease         time.Duration
	HeartbeatInterval time.Duration
	ShardLease        time.Duration
	MaxRetries        *int
	RetryMin          time.Duration
	RetryMax          time.Duration
	Retention         time.Duration
	CleanupInterval   time.Duration
	CleanupBatchSize  int
	OperationTimeout  time.Duration
	CloseTimeout      time.Duration
	Postgres          *PostgresOptions
	Logger            *slog.Logger
}

type resolvedConfig struct {
	Config
	retries  int
	postgres PostgresOptions
}

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var suffixPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

func resolveConfig(c Config) (resolvedConfig, error) {
	if (c.SchemaName != "" && (len(c.SchemaName) > 63 || !identifierPattern.MatchString(c.SchemaName))) ||
		(c.TableSuffix != "" && (len(c.TableSuffix) > 49 || !suffixPattern.MatchString(c.TableSuffix))) {
		return resolvedConfig{}, fmt.Errorf("%w: unsafe schema name or table suffix", ErrInvalidConfig)
	}
	for _, pair := range []struct {
		p   *int
		def int
	}{{&c.WorkerCount, 4}, {&c.BatchSize, 100}, {&c.CleanupBatchSize, 1000}} {
		if *pair.p < 0 {
			return resolvedConfig{}, fmt.Errorf("%w: negative count", ErrInvalidConfig)
		}
		if *pair.p == 0 {
			*pair.p = pair.def
		}
	}
	for _, pair := range []struct {
		p   *time.Duration
		def time.Duration
	}{
		{&c.ScanInterval, time.Second}, {&c.HandlerTimeout, 30 * time.Second}, {&c.TaskLease, time.Minute},
		{&c.HeartbeatInterval, 5 * time.Second}, {&c.ShardLease, 30 * time.Second},
		{&c.RetryMin, time.Second}, {&c.RetryMax, 5 * time.Minute}, {&c.Retention, 24 * time.Hour},
		{&c.CleanupInterval, time.Minute}, {&c.OperationTimeout, 5 * time.Second}, {&c.CloseTimeout, 35 * time.Second},
	} {
		if *pair.p == 0 {
			*pair.p = pair.def
		}
		if *pair.p < time.Millisecond {
			return resolvedConfig{}, fmt.Errorf("%w: durations must be at least 1ms", ErrInvalidConfig)
		}
	}
	if c.TaskLease <= c.HandlerTimeout || c.TaskLease-c.HandlerTimeout < c.OperationTimeout ||
		c.HeartbeatInterval > c.ShardLease/3 || c.RetryMax < c.RetryMin {
		return resolvedConfig{}, fmt.Errorf("%w: inconsistent timeout, lease or retry durations", ErrInvalidConfig)
	}
	retries := 20
	if c.MaxRetries != nil {
		retries = *c.MaxRetries
	}
	if retries < 0 || retries >= math.MaxInt32 {
		return resolvedConfig{}, fmt.Errorf("%w: max retries out of range", ErrInvalidConfig)
	}
	pg := DefaultPostgresOptions()
	if c.Postgres != nil {
		pg = *c.Postgres
	}
	if pg.FillFactor < 10 || pg.FillFactor > 100 || pg.VacuumThreshold < 0 || pg.AnalyzeThreshold < 0 ||
		math.IsNaN(pg.VacuumScaleFactor) || math.IsNaN(pg.AnalyzeScaleFactor) ||
		pg.VacuumScaleFactor < 0 || pg.VacuumScaleFactor > 100 || pg.AnalyzeScaleFactor < 0 || pg.AnalyzeScaleFactor > 100 {
		return resolvedConfig{}, fmt.Errorf("%w: invalid PostgreSQL storage options", ErrInvalidConfig)
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	// 拷贝指针配置，调用方后续修改原配置不影响运行中的实例。
	c.MaxRetries = nil
	c.Postgres = nil
	return resolvedConfig{Config: c, retries: retries, postgres: pg}, nil
}

func validateType(value string) error {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || utf8.RuneCountInString(value) > 128 {
		return fmt.Errorf("%w: message type must contain 1-128 UTF-8 characters", ErrInvalidArgument)
	}
	return nil
}
