// Package compensator 提供基于 PostgreSQL 的本地任务补偿组件。
//
// 业务在自己的事务中登记任务，提交后由所有节点的后台 worker 共同领取执行；
// 失败或进程崩溃时按退避策略重试，直至成功或耗尽重试次数。
package compensator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bytedance/sonic"
	"github.com/google/uuid"
)

var (
	ErrInvalidConfig    = errors.New("compensator: invalid config")
	ErrInvalidArgument  = errors.New("compensator: invalid argument")
	ErrResourceNotFound = errors.New("compensator: SQL resource not found")
	ErrClosed           = errors.New("compensator: closed")
	ErrNotFound         = errors.New("compensator: task not found")
	ErrNotFailed        = errors.New("compensator: task is not failed")
	ErrLeaseLost        = errors.New("compensator: lease lost")
	ErrHandlerPanic     = errors.New("compensator: handler panic")
	ErrNoHandler        = errors.New("compensator: handler not registered")
)

// Status 是任务的持久化状态。
type Status int16

const (
	StatusPending Status = iota
	StatusRunning
	StatusSucceeded
	StatusFailed
)

func (s Status) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusRunning:
		return "running"
	case StatusSucceeded:
		return "succeeded"
	case StatusFailed:
		return "failed"
	default:
		return fmt.Sprintf("Status(%d)", int16(s))
	}
}

// Task 是交给回调的任务。ID 稳定不变，业务应以它实现幂等。
type Task struct {
	ID      uuid.UUID
	Type    string
	Payload string
	// Attempt 是当前第几次执行，首次为 1。
	Attempt int
}

// NewTask 使用 sonic 将 value 编码为 Payload 字符串。
func NewTask(taskType string, value any) (Task, error) {
	if err := validateType(taskType); err != nil {
		return Task{}, err
	}
	payload, err := sonic.MarshalString(value)
	if err != nil {
		return Task{}, fmt.Errorf("%w: encode payload: %w", ErrInvalidArgument, err)
	}
	return Task{Type: taskType, Payload: payload}, nil
}

// Decode 使用 sonic 将 Payload 解码到 dst，dst 应为业务结构体指针以保留 int64 精度。
func (t *Task) Decode(dst any) error {
	if dst == nil {
		return fmt.Errorf("%w: decode destination is nil", ErrInvalidArgument)
	}
	if err := sonic.UnmarshalString(t.Payload, dst); err != nil {
		return fmt.Errorf("%w: decode payload: %w", ErrInvalidArgument, err)
	}
	return nil
}

// Record 是任务的完整持久化状态，时间均来自数据库时钟。
type Record struct {
	Task
	Status    Status
	Attempts  int
	NextRunAt time.Time
	LastError string
	CreatedAt time.Time
	UpdatedAt time.Time
	// CompletedAt 在成功或失败终态时设置，否则为零值。
	CompletedAt time.Time
}

// Handler 必须并发安全、幂等，并及时响应 context 取消。
type Handler func(ctx context.Context, task *Task) error

// Config 的数量和时长为 0 时采用默认值。
type Config struct {
	// TableSuffix 必填，只能包含小写字母、数字和下划线，最长 40 字符。
	TableSuffix string
	// Schema 为空时使用连接的 current_schema()，必须预先存在。
	Schema string

	// WorkerCount 是每个节点同时执行回调的 worker 数，默认 8。
	WorkerCount int
	// ScanInterval 是空闲扫描间隔，默认 1 秒；处理满批后会立即再扫。
	ScanInterval time.Duration
	// BatchSize 是单次领取上限，默认 2×WorkerCount，实际不超过空闲 worker 数。
	BatchSize int
	// MaxRetries 不含首次执行，默认 10；-1 表示不重试。
	MaxRetries int
	// RetryBackoffMin / RetryBackoffMax 是指数退避的基数和上限，默认 1 秒 / 5 分钟。
	RetryBackoffMin time.Duration
	RetryBackoffMax time.Duration
	// HandlerTimeout 是回调 context 的超时，默认 30 秒。
	HandlerTimeout time.Duration
	// LeaseDuration 是 running 任务被其它节点接管前的期限，默认 HandlerTimeout+10 秒。
	LeaseDuration time.Duration

	// CleanupAfter 是成功记录的保留时长，默认 5 分钟。
	CleanupAfter time.Duration
	// CleanupInterval 是清理检查间隔，默认 30 秒。
	CleanupInterval time.Duration
	// CleanupBatchSize 是每批删除的行数，默认 1000。
	CleanupBatchSize int
	// FailedRetention 大于 0 时清理失败超过该时长的记录；默认 0 表示保留。
	FailedRetention time.Duration

	// OperationTimeout 是单次后台数据库操作的超时，默认 5 秒。
	OperationTimeout time.Duration

	// DisableWorker 为 true 时只登记任务，不执行和清理。
	DisableWorker bool
	Logger        *slog.Logger
	// Metrics 为空时不上报指标。
	Metrics Metrics
}

type resolvedConfig struct {
	Config
	retries int
}

var (
	suffixPattern     = regexp.MustCompile(`^[a-z0-9_]{1,40}$`)
	identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)
)

func resolveConfig(c Config) (resolvedConfig, error) {
	if !suffixPattern.MatchString(c.TableSuffix) {
		return resolvedConfig{}, fmt.Errorf("%w: table suffix must match %s", ErrInvalidConfig, suffixPattern)
	}
	if c.Schema != "" && !identifierPattern.MatchString(c.Schema) {
		return resolvedConfig{}, fmt.Errorf("%w: unsafe schema name", ErrInvalidConfig)
	}
	if c.WorkerCount < 0 || c.BatchSize < 0 || c.CleanupBatchSize < 0 {
		return resolvedConfig{}, fmt.Errorf("%w: negative count", ErrInvalidConfig)
	}
	if c.WorkerCount == 0 {
		c.WorkerCount = 8
	}
	if c.BatchSize == 0 {
		c.BatchSize = 2 * c.WorkerCount
	}
	if c.CleanupBatchSize == 0 {
		c.CleanupBatchSize = 1000
	}
	if c.HandlerTimeout == 0 {
		c.HandlerTimeout = 30 * time.Second
	}
	if c.LeaseDuration == 0 {
		c.LeaseDuration = c.HandlerTimeout + 10*time.Second
	}
	for _, d := range []struct {
		p   *time.Duration
		def time.Duration
	}{
		{&c.ScanInterval, time.Second}, {&c.RetryBackoffMin, time.Second}, {&c.RetryBackoffMax, 5 * time.Minute},
		{&c.CleanupAfter, 5 * time.Minute}, {&c.CleanupInterval, 30 * time.Second}, {&c.OperationTimeout, 5 * time.Second},
		{&c.HandlerTimeout, 0}, {&c.LeaseDuration, 0},
	} {
		if *d.p == 0 {
			*d.p = d.def
		}
		if *d.p < time.Millisecond {
			return resolvedConfig{}, fmt.Errorf("%w: durations must be at least 1ms", ErrInvalidConfig)
		}
	}
	if c.FailedRetention < 0 {
		return resolvedConfig{}, fmt.Errorf("%w: negative failed retention", ErrInvalidConfig)
	}
	if c.LeaseDuration <= c.HandlerTimeout {
		return resolvedConfig{}, fmt.Errorf("%w: lease duration must exceed handler timeout", ErrInvalidConfig)
	}
	if c.RetryBackoffMax < c.RetryBackoffMin {
		return resolvedConfig{}, fmt.Errorf("%w: retry backoff max is less than min", ErrInvalidConfig)
	}
	retries := c.MaxRetries
	switch {
	case retries == 0:
		retries = 10
	case retries == -1:
		retries = 0
	case retries < -1 || retries > 1_000_000:
		return resolvedConfig{}, fmt.Errorf("%w: max retries out of range", ErrInvalidConfig)
	}
	if c.Metrics == nil {
		c.Metrics = noopMetrics{}
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return resolvedConfig{Config: c, retries: retries}, nil
}

func validateType(value string) error {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || len(value) > 255 {
		return fmt.Errorf("%w: task type must be 1-255 bytes of UTF-8", ErrInvalidArgument)
	}
	return nil
}
