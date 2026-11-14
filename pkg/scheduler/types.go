// Package scheduler 提供基于 PostgreSQL 与 Redis 的分布式定时任务组件。
//
// 任务在 PostgreSQL 中持久化，近期窗口内的任务由 Redis 承载到期触发。业务可把任务与业务
// 数据写在同一个事务里，提交后由所有节点的后台循环共同领取执行；回调失败按指数退避重试，
// 进程崩溃时由租约回收兜底。语义为至少执行一次，回调必须幂等。
//
// Redis 只保存窗口内任务的索引和易失任务的内容，可丢失、可重建；PostgreSQL 是唯一真相源。
// 延迟小于 VolatileBelow 的任务默认只写 Redis（第 4.9 节），不参与上述持久化保证。
package scheduler

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
	ErrInvalidConfig   = errors.New("scheduler: invalid config")
	ErrInvalidArgument = errors.New("scheduler: invalid argument")
	ErrNoHandler       = errors.New("scheduler: handler not registered")
	ErrNotFound        = errors.New("scheduler: task not found")
	ErrFinished        = errors.New("scheduler: task is already finished")
	ErrClosed          = errors.New("scheduler: closed")
)

// Status 是任务的持久化状态。执行中的事实由 Redis processing 集合表达，PostgreSQL 中没有
// 独立的 running 状态。
type Status int16

const (
	// StatusPending 表示任务只在 PostgreSQL，尚未进入 Redis。
	StatusPending Status = iota
	// StatusQueued 表示任务已进入（或应当进入）Redis，包含执行中。
	StatusQueued
	StatusSucceeded
	StatusFailed
	StatusCancelled
)

func (s Status) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusQueued:
		return "queued"
	case StatusSucceeded:
		return "succeeded"
	case StatusFailed:
		return "failed"
	case StatusCancelled:
		return "cancelled"
	default:
		return fmt.Sprintf("Status(%d)", int16(s))
	}
}

// Task 是登记入口接收、回调接收的任务。ID 稳定不变，业务应以它实现幂等。
type Task struct {
	ID      uuid.UUID
	Type    string
	Payload string
	// Attempts 是已失败的尝试次数，首次执行时为 0；回调可据此区分首次与重试。
	Attempts int
	// RunAt 指定首次执行时间：零值或过去的时间表示立即执行。到期判断使用 Redis 或数据库
	// 时钟，判断「是否近期任务」时例外地使用本机时钟，后者只影响走哪条路径。
	RunAt time.Time
	// Volatile 为 true 时任务只写 Redis、完全不进 PostgreSQL，丢失后不恢复。
	// 只能在 Add 中使用；Transaction / AddTx 中登记返回 ErrInvalidArgument。
	Volatile bool
	// Durable 为 true 时任务不受 VolatileBelow 阈值影响，始终持久化。只在 Add 中有效。
	Durable bool
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
	LastError string
	CreatedAt time.Time
	UpdatedAt time.Time
	// CompletedAt 在终态时设置，否则为零值。
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
	// KeyPrefix 是 Redis key 前缀，默认 sched。
	KeyPrefix string
	// Buckets 是 Redis 分桶数，默认 8；上线后不可直接修改。
	Buckets int
	// Window 是近期窗口，默认 5 分钟，决定 Redis 中的任务数量。
	Window time.Duration
	// LoadInterval 是 loader 扫描间隔，默认 1 秒。
	LoadInterval time.Duration
	// LoadBatch 是 loader 单批行数，默认 1000。
	LoadBatch int
	// PollInterval 是到期扫描间隔，默认 100 毫秒，决定触发精度。
	PollInterval time.Duration
	// WorkerCount 是每个节点同时执行的回调数，默认 16。
	WorkerCount int
	// HandlerTimeout 是回调 context 的超时，默认 30 秒。
	HandlerTimeout time.Duration
	// LeaseDuration 是执行租约，默认 60 秒，必须大于 HandlerTimeout。
	LeaseDuration time.Duration
	// MaxAttempts 是最大尝试次数（含首次），默认 10。
	MaxAttempts int
	// RetryBackoffMin / RetryBackoffMax 是重试退避范围，默认 1 秒 / 5 分钟，
	// 上限不得大于 Window。
	RetryBackoffMin time.Duration
	RetryBackoffMax time.Duration
	// ReclaimInterval 是租约回收间隔，默认 5 秒。
	ReclaimInterval time.Duration
	// ReconcileInterval 是对账间隔，默认 5 秒。
	ReconcileInterval time.Duration
	// ReconcileGrace 是任务逾期多久后才参与对账，默认 10 秒。
	ReconcileGrace time.Duration
	// VolatileBelow 是经 Add 登记时自动转为易失任务的延迟阈值，默认 10 秒；
	// 负数表示关闭阈值，只有显式 Volatile = true 的任务才是易失任务。
	VolatileBelow time.Duration
	// VolatileMaxPayload 是易失任务的 payload 上限，默认 4KB。
	VolatileMaxPayload int
	// Retention 是成功、取消任务的保留时长，默认 24 小时。
	Retention time.Duration
	// FailedRetention 是失败任务的保留时长，0 表示不删除。
	FailedRetention time.Duration
	// CleanupInterval 是清理间隔，默认 1 分钟。
	CleanupInterval time.Duration
	// ShutdownTimeout 是 Close 等待回调的上限，默认 30 秒。
	ShutdownTimeout time.Duration

	Logger  *slog.Logger
	Metrics Metrics
}

var (
	suffixPattern     = regexp.MustCompile(`^[a-z0-9_]{1,40}$`)
	identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)
)

func resolveConfig(c Config) (Config, error) {
	if !suffixPattern.MatchString(c.TableSuffix) {
		return Config{}, fmt.Errorf("%w: table suffix must match %s", ErrInvalidConfig, suffixPattern)
	}
	if c.Schema != "" && !identifierPattern.MatchString(c.Schema) {
		return Config{}, fmt.Errorf("%w: unsafe schema name", ErrInvalidConfig)
	}
	if c.KeyPrefix == "" {
		c.KeyPrefix = "sched"
	}
	if strings.TrimSpace(c.KeyPrefix) == "" {
		return Config{}, fmt.Errorf("%w: key prefix is blank", ErrInvalidConfig)
	}
	for _, d := range []struct {
		p   *time.Duration
		def time.Duration
	}{
		{&c.Window, 5 * time.Minute}, {&c.LoadInterval, time.Second}, {&c.PollInterval, 100 * time.Millisecond},
		{&c.HandlerTimeout, 30 * time.Second}, {&c.LeaseDuration, 60 * time.Second},
		{&c.RetryBackoffMin, time.Second}, {&c.RetryBackoffMax, 5 * time.Minute},
		{&c.ReclaimInterval, 5 * time.Second}, {&c.ReconcileInterval, 5 * time.Second},
		{&c.ReconcileGrace, 10 * time.Second}, {&c.Retention, 24 * time.Hour},
		{&c.CleanupInterval, time.Minute}, {&c.ShutdownTimeout, 30 * time.Second},
	} {
		if *d.p == 0 {
			*d.p = d.def
		}
		if *d.p < time.Millisecond {
			return Config{}, fmt.Errorf("%w: durations must be at least 1ms", ErrInvalidConfig)
		}
	}
	if c.FailedRetention < 0 {
		return Config{}, fmt.Errorf("%w: negative failed retention", ErrInvalidConfig)
	}
	// VolatileBelow 的零值取默认 10 秒，负数关闭阈值。
	switch {
	case c.VolatileBelow == 0:
		c.VolatileBelow = 10 * time.Second
	case c.VolatileBelow < 0:
	default:
		if c.VolatileBelow < time.Millisecond {
			return Config{}, fmt.Errorf("%w: volatile threshold must be at least 1ms", ErrInvalidConfig)
		}
	}
	if c.Buckets < 0 || c.LoadBatch < 0 || c.WorkerCount < 0 || c.VolatileMaxPayload < 0 {
		return Config{}, fmt.Errorf("%w: negative count", ErrInvalidConfig)
	}
	if c.Buckets == 0 {
		c.Buckets = 8
	}
	if c.LoadBatch == 0 {
		c.LoadBatch = 1000
	}
	if c.WorkerCount == 0 {
		c.WorkerCount = 16
	}
	if c.VolatileMaxPayload == 0 {
		c.VolatileMaxPayload = 4096
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 10
	}
	if c.MaxAttempts < 1 || c.MaxAttempts > 1_000_000 {
		return Config{}, fmt.Errorf("%w: max attempts out of range", ErrInvalidConfig)
	}
	if c.LeaseDuration <= c.HandlerTimeout {
		return Config{}, fmt.Errorf("%w: lease duration must exceed handler timeout", ErrInvalidConfig)
	}
	if c.RetryBackoffMax < c.RetryBackoffMin {
		return Config{}, fmt.Errorf("%w: retry backoff max is less than min", ErrInvalidConfig)
	}
	if c.RetryBackoffMax > c.Window {
		return Config{}, fmt.Errorf("%w: retry backoff max must not exceed window", ErrInvalidConfig)
	}
	if c.Metrics == nil {
		c.Metrics = noopMetrics{}
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c, nil
}

func validateType(value string) error {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || len(value) > 255 {
		return fmt.Errorf("%w: task type must be 1-255 bytes of UTF-8", ErrInvalidArgument)
	}
	return nil
}

// volatileData 是 Redis data HASH 中易失任务的内容。
type volatileData struct {
	Type     string `json:"type"`
	Payload  string `json:"payload"`
	Attempts int    `json:"attempts"`
}

func encodeVolatile(task Task, attempts int) (string, error) {
	data, err := sonic.MarshalString(volatileData{Type: task.Type, Payload: task.Payload, Attempts: attempts})
	if err != nil {
		return "", fmt.Errorf("scheduler: encode volatile task: %w", err)
	}
	return data, nil
}

// decodeVolatile 解码易失任务的内容。ID 不在内容里，必须由调用方传入 Redis 成员对应的 ID，
// 否则回调拿不到幂等键，ack / requeue 也无法匹配成员。
func decodeVolatile(id uuid.UUID, data string) (Task, error) {
	var v volatileData
	if err := sonic.UnmarshalString(data, &v); err != nil {
		return Task{}, fmt.Errorf("scheduler: decode volatile task: %w", err)
	}
	if err := validateType(v.Type); err != nil {
		return Task{}, err
	}
	return Task{ID: id, Type: v.Type, Payload: v.Payload, Attempts: v.Attempts, Volatile: true}, nil
}
