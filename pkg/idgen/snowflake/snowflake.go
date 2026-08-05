// Package snowflake 提供由 SQL 租约协调 worker ID 的分布式 Snowflake ID 生成器。
//
// ID 位布局与 github.com/kanengo/ku 的 snowflakex 实现一致；数据库协调层针对
// sqlx、PostgreSQL 和 MySQL 做了重新实现。上游代码采用 Apache-2.0 许可证。
package snowflake

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/project-kgo/kc/pkg/resource"
)

var (
	// ErrInvalidConfig 表示配置、上下文或数据库对象无效。
	ErrInvalidConfig = errors.New("snowflake: invalid config")
	// ErrResourceNotFound 表示指定名称的 SQL 资源不存在或是 nil。
	ErrResourceNotFound = errors.New("snowflake: SQL resource not found")
	// ErrClosed 表示生成器已经关闭或其父上下文已经取消。
	ErrClosed = errors.New("snowflake: closed")
	// ErrLeaseLost 表示数据库租约已丢失，继续生成可能导致 ID 冲突。
	ErrLeaseLost = errors.New("snowflake: worker lease lost")
	// ErrClockBackwards 表示系统时钟发生回拨。
	ErrClockBackwards = errors.New("snowflake: clock moved backwards")
	// ErrNoWorkerID 表示 0-1023 均已被占用且没有过期租约。
	ErrNoWorkerID = errors.New("snowflake: no available worker ID")
	// ErrTimestampOverflow 表示 41 位时间戳空间已经耗尽。
	ErrTimestampOverflow = errors.New("snowflake: timestamp overflow")
)

// Config 配置 worker 表及 Snowflake 纪元。
// Namespace 为空时使用连接当前数据库或 PostgreSQL search_path。
type Config struct {
	Namespace string
	TableName string
	Epoch     int64
}

const (
	stateActive uint32 = iota
	stateClosed
	stateLeaseLost
)

// Snowflake 是一个由数据库租约保护的并发安全 ID 生成器。
type Snowflake struct {
	node       *node
	storage    *storage
	workerID   int64
	leaseToken string

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	lastHeartbeat atomic.Int64
	state         atomic.Uint32
	closeOnce     sync.Once
	closeErr      error
}

// New 使用调用方提供的 sqlx 数据库连接创建生成器。
// 数据库连接只会被借用，Close 不会关闭它。
func New(ctx context.Context, db *sqlx.DB, config Config) (*Snowflake, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidConfig)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: context is already done: %v", ErrInvalidConfig, err)
	}
	resolved, err := resolveConfig(config, time.Now())
	if err != nil {
		return nil, err
	}
	store, err := newStorage(db, resolved.table)
	if err != nil {
		return nil, err
	}
	if err := store.ensureTable(ctx); err != nil {
		return nil, fmt.Errorf("snowflake: ensure worker table %q: %w", resolved.table, err)
	}

	allocated, err := store.allocateWorker(ctx)
	if err != nil {
		return nil, fmt.Errorf("snowflake: allocate worker: %w", err)
	}
	if err := waitForClock(ctx, allocated.lastTimestamp); err != nil {
		releaseErr := releaseAllocation(store, allocated)
		return nil, errors.Join(err, releaseErr)
	}

	node, err := newNode(allocated.workerID, resolved.epoch)
	if err != nil {
		releaseErr := releaseAllocation(store, allocated)
		return nil, errors.Join(err, releaseErr)
	}

	workerCtx, cancel := context.WithCancel(ctx)
	generator := &Snowflake{
		node:       node,
		storage:    store,
		workerID:   allocated.workerID,
		leaseToken: allocated.leaseToken,
		ctx:        workerCtx,
		cancel:     cancel,
		done:       make(chan struct{}),
	}
	generator.lastHeartbeat.Store(time.Now().UnixNano())
	generator.state.Store(stateActive)
	go generator.heartbeatLoop()
	return generator, nil
}

// NewFromResource 从 resource 中获取指定名称的 *sqlx.DB 并创建生成器。
func NewFromResource(ctx context.Context, name string, config Config) (*Snowflake, error) {
	if err := validateResourceName(name); err != nil {
		return nil, err
	}
	db, ok := resource.Get[*sqlx.DB](name)
	if !ok || db == nil {
		return nil, fmt.Errorf("%w: %q", ErrResourceNotFound, name)
	}
	return New(ctx, db, config)
}

// Generate 生成一个新的 ID。租约不可确认时不会继续生成。
func (s *Snowflake) Generate() (int64, error) {
	if s == nil {
		return 0, ErrClosed
	}
	switch s.state.Load() {
	case stateClosed:
		return 0, ErrClosed
	case stateLeaseLost:
		return 0, ErrLeaseLost
	}
	if s.ctx.Err() != nil {
		return 0, ErrClosed
	}
	last := time.Unix(0, s.lastHeartbeat.Load())
	if time.Since(last) > defaultSafetyThreshold {
		s.state.CompareAndSwap(stateActive, stateLeaseLost)
		return 0, ErrLeaseLost
	}
	return s.node.generate()
}

// GetWorkerID 返回当前实例持有的 worker ID。
func (s *Snowflake) GetWorkerID() int64 {
	if s == nil {
		return 0
	}
	return s.workerID
}

// GetTimeFromID 从 ID 中解析 Unix 毫秒时间戳。
func (s *Snowflake) GetTimeFromID(id int64) int64 {
	if s == nil || s.node == nil {
		return 0
	}
	return s.node.getTimeFromID(id)
}

// GetTime 从 ID 中解析生成时间。
func (s *Snowflake) GetTime(id int64) time.Time {
	return time.UnixMilli(s.GetTimeFromID(id))
}

// Close 停止心跳并释放 worker 租约。重复调用会返回相同结果。
// Close 不会关闭传入的数据库连接。
func (s *Snowflake) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.state.Store(stateClosed)
		s.cancel()
		<-s.done

		ctx, cancel := context.WithTimeout(context.Background(), defaultCloseTimeout)
		defer cancel()
		s.closeErr = s.storage.release(ctx, s.workerID, s.leaseToken, s.node.latestTimestamp())
	})
	return s.closeErr
}

func (s *Snowflake) heartbeatLoop() {
	defer close(s.done)
	ticker := time.NewTicker(defaultHeartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := s.updateHeartbeat(); err != nil {
				if errors.Is(err, ErrLeaseLost) {
					s.state.CompareAndSwap(stateActive, stateLeaseLost)
					return
				}
				slog.Error("snowflake heartbeat failed", "worker_id", s.workerID, "error", err)
				last := time.Unix(0, s.lastHeartbeat.Load())
				if time.Since(last) > defaultSafetyThreshold {
					s.state.CompareAndSwap(stateActive, stateLeaseLost)
					return
				}
			}
		}
	}
}

func (s *Snowflake) updateHeartbeat() error {
	if err := s.storage.heartbeat(s.ctx, s.workerID, s.leaseToken, s.node.latestTimestamp()); err != nil {
		return err
	}
	s.lastHeartbeat.Store(time.Now().UnixNano())
	return nil
}

func waitForClock(ctx context.Context, lastTimestamp int64) error {
	now := time.Now().UnixMilli()
	if lastTimestamp <= now {
		return nil
	}
	wait := time.Duration(lastTimestamp-now) * time.Millisecond
	if wait > maxStartupClockRollback {
		return fmt.Errorf("%w by %s", ErrClockBackwards, wait)
	}
	timer := time.NewTimer(wait + time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func releaseAllocation(store *storage, allocated allocation) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultCloseTimeout)
	defer cancel()
	return store.release(ctx, allocated.workerID, allocated.leaseToken, allocated.lastTimestamp)
}
