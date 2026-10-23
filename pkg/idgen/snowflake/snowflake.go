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

// lease 是当前持有的 worker 租约，丢失后整体替换。
type lease struct {
	node     *node
	workerID int64
	token    string
}

// Snowflake 是一个由数据库租约保护的并发安全 ID 生成器。
// 租约丢失后会在后台自动申请新的 worker，期间 Generate 返回 ErrLeaseLost。
type Snowflake struct {
	storage *storage
	lease   atomic.Pointer[lease]

	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	recover chan struct{}

	lastHeartbeat atomic.Pointer[time.Time]
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
	store, err := newStorage(db, resolved)
	if err != nil {
		return nil, err
	}
	if err := store.ensureTable(ctx, resolved.epoch); err != nil {
		return nil, fmt.Errorf("snowflake: ensure worker table %q: %w", resolved.table, err)
	}

	allocated, err := store.allocateWorker(ctx, resolved.epoch)
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
		storage: store,
		ctx:     workerCtx,
		cancel:  cancel,
		done:    make(chan struct{}),
		recover: make(chan struct{}, 1),
	}
	generator.lease.Store(&lease{node: node, workerID: allocated.workerID, token: allocated.leaseToken})
	generator.recordHeartbeat(time.Now())
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
	if s.heartbeatExpired(time.Now()) {
		if s.markLeaseLost("heartbeat expired") {
			// 进程挂起（休眠、断点）恢复后立即重新申请，不必等下一次心跳。
			select {
			case s.recover <- struct{}{}:
			default:
			}
		}
		return 0, ErrLeaseLost
	}
	return s.lease.Load().node.generate()
}

// GetWorkerID 返回当前实例持有的 worker ID。
func (s *Snowflake) GetWorkerID() int64 {
	if s == nil {
		return 0
	}
	return s.lease.Load().workerID
}

// GetTimeFromID 从 ID 中解析 Unix 毫秒时间戳。
func (s *Snowflake) GetTimeFromID(id int64) int64 {
	if s == nil || s.lease.Load() == nil {
		return 0
	}
	return s.lease.Load().node.getTimeFromID(id)
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
		current := s.lease.Load()
		s.closeErr = s.storage.release(ctx, current.workerID, current.token, current.node.retire())
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
		case <-s.recover:
		case <-ticker.C:
		}
		if s.state.Load() == stateActive {
			if err := s.updateHeartbeat(); err != nil {
				if s.ctx.Err() != nil {
					return
				}
				if errors.Is(err, ErrLeaseLost) {
					s.markLeaseLost("lease token replaced")
				} else {
					slog.Error("snowflake heartbeat failed", "worker_id", s.GetWorkerID(), "error", err)
					if s.heartbeatExpired(time.Now()) {
						s.markLeaseLost("heartbeat expired")
					}
				}
			}
		}
		if s.state.Load() == stateLeaseLost {
			s.recoverLease()
		}
	}
}

func (s *Snowflake) markLeaseLost(reason string) bool {
	if !s.state.CompareAndSwap(stateActive, stateLeaseLost) {
		return false
	}
	slog.Warn("snowflake worker lease lost", "worker_id", s.GetWorkerID(), "reason", reason)
	return true
}

// recoverLease 停用旧节点、释放旧租约并申请新 worker；失败时等下一次心跳重试。
func (s *Snowflake) recoverLease() {
	old := s.lease.Load()
	// 先停用旧节点再释放，保证旧 ID 的时间戳都写入了时间屏障；token 不匹配时释放是空操作。
	if err := releaseAllocation(s.storage, allocation{workerID: old.workerID, leaseToken: old.token, lastTimestamp: old.node.retire()}); err != nil {
		slog.Warn("snowflake release lost lease failed", "worker_id", old.workerID, "error", err)
	}
	allocated, err := s.storage.allocateWorker(s.ctx, old.node.epoch)
	if err != nil {
		if s.ctx.Err() == nil {
			slog.Error("snowflake reallocate worker failed", "error", err)
		}
		return
	}
	node, err := newNode(allocated.workerID, old.node.epoch)
	if err == nil {
		err = waitForClock(s.ctx, allocated.lastTimestamp)
	}
	if err != nil {
		if releaseErr := releaseAllocation(s.storage, allocated); releaseErr != nil {
			err = errors.Join(err, releaseErr)
		}
		if s.ctx.Err() == nil {
			slog.Error("snowflake reallocate worker failed", "worker_id", allocated.workerID, "error", err)
		}
		return
	}
	s.lease.Store(&lease{node: node, workerID: allocated.workerID, token: allocated.leaseToken})
	s.recordHeartbeat(time.Now())
	if s.state.CompareAndSwap(stateLeaseLost, stateActive) {
		slog.Info("snowflake worker lease recovered", "old_worker_id", old.workerID, "worker_id", allocated.workerID)
	}
}

func (s *Snowflake) updateHeartbeat() error {
	current := s.lease.Load()
	if err := s.storage.heartbeat(s.ctx, current.workerID, current.token, current.node.latestTimestamp()); err != nil {
		return err
	}
	s.recordHeartbeat(time.Now())
	return nil
}

func waitForClock(ctx context.Context, lastTimestamp int64) error {
	for {
		wait, err := startupClockWait(time.Now().UnixMilli(), lastTimestamp)
		if err != nil {
			return err
		}
		if wait == 0 {
			return nil
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
			// 系统时间可能在等待期间再次变化，必须重新确认已经越过时间屏障。
		}
	}
}

func startupClockWait(now, lastTimestamp int64) (time.Duration, error) {
	if lastTimestamp < now {
		return 0, nil
	}
	rollback := time.Duration(lastTimestamp-now) * time.Millisecond
	if rollback > maxStartupClockRollback {
		return 0, fmt.Errorf("%w by %s", ErrClockBackwards, rollback)
	}
	// 必须进入持久化时间戳的下一毫秒，避免复用 worker 后序列号从 0 重放。
	return rollback + time.Millisecond, nil
}

func (s *Snowflake) recordHeartbeat(now time.Time) {
	value := now
	s.lastHeartbeat.Store(&value)
}

func (s *Snowflake) heartbeatExpired(now time.Time) bool {
	last := s.lastHeartbeat.Load()
	return last == nil || now.Sub(*last) > defaultSafetyThreshold
}

func releaseAllocation(store *storage, allocated allocation) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultCloseTimeout)
	defer cancel()
	return store.release(ctx, allocated.workerID, allocated.leaseToken, allocated.lastTimestamp)
}
