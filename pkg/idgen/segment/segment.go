// Package segment 提供基于数据库号段的分布式 ID 生成器。
//
// 双缓冲算法改编自 github.com/kanengo/ku/distributedx/segment，数据库层针对
// SQLx、PostgreSQL 和 MySQL 做了重新实现。上游代码采用 Apache-2.0 许可证。
package segment

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

const (
	// DefaultStep 是未指定有效步长时使用的默认号段大小。
	DefaultStep        int32 = 1000
	preloadNumerator         = 1
	preloadDenominator       = 5
	asyncLoadTimeout         = 5 * time.Second
)

var (
	// ErrInvalidConfig 表示上下文、配置、业务标签或初始化参数非法。
	ErrInvalidConfig = errors.New("segment: invalid config")
	// ErrResourceNotFound 表示指定名称的 SQL 资源不存在或是 nil。
	ErrResourceNotFound = errors.New("segment: SQL resource not found")
	// ErrClosed 表示生成器已关闭或其父上下文已取消。
	ErrClosed = errors.New("segment: closed")
	// ErrBizTagNotFound 表示数据库中不存在指定业务标签。
	ErrBizTagNotFound = errors.New("segment: biz tag not found")
	// ErrIDOverflow 表示分配新号段会超过 int64 上限。
	ErrIDOverflow = errors.New("segment: ID overflow")
)

// Config 配置号段表。Namespace 为空时使用当前数据库或 PostgreSQL search_path。
type Config struct {
	Namespace string
	TableName string
}

// Segment 描述一个包含首尾边界的已分配号段。
type Segment struct {
	Start   int64
	End     int64
	Current int64
}

type segmentStore interface {
	initTag(context.Context, string, int64, int32) error
	fetchSegment(context.Context, string) (*Segment, error)
}

type segmentBuffer struct {
	bizTag string
	owner  *SegmentIDGen

	ready   chan struct{}
	initErr error
	initMu  sync.Mutex

	mu        sync.Mutex
	current   *Segment
	exhausted bool
	next      *Segment
	loading   bool
	loadDone  chan struct{}
}

const (
	stateActive uint32 = iota
	stateClosed
)

// SegmentIDGen 为多个业务标签维护独立的双缓冲号段。
type SegmentIDGen struct {
	store segmentStore

	ctx    context.Context
	cancel context.CancelFunc
	state  atomic.Uint32

	mu      sync.RWMutex
	buffers map[string]*segmentBuffer

	asyncMu sync.Mutex
	asyncWG sync.WaitGroup

	closeOnce sync.Once
	closeErr  error
}

// New 使用调用方提供的 SQLx 数据库连接创建号段生成器。
// 数据库连接只会被借用，Close 不会关闭它。
func New(ctx context.Context, db *sqlx.DB, config Config) (*SegmentIDGen, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidConfig)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: context is already done: %v", ErrInvalidConfig, err)
	}
	resolved, err := resolveConfig(config)
	if err != nil {
		return nil, err
	}
	store, err := newSQLStorage(db, resolved.table)
	if err != nil {
		return nil, err
	}
	if err := store.ensureTable(ctx); err != nil {
		return nil, fmt.Errorf("segment: ensure table %q: %w", resolved.table, err)
	}
	return newSegmentIDGen(ctx, store), nil
}

// NewFromResource 从 resource 中获取指定名称的 *sqlx.DB 并创建生成器。
func NewFromResource(ctx context.Context, name string, config Config) (*SegmentIDGen, error) {
	if err := validateResourceName(name); err != nil {
		return nil, err
	}
	db, ok := resource.Get[*sqlx.DB](name)
	if !ok || db == nil {
		return nil, fmt.Errorf("%w: %q", ErrResourceNotFound, name)
	}
	return New(ctx, db, config)
}

func newSegmentIDGen(ctx context.Context, store segmentStore) *SegmentIDGen {
	generatorCtx, cancel := context.WithCancel(ctx)
	generator := &SegmentIDGen{
		store:   store,
		ctx:     generatorCtx,
		cancel:  cancel,
		buffers: make(map[string]*segmentBuffer),
	}
	generator.state.Store(stateActive)
	return generator
}

// Init 初始化或更新业务标签。已有标签只更新后续号段的步长，不重置 max_id。
func (g *SegmentIDGen) Init(ctx context.Context, bizTag string, startID int64, step int32) error {
	if g == nil {
		return ErrClosed
	}
	if err := validateBizTag(bizTag); err != nil {
		return err
	}
	if startID < 0 {
		return fmt.Errorf("%w: start ID must not be negative", ErrInvalidConfig)
	}
	if step <= 0 {
		step = DefaultStep
	}

	operationCtx, cancel, err := g.operationContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()

	buffer, creator := g.getOrCreateBuffer(bizTag)
	if creator {
		err := g.initializeBuffer(operationCtx, buffer, startID, step)
		buffer.initErr = err
		close(buffer.ready)
		if err != nil {
			g.removeBuffer(bizTag, buffer)
		}
		return g.normalizeOperationError(err)
	}

	select {
	case <-buffer.ready:
		if buffer.initErr != nil {
			return g.normalizeOperationError(buffer.initErr)
		}
	case <-operationCtx.Done():
		return g.normalizeOperationError(operationCtx.Err())
	}

	// 同一标签的重复 Init 串行更新数据库中的步长。
	buffer.initMu.Lock()
	defer buffer.initMu.Unlock()
	if err := g.store.initTag(operationCtx, bizTag, startID, step); err != nil {
		return g.normalizeOperationError(err)
	}
	return nil
}

// GetID 返回业务标签的下一个 ID。未初始化的标签会以 0/1000 自动初始化。
func (g *SegmentIDGen) GetID(ctx context.Context, bizTag string) (int64, error) {
	if g == nil {
		return 0, ErrClosed
	}
	if err := validateBizTag(bizTag); err != nil {
		return 0, err
	}

	buffer, err := g.ensureDefaultBuffer(ctx, bizTag)
	if err != nil {
		return 0, err
	}

	operationCtx, cancel, err := g.operationContext(ctx)
	if err != nil {
		return 0, err
	}
	defer cancel()

	select {
	case <-buffer.ready:
		if buffer.initErr != nil {
			return 0, g.normalizeOperationError(buffer.initErr)
		}
	case <-operationCtx.Done():
		return 0, g.normalizeOperationError(operationCtx.Err())
	}

	id, err := buffer.nextID(operationCtx)
	if err != nil {
		return 0, g.normalizeOperationError(err)
	}
	return id, nil
}

func (g *SegmentIDGen) ensureDefaultBuffer(ctx context.Context, bizTag string) (*segmentBuffer, error) {
	operationCtx, cancel, err := g.operationContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()

	buffer, creator := g.getOrCreateBuffer(bizTag)
	if creator {
		err := g.initializeBuffer(operationCtx, buffer, 0, DefaultStep)
		buffer.initErr = err
		close(buffer.ready)
		if err != nil {
			g.removeBuffer(bizTag, buffer)
			return nil, g.normalizeOperationError(err)
		}
		return buffer, nil
	}

	select {
	case <-buffer.ready:
		if buffer.initErr != nil {
			return nil, g.normalizeOperationError(buffer.initErr)
		}
		return buffer, nil
	case <-operationCtx.Done():
		return nil, g.normalizeOperationError(operationCtx.Err())
	}
}

// Close 取消后台预加载并等待其退出。重复调用会返回相同结果。
func (g *SegmentIDGen) Close() error {
	if g == nil {
		return nil
	}
	g.closeOnce.Do(func() {
		g.state.Store(stateClosed)
		g.cancel()

		// 与后台任务的 Add 建立互斥屏障，避免 Add 与 Wait 并发。
		g.asyncMu.Lock()
		g.asyncMu.Unlock()
		g.asyncWG.Wait()
	})
	return g.closeErr
}

func (g *SegmentIDGen) initializeBuffer(ctx context.Context, buffer *segmentBuffer, startID int64, step int32) error {
	buffer.initMu.Lock()
	defer buffer.initMu.Unlock()
	if err := g.store.initTag(ctx, buffer.bizTag, startID, step); err != nil {
		return err
	}
	segment, err := g.store.fetchSegment(ctx, buffer.bizTag)
	if err != nil {
		return err
	}
	buffer.mu.Lock()
	buffer.current = segment
	buffer.mu.Unlock()
	return nil
}

func (g *SegmentIDGen) getOrCreateBuffer(bizTag string) (*segmentBuffer, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if buffer := g.buffers[bizTag]; buffer != nil {
		return buffer, false
	}
	buffer := &segmentBuffer{
		bizTag: bizTag,
		owner:  g,
		ready:  make(chan struct{}),
	}
	g.buffers[bizTag] = buffer
	return buffer, true
}

func (g *SegmentIDGen) getBuffer(bizTag string) *segmentBuffer {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.buffers[bizTag]
}

func (g *SegmentIDGen) removeBuffer(bizTag string, expected *segmentBuffer) {
	g.mu.Lock()
	if g.buffers[bizTag] == expected {
		delete(g.buffers, bizTag)
	}
	g.mu.Unlock()
}

func (g *SegmentIDGen) operationContext(parent context.Context) (context.Context, context.CancelFunc, error) {
	if parent == nil {
		return nil, nil, fmt.Errorf("%w: context is nil", ErrInvalidConfig)
	}
	if g.state.Load() != stateActive || g.ctx.Err() != nil {
		return nil, nil, ErrClosed
	}
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(g.ctx, cancel)
	return ctx, func() {
		stop()
		cancel()
	}, nil
}

func (g *SegmentIDGen) normalizeOperationError(err error) error {
	if err == nil {
		return nil
	}
	if g.state.Load() != stateActive || g.ctx.Err() != nil {
		return ErrClosed
	}
	return err
}

func (b *segmentBuffer) nextID(ctx context.Context) (int64, error) {
	for {
		b.mu.Lock()
		if b.current == nil {
			b.mu.Unlock()
			return 0, fmt.Errorf("%w: buffer for %q is not initialized", ErrBizTagNotFound, b.bizTag)
		}

		if b.exhausted || b.current.Current > b.current.End {
			switch {
			case b.next != nil:
				b.current = b.next
				b.exhausted = false
				b.next = nil
			case b.loading:
				loadDone := b.loadDone
				b.mu.Unlock()
				select {
				case <-loadDone:
					continue
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			default:
				// 同步加载时保持 buffer 锁，避免同一进程重复申请号段。
				segment, err := b.owner.store.fetchSegment(ctx, b.bizTag)
				if err != nil {
					b.mu.Unlock()
					return 0, err
				}
				b.current = segment
				b.exhausted = false
			}
		}

		id := b.current.Current
		if id == b.current.End {
			// End 可能是 MaxInt64，使用独立状态避免 Current++ 溢出。
			b.exhausted = true
		} else {
			b.current.Current++
		}
		b.maybeStartPreloadLocked(id)
		b.mu.Unlock()
		return id, nil
	}
}

func (b *segmentBuffer) maybeStartPreloadLocked(lastID int64) {
	if b.loading || b.next != nil {
		return
	}
	total := b.current.End - b.current.Start + 1
	used := lastID - b.current.Start + 1
	if total <= 0 || used*preloadDenominator < total*preloadNumerator {
		return
	}

	b.loading = true
	b.loadDone = make(chan struct{})
	loadDone := b.loadDone
	if b.owner.startAsync(func(ctx context.Context) {
		b.loadNext(ctx, loadDone)
	}) {
		return
	}
	b.loading = false
	close(loadDone)
}

func (g *SegmentIDGen) startAsync(task func(context.Context)) bool {
	g.asyncMu.Lock()
	defer g.asyncMu.Unlock()
	if g.state.Load() != stateActive || g.ctx.Err() != nil {
		return false
	}
	g.asyncWG.Add(1)
	go func() {
		defer g.asyncWG.Done()
		ctx, cancel := context.WithTimeout(g.ctx, asyncLoadTimeout)
		defer cancel()
		task(ctx)
	}()
	return true
}

func (b *segmentBuffer) loadNext(ctx context.Context, loadDone chan struct{}) {
	next, err := b.owner.store.fetchSegment(ctx, b.bizTag)

	b.mu.Lock()
	if b.loading && b.loadDone == loadDone {
		if err == nil {
			b.next = next
		}
		b.loading = false
		close(loadDone)
	}
	b.mu.Unlock()

	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("segment async preload failed", "biz_tag", b.bizTag, "error", err)
	}
}
