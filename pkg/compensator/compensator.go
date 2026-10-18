package compensator

import (
	"context"
	"fmt"
	"math"
	mathrand "math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/project-kgo/kc/pkg/resource"
)

// Compensator 是并发安全的本地任务补偿组件，不持有数据库连接池的生命周期。
type Compensator struct {
	db       *sqlx.DB
	store    storage
	config   resolvedConfig
	handlers map[string]Handler

	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	closed     bool
	closeOnce  sync.Once
	operations sync.WaitGroup
	background sync.WaitGroup

	slots   chan struct{} // worker 容量：已领取（排队或执行中）的任务各占一个
	jobs    chan claimed
	wake    chan struct{}
	backlog atomic.Bool // 上一轮领取满批，worker 空闲时需要立即再扫
}

// New 自动建表并校验表结构；未关闭后台时启动 worker。
// ctx 只用于初始化，组件生命周期由 Close 控制。
func New(ctx context.Context, db *sqlx.DB, config Config, handlers map[string]Handler) (*Compensator, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidConfig)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c, err := resolveConfig(config)
	if err != nil {
		return nil, err
	}
	if len(handlers) == 0 {
		return nil, fmt.Errorf("%w: handlers are empty", ErrInvalidConfig)
	}
	registry := make(map[string]Handler, len(handlers))
	types := make([]string, 0, len(handlers))
	for name, handler := range handlers {
		if err := validateType(name); err != nil {
			return nil, fmt.Errorf("%w: invalid handler type %q", ErrInvalidConfig, name)
		}
		if handler == nil {
			return nil, fmt.Errorf("%w: handler %q is nil", ErrInvalidConfig, name)
		}
		registry[name] = handler
		types = append(types, name)
	}
	store, err := newPGStorage(db, c, types)
	if err != nil {
		return nil, err
	}
	if err = store.ensureSchema(ctx); err != nil {
		return nil, fmt.Errorf("compensator: initialize schema: %w", err)
	}
	return start(ctx, db, store, c, registry)
}

func start(ctx context.Context, db *sqlx.DB, store storage, c resolvedConfig, handlers map[string]Handler) (*Compensator, error) {
	lifecycle, cancel := context.WithCancel(context.WithoutCancel(ctx))
	comp := &Compensator{
		db: db, store: store, config: c, handlers: handlers, ctx: lifecycle, cancel: cancel,
		slots: make(chan struct{}, c.WorkerCount), jobs: make(chan claimed, c.WorkerCount), wake: make(chan struct{}, 1),
	}
	if c.DisableWorker {
		return comp, nil
	}
	comp.background.Add(c.WorkerCount + 2)
	for range c.WorkerCount {
		go comp.worker()
	}
	go comp.scanLoop()
	go comp.cleanupLoop()
	return comp, nil
}

// NewFromResource 从 resource 中获取指定名称的 *sqlx.DB 并创建组件。
func NewFromResource(ctx context.Context, name string, config Config, handlers map[string]Handler) (*Compensator, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%w: resource name is empty", ErrInvalidConfig)
	}
	db, ok := resource.Get[*sqlx.DB](name)
	if !ok || db == nil {
		return nil, fmt.Errorf("%w: %q", ErrResourceNotFound, name)
	}
	return New(ctx, db, config, handlers)
}

// newToken 返回非零随机令牌；0 表示任务未被任何执行者持有。
func newToken() int64 { return mathrand.Int64N(math.MaxInt64) + 1 }

// begin 把公开调用纳入关闭屏障：Close 会等待已开始的调用结束。
func (c *Compensator) begin(ctx context.Context) (func(), error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c == nil {
		return nil, ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	c.operations.Add(1)
	return c.operations.Done, nil
}

// prepare 校验新任务并补齐 ID。业务可预先指定 ID，否则生成时间有序的 UUIDv7。
func (c *Compensator) prepare(task Task) (Task, error) {
	if err := validateType(task.Type); err != nil {
		return Task{}, err
	}
	if _, ok := c.handlers[task.Type]; !ok {
		return Task{}, fmt.Errorf("%w: %q", ErrNoHandler, task.Type)
	}
	if task.Attempt != 0 {
		return Task{}, fmt.Errorf("%w: new task must not set attempt", ErrInvalidArgument)
	}
	if task.ID == uuid.Nil {
		id, err := uuid.NewV7()
		if err != nil {
			return Task{}, fmt.Errorf("compensator: generate ID: %w", err)
		}
		task.ID = id
	}
	return task, nil
}

// Tx 仅在 Transaction 回调期间有效，不得手动提交或回滚 SQLTx。
type Tx struct {
	c      *Compensator
	tx     *sqlx.Tx
	mu     sync.Mutex
	tasks  []Task
	ids    map[uuid.UUID]struct{}
	err    error
	closed bool
}

// SQLTx 返回业务事务，DAO 可直接复用。
func (t *Tx) SQLTx() *sqlx.Tx { return t.tx }

// Add 登记任务并返回任务 ID，任务在业务回调返回后随事务一起写入。
// 返回错误时整个事务不会提交，即使业务忽略了该错误。
func (t *Tx) Add(ctx context.Context, task Task) (uuid.UUID, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return uuid.Nil, fmt.Errorf("%w: transaction callback has finished", ErrInvalidArgument)
	}
	if t.err != nil {
		return uuid.Nil, t.err
	}
	if ctx == nil {
		t.err = fmt.Errorf("%w: context is nil", ErrInvalidArgument)
		return uuid.Nil, t.err
	}
	task, err := t.c.prepare(task)
	if err == nil {
		err = ctx.Err()
	}
	if _, dup := t.ids[task.ID]; err == nil && dup {
		// 提前拒绝，避免提交前才因主键冲突回滚整个业务事务。
		err = fmt.Errorf("%w: duplicate task ID %s", ErrInvalidArgument, task.ID)
	}
	if err != nil {
		t.err = err
		return uuid.Nil, err
	}
	if t.ids == nil {
		t.ids = make(map[uuid.UUID]struct{})
	}
	t.ids[task.ID] = struct{}{}
	t.tasks = append(t.tasks, task)
	return task.ID, nil
}

// close 使 Tx 失效，返回登记的任务和第一个 Add 错误。
func (t *Tx) close() ([]Task, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return t.tasks, t.err
}

// Transaction 在一个事务中执行业务回调，并在同一事务内写入登记的任务。提交后唤醒本节点
// 扫描：有空闲 worker 时任务立即被领取执行，否则由任一节点在 ScanInterval 内领取。
//
// 返回的 error 表示业务失败、登记失败或提交失败，此时没有任何任务生效。
func (c *Compensator) Transaction(ctx context.Context, fn func(tx *Tx) error) error {
	done, err := c.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	if fn == nil {
		return fmt.Errorf("%w: transaction function is nil", ErrInvalidArgument)
	}
	sqlTx, err := c.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("compensator: begin transaction: %w", err)
	}
	defer sqlTx.Rollback()
	bound := &Tx{c: c, tx: sqlTx}
	defer bound.close()
	err = fn(bound)
	tasks, addErr := bound.close()
	if err != nil {
		return err
	}
	if addErr != nil {
		return addErr
	}
	if len(tasks) > 0 {
		if err = c.store.insert(ctx, sqlTx, tasks); err != nil {
			return fmt.Errorf("compensator: insert tasks: %w", err)
		}
	}
	if err = sqlTx.Commit(); err != nil {
		return fmt.Errorf("compensator: commit transaction (outcome may be uncertain): %w", err)
	}
	if len(tasks) > 0 {
		c.notify()
	}
	return nil
}

// AddTx 把任务写入调用方已有的事务并返回任务 ID。提交后任务由任一节点在 ScanInterval
// 内领取执行。tx 必须属于创建组件时使用的同一数据库。
func (c *Compensator) AddTx(ctx context.Context, tx *sqlx.Tx, task Task) (uuid.UUID, error) {
	done, err := c.begin(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	defer done()
	if tx == nil {
		return uuid.Nil, fmt.Errorf("%w: transaction is nil", ErrInvalidArgument)
	}
	if task, err = c.prepare(task); err != nil {
		return uuid.Nil, err
	}
	if err = c.store.insert(ctx, tx, []Task{task}); err != nil {
		return uuid.Nil, fmt.Errorf("compensator: insert task: %w", err)
	}
	return task.ID, nil
}

// Get 查询任务当前状态，状态是观察时刻的快照。
func (c *Compensator) Get(ctx context.Context, id uuid.UUID) (Record, error) {
	done, err := c.begin(ctx)
	if err != nil {
		return Record{}, err
	}
	defer done()
	opCtx, cancel := context.WithTimeout(ctx, c.config.OperationTimeout)
	defer cancel()
	return c.store.get(opCtx, id)
}

// Retry 把失败任务重置为待执行并清零执行次数，任务 ID 不变。
func (c *Compensator) Retry(ctx context.Context, id uuid.UUID) error {
	done, err := c.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	opCtx, cancel := context.WithTimeout(ctx, c.config.OperationTimeout)
	defer cancel()
	if err = c.store.retry(opCtx, id); err != nil {
		return err
	}
	c.notify()
	return nil
}

// Close 停止领取新任务，取消执行中的回调并归还尚未完成的任务，等待已开始的公开调用结束。
// 可重复调用，不会关闭传入的连接池。
func (c *Compensator) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.cancel()
		c.background.Wait()
		c.operations.Wait()
	})
	return nil
}

// notify 唤醒本节点的扫描循环，已有待处理的唤醒时直接返回。
func (c *Compensator) notify() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}
