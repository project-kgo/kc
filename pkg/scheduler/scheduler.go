package scheduler

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

// operationTimeout 是单次后台数据库或 Redis 操作的超时。
const operationTimeout = 5 * time.Second

// Scheduler 是并发安全的分布式定时任务组件，不持有数据库连接池和 Redis 客户端的生命周期。
type Scheduler struct {
	db       *sqlx.DB
	store    storage
	queue    *queue
	config   Config
	handlers atomic.Pointer[registry] // 写时复制，Register 替换整份快照，读取无锁

	ctx    context.Context // 后台循环的生命周期，Close 时立即取消
	cancel context.CancelFunc
	// handlerCtx 是回调 context 的父级。Close 先等待回调至多 ShutdownTimeout，超时后才取消它。
	handlerCtx    context.Context
	handlerCancel context.CancelFunc
	mu            sync.Mutex
	closed        bool
	closeOnce     sync.Once
	operations    sync.WaitGroup
	background    sync.WaitGroup

	slots      chan struct{} // worker 容量：已弹出（排队或执行中）的任务各占一个
	jobs       chan workItem
	nextBucket atomic.Uint64 // poller 的轮转起始桶
}

// registry 是回调的不可变快照。
type registry struct {
	handlers map[string]Handler
}

func (r *registry) get(taskType string) Handler {
	if r == nil {
		return nil
	}
	return r.handlers[taskType]
}

// with 校验并返回追加了一组回调的新快照，不修改 r。
func (r *registry) with(handlers map[string]Handler) (*registry, error) {
	next := &registry{handlers: make(map[string]Handler, len(handlers))}
	if r != nil {
		maps.Copy(next.handlers, r.handlers)
	}
	for name, handler := range handlers {
		if err := validateType(name); err != nil {
			return nil, fmt.Errorf("invalid handler type %q", name)
		}
		if handler == nil {
			return nil, fmt.Errorf("handler %q is nil", name)
		}
		if _, dup := next.handlers[name]; dup {
			return nil, fmt.Errorf("handler %q already registered", name)
		}
		next.handlers[name] = handler
	}
	return next, nil
}

// New 自动建表并校验表结构，然后启动后台循环。handlers 可为空，之后用 Register 补充；
// ctx 只用于初始化，组件生命周期由 Close 控制。
func New(ctx context.Context, db *sqlx.DB, rdb redis.UniversalClient, config Config, handlers map[string]Handler) (*Scheduler, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidConfig)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	resolved, err := resolveConfig(config)
	if err != nil {
		return nil, err
	}
	reg, err := (*registry)(nil).with(handlers)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	store, err := newPGStorage(db, resolved)
	if err != nil {
		return nil, err
	}
	q, err := newQueue(rdb, resolved)
	if err != nil {
		return nil, err
	}
	if err = store.ensureSchema(ctx); err != nil {
		return nil, fmt.Errorf("scheduler: initialize schema: %w", err)
	}
	// Redis 暂不可用不阻止启动：登记照常成功，触发暂停，对账在恢复后补齐。
	// 脚本在 EVALSHA 未命中时由 queue 重新加载。
	loadCtx, cancel := context.WithTimeout(ctx, operationTimeout)
	if err = q.load(loadCtx); err != nil {
		resolved.Logger.Warn("scheduler preload scripts failed", "error", err)
	}
	cancel()
	return start(ctx, db, store, q, resolved, reg), nil
}

func start(ctx context.Context, db *sqlx.DB, store storage, q *queue, c Config, reg *registry) *Scheduler {
	lifecycle, cancel := context.WithCancel(context.WithoutCancel(ctx))
	handlerCtx, handlerCancel := context.WithCancel(context.WithoutCancel(ctx))
	s := &Scheduler{
		db: db, store: store, queue: q, config: c, ctx: lifecycle, cancel: cancel,
		handlerCtx: handlerCtx, handlerCancel: handlerCancel,
		slots: make(chan struct{}, c.WorkerCount), jobs: make(chan workItem, c.WorkerCount),
	}
	s.handlers.Store(reg)
	s.background.Add(c.WorkerCount + 5)
	for range c.WorkerCount {
		go s.worker()
	}
	go s.pollLoop()
	go s.loadLoop()
	go s.reclaimLoop()
	go s.reconcileLoop()
	go s.cleanLoop()
	return s
}

// Register 在组件运行期间注册一个任务类型的回调，注册后本节点立即开始领取该类型的任务，
// 也可以用它登记该类型的新任务。同一类型只能注册一次，已注册时返回 ErrInvalidArgument。
func (s *Scheduler) Register(taskType string, handler Handler) error {
	return s.RegisterAll(map[string]Handler{taskType: handler})
}

// RegisterAll 原子地注册一组回调：任一类型无效或已注册时全部不生效。
func (s *Scheduler) RegisterAll(handlers map[string]Handler) error {
	if s == nil {
		return ErrClosed
	}
	if len(handlers) == 0 {
		return fmt.Errorf("%w: handlers are empty", ErrInvalidArgument)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	next, err := s.handlers.Load().with(handlers)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	s.handlers.Store(next)
	return nil
}

// begin 把公开调用纳入关闭屏障：Close 会等待已开始的调用结束。
func (s *Scheduler) begin(ctx context.Context) (func(), error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	s.operations.Add(1)
	return s.operations.Done, nil
}

// prepare 校验新任务并补齐 ID。登记时未注册的任务类型返回 ErrNoHandler。
func (s *Scheduler) prepare(task Task) (Task, error) {
	if err := validateType(task.Type); err != nil {
		return Task{}, err
	}
	if s.handlers.Load().get(task.Type) == nil {
		return Task{}, fmt.Errorf("%w: %q", ErrNoHandler, task.Type)
	}
	if task.Attempts != 0 {
		return Task{}, fmt.Errorf("%w: new task must not set attempts", ErrInvalidArgument)
	}
	// timestamptz 能表示更宽的范围，但 RFC3339 只覆盖公元 1..9999 年；
	// 按 UTC 判断并在这里拒绝，避免非法时间拖到 INSERT 时才以数据库错误让整个业务事务回滚。
	if runAt := task.RunAt.UTC(); !task.RunAt.IsZero() && (runAt.Year() < 1 || runAt.Year() > 9999) {
		return Task{}, fmt.Errorf("%w: run at must be between years 1 and 9999", ErrInvalidArgument)
	}
	if task.ID == uuid.Nil {
		id, err := uuid.NewV7()
		if err != nil {
			return Task{}, fmt.Errorf("scheduler: generate ID: %w", err)
		}
		task.ID = id
	}
	return task, nil
}

// delay 返回任务的剩余延迟：零值或过去的时间返回零，表示立即执行。
func (s *Scheduler) delay(runAt time.Time) time.Duration {
	if runAt.IsZero() {
		return 0
	}
	return max(0, time.Until(runAt))
}

func (s *Scheduler) nearTerm(runAt time.Time) bool { return s.delay(runAt) < s.config.Window }

// isVolatile 判断经 Add 登记的任务是否只走 Redis。阈值只对 Add 生效，负值关闭阈值。
func (s *Scheduler) isVolatile(task Task) bool {
	if task.Volatile {
		return true
	}
	if task.Durable || s.config.VolatileBelow < 0 {
		return false
	}
	return s.delay(task.RunAt) < s.config.VolatileBelow
}

func (s *Scheduler) validateVolatile(task Task) error {
	if len(task.Payload) > s.config.VolatileMaxPayload {
		return fmt.Errorf("%w: volatile payload exceeds %d bytes", ErrInvalidArgument, s.config.VolatileMaxPayload)
	}
	if s.delay(task.RunAt) > s.config.Window {
		return fmt.Errorf("%w: volatile task delay exceeds window", ErrInvalidArgument)
	}
	return nil
}

// volatileFor 决定经 Add 登记的任务是否走易失路径。显式 Volatile 的任务不满足易失条件时
// 返回 ErrInvalidArgument；只是被 VolatileBelow 阈值自动判定的任务则回落为持久任务，
// 调用方没有要求易失，不应因此登记失败。
func (s *Scheduler) volatileFor(task Task) (bool, error) {
	if !s.isVolatile(task) {
		return false, nil
	}
	if err := s.validateVolatile(task); err != nil {
		if task.Volatile {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

// Add 登记独立任务并返回任务 ID。延迟小于 VolatileBelow 的任务默认只写 Redis（易失任务），
// 可设置 Task.Durable = true 排除在阈值之外；被阈值自动判定但 payload 超过 VolatileMaxPayload
// 或延迟超过 Window 的任务回落为持久任务。易失任务的 Redis 写入失败时直接返回错误。
func (s *Scheduler) Add(ctx context.Context, task Task) (uuid.UUID, error) {
	done, err := s.begin(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	defer done()
	if task, err = s.prepare(task); err != nil {
		return uuid.Nil, err
	}
	volatile, err := s.volatileFor(task)
	if err != nil {
		return uuid.Nil, err
	}
	if volatile {
		if err = s.addVolatile(ctx, task); err != nil {
			return uuid.Nil, err
		}
		return task.ID, nil
	}
	status := StatusPending
	if s.nearTerm(task.RunAt) {
		status = StatusQueued
	}
	queued, err := s.store.insert(ctx, s.db, []insertTask{{task: task, status: status}})
	if err != nil {
		return uuid.Nil, err
	}
	s.config.Metrics.Registered(task.Type, false)
	if len(queued) > 0 {
		// 入队失败不回滚、不报错：任务已是 queued，由 reconciler 在逾期后补回。
		if _, err = s.enqueue(context.WithoutCancel(ctx), queued); err != nil {
			s.config.Logger.Error("scheduler enqueue task failed", "task_id", task.ID, "error", err)
		}
	}
	return task.ID, nil
}

func (s *Scheduler) addVolatile(ctx context.Context, task Task) error {
	data, err := encodeVolatile(task, 0)
	if err != nil {
		return err
	}
	if err = s.queue.enqueueVolatile(ctx, task.ID, s.delay(task.RunAt), data); err != nil {
		return fmt.Errorf("scheduler: enqueue volatile task: %w", err)
	}
	s.config.Metrics.Registered(task.Type, true)
	s.config.Metrics.Enqueued(1, true)
	return nil
}

// Tx 仅在 Transaction 回调期间有效，不得手动提交或回滚 SQLTx。
type Tx struct {
	s      *Scheduler
	tx     *sqlx.Tx
	mu     sync.Mutex
	tasks  []txTask
	ids    map[uuid.UUID]struct{}
	err    error
	closed bool
}

type txTask struct {
	task Task
	near bool
}

// SQLTx 返回业务事务，DAO 可直接复用。
func (t *Tx) SQLTx() *sqlx.Tx { return t.tx }

// Add 登记任务并返回任务 ID。任务在业务回调返回后随事务一起写入；事务中登记的任务始终是
// 持久任务，Volatile 为 true 时返回 ErrInvalidArgument。
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
	task, err := t.s.prepare(task)
	if err == nil && task.Volatile {
		err = fmt.Errorf("%w: volatile task is not allowed in a transaction", ErrInvalidArgument)
	}
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
	t.tasks = append(t.tasks, txTask{task: task, near: t.s.nearTerm(task.RunAt)})
	return task.ID, nil
}

// close 使 Tx 失效，返回登记的任务和第一个 Add 错误。
func (t *Tx) close() ([]txTask, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return t.tasks, t.err
}

// Transaction 在一个事务中执行业务回调，并在同一事务内写入登记的任务。提交后立即把近期
// 任务写入 Redis；入队失败不回滚，由 reconciler 补回。
func (s *Scheduler) Transaction(ctx context.Context, fn func(tx *Tx) error) error {
	done, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	if fn == nil {
		return fmt.Errorf("%w: transaction function is nil", ErrInvalidArgument)
	}
	sqlTx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("scheduler: begin transaction: %w", err)
	}
	defer sqlTx.Rollback()
	bound := &Tx{s: s, tx: sqlTx}
	defer bound.close()
	err = fn(bound)
	tasks, addErr := bound.close()
	if err != nil {
		return err
	}
	if addErr != nil {
		return addErr
	}
	var queued []queuedTask
	if len(tasks) > 0 {
		inserts := make([]insertTask, len(tasks))
		for i, task := range tasks {
			status := StatusPending
			if task.near {
				status = StatusQueued
			}
			inserts[i] = insertTask{task: task.task, status: status}
		}
		if queued, err = s.store.insert(ctx, sqlTx, inserts); err != nil {
			return fmt.Errorf("scheduler: insert tasks: %w", err)
		}
	}
	if err = sqlTx.Commit(); err != nil {
		return fmt.Errorf("scheduler: commit transaction (outcome may be uncertain): %w", err)
	}
	for _, task := range tasks {
		s.config.Metrics.Registered(task.task.Type, false)
	}
	if len(queued) > 0 {
		if _, err = s.enqueue(context.WithoutCancel(ctx), queued); err != nil {
			s.config.Logger.Error("scheduler enqueue transaction tasks failed", "count", len(queued), "error", err)
		}
	}
	return nil
}

// AddTx 把任务写入调用方已有的事务。任务以 pending 写入，由 loader 在 LoadInterval 内
// 入队；Volatile 为 true 时返回 ErrInvalidArgument。tx 必须属于组件使用的同一数据库。
func (s *Scheduler) AddTx(ctx context.Context, tx *sqlx.Tx, task Task) (uuid.UUID, error) {
	done, err := s.begin(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	defer done()
	if tx == nil {
		return uuid.Nil, fmt.Errorf("%w: transaction is nil", ErrInvalidArgument)
	}
	if task, err = s.prepare(task); err != nil {
		return uuid.Nil, err
	}
	if task.Volatile {
		return uuid.Nil, fmt.Errorf("%w: volatile task is not allowed in a transaction", ErrInvalidArgument)
	}
	if _, err = s.store.insert(ctx, tx, []insertTask{{task: task, status: StatusPending}}); err != nil {
		return uuid.Nil, fmt.Errorf("scheduler: insert task: %w", err)
	}
	s.config.Metrics.Registered(task.Type, false)
	return task.ID, nil
}

// Get 查询任务的当前状态。易失任务不在 PostgreSQL 中，返回 ErrNotFound。
func (s *Scheduler) Get(ctx context.Context, id uuid.UUID) (Record, error) {
	done, err := s.begin(ctx)
	if err != nil {
		return Record{}, err
	}
	defer done()
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	return s.store.get(ctx, id)
}

// Cancel 取消尚未开始执行的任务。任务不存在返回 ErrNotFound，已是终态返回 ErrFinished。
// 已经开始执行的回调不会被中断，回调结束后状态保持 cancelled。
func (s *Scheduler) Cancel(ctx context.Context, id uuid.UUID) error {
	done, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	err = s.store.cancel(ctx, id)
	switch {
	case err == nil:
		// 尽力从 ready 删除；已经在执行的任务不打断，状态保持 cancelled。
		if _, qerr := s.queue.cancel(ctx, id); qerr != nil {
			s.config.Logger.Warn("scheduler remove cancelled task from redis failed", "task_id", id, "error", qerr)
		}
		s.config.Metrics.Cancelled(1)
		return nil
	case errors.Is(err, ErrNotFound):
		// PostgreSQL 中没有该 ID，可能是易失任务。
		ok, qerr := s.queue.cancel(ctx, id)
		if qerr != nil {
			return qerr
		}
		if ok {
			s.config.Metrics.Cancelled(1)
			return nil
		}
		return ErrNotFound
	default:
		return err
	}
}

// Retry 把失败任务重置为 pending，由 loader 重新入队。易失任务返回 ErrNotFound。
func (s *Scheduler) Retry(ctx context.Context, id uuid.UUID) error {
	done, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	return s.store.retry(ctx, id)
}

// Close 停止后台循环，等待执行中的回调至多 ShutdownTimeout。等待期间回调的 context 不会被
// 取消；超时后才取消，被中断的任务立即放回且不计尝试次数。忽略取消的回调靠租约回收。
// 可重复调用，不会关闭传入的连接池和 Redis 客户端。
func (s *Scheduler) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.cancel()
		if !waitTimeout(&s.background, s.config.ShutdownTimeout) {
			// 回调超时仍未结束：取消它们的 context，并留出一次操作的时间把任务放回。
			s.handlerCancel()
			waitTimeout(&s.background, operationTimeout)
		}
		s.handlerCancel()
		s.operations.Wait()
	})
	return nil
}

// waitTimeout 最多等待 d，返回 false 表示超时。超时后保留后台 goroutine，由进程退出或
// 租约回收兜底。
func waitTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// enqueue 把已标记为 queued 的任务写入 Redis；失败时调用方只记录日志，由 reconciler 补回。
func (s *Scheduler) enqueue(ctx context.Context, tasks []queuedTask) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	n, err := s.queue.enqueue(ctx, tasks)
	if err != nil {
		return 0, err
	}
	s.config.Metrics.Enqueued(n, false)
	return n, nil
}

// pause 等待下一个间隔并对间隔加 10% 随机抖动，避免多节点同步；返回 false 表示组件已关闭。
func (s *Scheduler) pause(interval time.Duration) bool {
	timer := time.NewTimer(jitter(interval))
	defer timer.Stop()
	select {
	case <-s.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
