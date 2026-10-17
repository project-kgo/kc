package compensator

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	mathrand "math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	nodeID   string
	// ownToken 是本进程在 AfterCommit 模式登记任务时持有的初始租约令牌。
	ownToken int64

	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	closed     bool
	closeOnce  sync.Once
	closeErr   error
	operations sync.WaitGroup
	background sync.WaitGroup

	slots   chan struct{} // 后台 worker 容量：已领取（排队或执行中）的任务各占一个
	jobs    chan claimed
	wake    chan struct{}
	backlog atomic.Bool // 上一轮领取满批，worker 空闲时需要立即再扫
	cursor  atomic.Uint64

	shardsMu    sync.RWMutex
	shards      []int16
	shardsUntil time.Time
}

// New 自动建表并校验表结构；未关闭后台时注册节点、领取分片并启动 worker。
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
	for name, handler := range handlers {
		if err := validateType(name); err != nil {
			return nil, fmt.Errorf("%w: invalid handler type %q", ErrInvalidConfig, name)
		}
		if handler == nil {
			return nil, fmt.Errorf("%w: handler %q is nil", ErrInvalidConfig, name)
		}
		registry[name] = handler
	}
	store, err := newPGStorage(db, c)
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
		db: db, store: store, config: c, handlers: handlers, nodeID: rand.Text(), ownToken: newToken(),
		ctx: lifecycle, cancel: cancel,
		slots: make(chan struct{}, c.WorkerCount), jobs: make(chan claimed, c.WorkerCount), wake: make(chan struct{}, 1),
	}
	if c.DisableWorker {
		return comp, nil
	}
	if err := comp.refresh(ctx); err != nil {
		cancel()
		releaseCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), c.OperationTimeout)
		defer stop()
		_ = store.release(releaseCtx, comp.nodeID)
		return nil, fmt.Errorf("compensator: register node: %w", err)
	}
	comp.background.Add(c.WorkerCount + 3)
	for range c.WorkerCount {
		go comp.worker()
	}
	go comp.scanLoop()
	go comp.coordinateLoop()
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

// Add 登记任务并返回任务 ID。任务在业务回调返回后、提交前统一执行首次回调并写入。
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
		// 提前拒绝，避免首次回调都执行后才因主键冲突回滚整个事务。
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

// Transaction 在一个事务中执行业务回调并写入登记的任务。
//
// ModeInTx：业务回调成功后，在事务内执行各任务的首次回调，再用一条多行 INSERT 写入，
// 回调成功的任务直接为成功状态，失败的等待后台补偿。注意回调已产生的外部副作用
// 不会因提交失败而撤销，回调须幂等。
// ModeAfterCommit：任务以本进程持有租约的状态写入，提交后立即执行回调并落库结果。
//
// 返回的 error 只表示业务失败或提交失败（此时没有任何任务生效）；首次回调失败放在
// 对应 Result.Err 中，调用方不应据此重做业务。
func (c *Compensator) Transaction(ctx context.Context, fn func(tx *Tx) error) ([]Result, error) {
	done, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if fn == nil {
		return nil, fmt.Errorf("%w: transaction function is nil", ErrInvalidArgument)
	}
	sqlTx, err := c.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("compensator: begin transaction: %w", err)
	}
	defer sqlTx.Rollback()
	bound := &Tx{c: c, tx: sqlTx}
	defer func() { bound.mu.Lock(); bound.closed = true; bound.mu.Unlock() }()
	err = fn(bound)
	bound.mu.Lock()
	bound.closed = true
	tasks, addErr := bound.tasks, bound.err
	bound.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if addErr != nil {
		return nil, addErr
	}
	var results []Result
	// 托管事务使用独立令牌：Dispatch 只认 ownToken，不会抢走本事务提交后正在执行的任务。
	token := newToken()
	if len(tasks) > 0 {
		var rows []newRow
		rows, results = c.firstRun(ctx, tasks, token)
		if err = c.store.insert(ctx, sqlTx, rows); err != nil {
			return nil, fmt.Errorf("compensator: insert tasks: %w", err)
		}
	}
	if err = sqlTx.Commit(); err != nil {
		return nil, fmt.Errorf("compensator: commit transaction (outcome may be uncertain): %w", err)
	}
	if c.config.Mode == ModeAfterCommit && len(tasks) > 0 {
		owned := make([]claimed, len(tasks))
		for i, task := range tasks {
			task.Attempt = 1
			owned[i] = claimed{Task: task, token: token}
		}
		results = c.runAll(ctx, owned, SourceAfterCommit)
	}
	return results, nil
}

// AddTx 把任务写入调用方已有的事务。ModeInTx 下立即执行首次回调并按结果写入；
// ModeAfterCommit 下只写入，调用方须在提交成功后调用 Dispatch，遗漏时租约到期后由后台补偿。
// tx 必须属于创建组件时使用的同一数据库。
func (c *Compensator) AddTx(ctx context.Context, tx *sqlx.Tx, task Task) (Result, error) {
	done, err := c.begin(ctx)
	if err != nil {
		return Result{}, err
	}
	defer done()
	if tx == nil {
		return Result{}, fmt.Errorf("%w: transaction is nil", ErrInvalidArgument)
	}
	if task, err = c.prepare(task); err != nil {
		return Result{}, err
	}
	rows, results := c.firstRun(ctx, []Task{task}, c.ownToken)
	if err = c.store.insert(ctx, tx, rows); err != nil {
		return Result{}, fmt.Errorf("compensator: insert task: %w", err)
	}
	return results[0], nil
}

// firstRun 生成待插入的行。ModeInTx 下并发执行各任务的首次回调，按结果决定初始状态；
// ModeAfterCommit 下任务以 running 状态写入并由 token 持有租约。
func (c *Compensator) firstRun(ctx context.Context, tasks []Task, token int64) ([]newRow, []Result) {
	rows := make([]newRow, len(tasks))
	results := make([]Result, len(tasks))
	build := func(i int) {
		task := tasks[i]
		row := newRow{id: task.ID, taskType: task.Type, payload: task.Payload, shard: int16(mathrand.IntN(shardCount)), attempts: 1}
		if c.config.Mode == ModeAfterCommit {
			row.status, row.delay, row.token = StatusRunning, c.config.LeaseDuration, token
			rows[i], results[i] = row, Result{ID: task.ID, Status: StatusRunning}
			return
		}
		task.Attempt = 1
		started := time.Now()
		err := c.invoke(ctx, &task)
		var lastError string
		row.status, lastError, row.delay = c.outcome(1, err)
		row.err = lastError
		// 首次执行的结果随业务事务提交，事务失败时该事件对应的记录不会存在。
		c.config.Metrics.Executed(ExecEvent{Type: task.Type, Source: SourceFirstRun, Attempt: 1, Status: row.status, Duration: time.Since(started), Err: err})
		rows[i], results[i] = row, Result{ID: task.ID, Status: row.status, Err: err}
	}
	if c.config.Mode == ModeAfterCommit {
		for i := range tasks {
			build(i)
		}
		return rows, results
	}
	c.parallel(len(tasks), build)
	return rows, results
}

// parallel 以至多 WorkerCount 的并发执行 fn(0..n-1)，避免单个大事务同时打满下游。
func (c *Compensator) parallel(n int, fn func(int)) {
	if n == 1 {
		fn(0)
		return
	}
	limit := make(chan struct{}, c.config.WorkerCount)
	var wg sync.WaitGroup
	for i := range n {
		limit <- struct{}{}
		wg.Go(func() {
			defer func() { <-limit }()
			fn(i)
		})
	}
	wg.Wait()
}

// Dispatch 立即尝试执行指定任务：本进程以 AfterCommit 登记且尚未执行的任务，
// 或已到期的待重试任务。正在被其它执行者处理、未到期或已终态的任务不会重复执行，
// 其 Result 为查询到的当前状态。
func (c *Compensator) Dispatch(ctx context.Context, ids ...uuid.UUID) ([]Result, error) {
	done, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	seen := make(map[uuid.UUID]bool, len(ids))
	for _, id := range ids {
		if id == uuid.Nil || seen[id] {
			return nil, fmt.Errorf("%w: IDs must be non-nil and unique", ErrInvalidArgument)
		}
		seen[id] = true
	}
	opCtx, cancel := context.WithTimeout(ctx, c.config.OperationTimeout)
	tasks, err := c.store.claimIDs(opCtx, ids, c.ownToken, newToken())
	cancel()
	if err != nil {
		return nil, fmt.Errorf("compensator: claim tasks: %w", err)
	}
	tasks = c.runnable(tasks, SourceDispatch)
	byID := make(map[uuid.UUID]Result, len(tasks))
	for _, r := range c.runAll(ctx, tasks, SourceDispatch) {
		byID[r.ID] = r
	}
	var missing []uuid.UUID
	for _, id := range ids {
		if _, ok := byID[id]; !ok {
			missing = append(missing, id)
		}
	}
	var statuses map[uuid.UUID]Status
	var lookupErr error
	if len(missing) > 0 {
		opCtx, cancel := context.WithTimeout(ctx, c.config.OperationTimeout)
		statuses, lookupErr = c.store.statuses(opCtx, missing)
		cancel()
	}
	results := make([]Result, len(ids))
	for i, id := range ids {
		if r, ok := byID[id]; ok {
			results[i] = r
			continue
		}
		status, ok := statuses[id]
		switch {
		case lookupErr != nil:
			results[i] = Result{ID: id, Err: lookupErr}
		case !ok:
			results[i] = Result{ID: id, Err: ErrNotFound}
		default:
			results[i] = Result{ID: id, Status: status}
		}
	}
	return results, nil
}

// runnable 过滤掉领取时因预算耗尽被标记为 failed 的任务，并为它们记录日志和指标。
func (c *Compensator) runnable(tasks []claimed, source Source) []claimed {
	result := tasks[:0:0]
	for _, t := range tasks {
		if !t.exhausted {
			result = append(result, t)
			continue
		}
		err := errors.New("retry budget exhausted after lease expiration")
		c.config.Metrics.Executed(ExecEvent{Type: t.Type, Source: source, Attempt: t.Attempt, Status: StatusFailed, Err: err})
		c.config.Logger.Warn("compensator task failed", "task_id", t.ID, "type", t.Type, "attempt", t.Attempt, "error", err)
	}
	return result
}

func (c *Compensator) runAll(ctx context.Context, tasks []claimed, source Source) []Result {
	results := make([]Result, len(tasks))
	c.parallel(len(tasks), func(i int) { results[i] = c.execute(ctx, tasks[i], source, false) })
	return results
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

// Close 停止领取新任务，取消后台回调并等待已开始的公开调用结束，最后释放分片租约，
// 使其它节点可以立即接管。可重复调用，不会关闭传入的连接池。
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
		if c.config.DisableWorker {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.config.OperationTimeout)
		defer cancel()
		c.closeErr = c.store.release(ctx, c.nodeID)
	})
	return c.closeErr
}

// execute 执行回调并按租约令牌落库结果。abandonOnClose 为 true 时，
// 因组件关闭而中断的执行会归还任务且不消耗次数。
func (c *Compensator) execute(ctx context.Context, t claimed, source Source, abandonOnClose bool) Result {
	started := time.Now()
	err := c.invoke(ctx, &t.Task)
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.config.OperationTimeout)
	defer cancel()
	if abandonOnClose && errors.Is(err, context.Canceled) && c.ctx.Err() != nil {
		if abandonErr := c.store.abandon(finishCtx, t); abandonErr != nil {
			c.config.Logger.Warn("compensator abandon task failed", "task_id", t.ID, "error", abandonErr)
		}
		return Result{ID: t.ID, Status: StatusPending, Err: err}
	}
	status, lastError, delay := c.outcome(t.Attempt, err)
	finishErr := c.store.finish(finishCtx, t, status, lastError, delay)
	if finishErr != nil {
		status = StatusRunning
	}
	c.config.Metrics.Executed(ExecEvent{Type: t.Type, Source: source, Attempt: t.Attempt, Status: status, Duration: time.Since(started), Err: errors.Join(err, finishErr)})
	if err != nil || finishErr != nil {
		c.config.Logger.Warn("compensator task failed", "task_id", t.ID, "type", t.Type, "attempt", t.Attempt,
			"duration", time.Since(started), "error", errors.Join(err, finishErr))
	}
	return Result{ID: t.ID, Status: status, Err: errors.Join(err, finishErr)}
}

// outcome 根据执行结果决定状态、错误文本和下次执行延迟。
func (c *Compensator) outcome(attempt int, err error) (Status, string, time.Duration) {
	if err == nil {
		return StatusSucceeded, "", 0
	}
	if attempt >= c.config.retries+1 {
		return StatusFailed, errorText(err), 0
	}
	return StatusPending, errorText(err), c.backoff(attempt)
}

// invoke 以 HandlerTimeout 调用回调，panic 转为错误，并传递任务副本。
func (c *Compensator) invoke(ctx context.Context, task *Task) (err error) {
	handler := c.handlers[task.Type]
	if handler == nil {
		return fmt.Errorf("%w: %q", ErrNoHandler, task.Type)
	}
	ctx, cancel := context.WithTimeout(ctx, c.config.HandlerTimeout)
	defer cancel()
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("%w: %v", ErrHandlerPanic, value)
		}
	}()
	if err = ctx.Err(); err != nil {
		return err
	}
	copied := *task
	if err = handler(ctx, &copied); err == nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		// 回调忽略超时并返回 nil 时按超时处理，避免租约可能已失效的结果被当作成功；
		// 仅被取消（如组件关闭）而回调已成功返回时仍记为成功，避免重复执行。
		err = ctx.Err()
	}
	return err
}

func (c *Compensator) notify() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}
