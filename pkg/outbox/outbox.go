package outbox

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/project-kgo/kc/pkg/resource"
)

// Outbox 是并发安全的本地消息组件，不持有数据库连接池的生命周期。
type Outbox struct {
	db           *sqlx.DB
	store        storage
	config       resolvedConfig
	handlers     map[string]Handler
	nodeID       string
	ctx          context.Context
	cancel       context.CancelFunc
	slots        chan struct{}
	jobs         chan task
	wake         chan struct{}
	background   sync.WaitGroup
	operations   sync.WaitGroup
	mu           sync.Mutex
	closed       bool
	closeOnce    sync.Once
	closeDone    chan struct{}
	closeErr     error
	coordinateMu sync.Mutex
	leasesMu     sync.RWMutex
	leases       []shardLease
	cursor       atomic.Uint64
	lastCleanup  atomic.Int64
}

// New 自动建表；开启后台模式时注册节点并启动固定数量的回调 worker。
func New(ctx context.Context, db *sqlx.DB, config Config, handlers map[string]Handler) (*Outbox, error) {
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
	registry := make(map[string]Handler, len(handlers))
	if len(handlers) == 0 {
		return nil, fmt.Errorf("%w: handlers are empty", ErrInvalidConfig)
	}
	for name, handler := range handlers {
		if err := validateType(name); err != nil {
			return nil, fmt.Errorf("%w: invalid handler type", ErrInvalidConfig)
		}
		if handler == nil {
			return nil, fmt.Errorf("%w: handler %q is nil", ErrInvalidConfig, name)
		}
		registry[name] = handler
	}
	store, err := newSQLStorage(db, c)
	if err != nil {
		return nil, err
	}
	if err = store.ensureSchema(ctx); err != nil {
		return nil, fmt.Errorf("outbox: initialize schema: %w", err)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	o := &Outbox{db: db, store: store, config: c, handlers: registry, nodeID: rand.Text(), ctx: workerCtx, cancel: cancel,
		slots: make(chan struct{}, c.WorkerCount), jobs: make(chan task, c.WorkerCount), wake: make(chan struct{}, 1), closeDone: make(chan struct{})}
	if !c.DisableBackground {
		if err = o.refresh(workerCtx); err != nil {
			cancel()
			releaseCtx, stop := context.WithTimeout(context.Background(), c.OperationTimeout)
			defer stop()
			_ = store.release(releaseCtx, o.nodeID)
			return nil, fmt.Errorf("outbox: register node: %w", err)
		}
		o.background.Add(c.WorkerCount + 2)
		for range c.WorkerCount {
			go o.worker()
		}
		go o.coordinateLoop()
		go o.scanLoop()
	}
	return o, nil
}

func NewFromResource(ctx context.Context, name string, c Config, handlers map[string]Handler) (*Outbox, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%w: resource name is empty", ErrInvalidConfig)
	}
	db, ok := resource.Get[*sqlx.DB](name)
	if !ok || db == nil {
		return nil, fmt.Errorf("%w: %q", ErrResourceNotFound, name)
	}
	return New(ctx, db, c, handlers)
}

// begin 将公开操作接入关闭屏障，并同时监听调用方和组件的取消信号。
func (o *Outbox) begin(ctx context.Context) (context.Context, func(), error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("%w: context is nil", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if o == nil || o.ctx == nil {
		return nil, nil, ErrClosed
	}
	o.mu.Lock()
	if o.closed || o.ctx.Err() != nil {
		o.mu.Unlock()
		return nil, nil, ErrClosed
	}
	o.operations.Add(1)
	o.mu.Unlock()
	linked, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(o.ctx, cancel)
	return linked, func() { stop(); cancel(); o.operations.Done() }, nil
}

func (o *Outbox) validateMessage(m Message) error {
	if err := validateType(m.Type); err != nil {
		return err
	}
	if m.ID != 0 || m.Attempts != 0 {
		return fmt.Errorf("%w: new message must not set ID or attempts", ErrInvalidArgument)
	}
	if strings.TrimSpace(m.Payload) == "" {
		return fmt.Errorf("%w: payload is empty", ErrInvalidArgument)
	}
	if _, ok := o.handlers[m.Type]; !ok {
		return fmt.Errorf("%w: %q", ErrNoHandler, m.Type)
	}
	// JSON 合法性由 JSONB/JSON 字段约束验证，不在热路径重复解码。
	return nil
}

// Publish 返回 nil error 表示已提交；请通过 Result 检查提交后的首次处理结果。
func (o *Outbox) Publish(ctx context.Context, m Message) (Result, error) {
	results, err := o.Transaction(ctx, func(tx *Tx) error { _, err := tx.Enqueue(ctx, m); return err })
	if err != nil {
		return Result{}, err
	}
	return results[0], nil
}

// Tx 仅在 Transaction 回调期间有效。不得手动 Commit/Rollback SQLTx。
type Tx struct {
	o      *Outbox
	tx     *sqlx.Tx
	mu     sync.Mutex
	ids    []int64
	err    error
	closed bool
}

func (t *Tx) SQLTx() *sqlx.Tx {
	if t == nil {
		return nil
	}
	return t.tx
}

// Enqueue 发生错误时会标记事务失败，即使业务忽略该错误也不会提交部分消息。
func (t *Tx) Enqueue(ctx context.Context, m Message) (int64, error) {
	if t == nil {
		return 0, fmt.Errorf("%w: transaction is nil", ErrInvalidArgument)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return 0, fmt.Errorf("%w: transaction callback has finished", ErrInvalidArgument)
	}
	if t.err != nil {
		return 0, t.err
	}
	id, err := t.o.EnqueueTx(ctx, t.tx, m)
	if err != nil {
		t.err = err
		return 0, err
	}
	t.ids = append(t.ids, id)
	return id, nil
}

// Transaction 将业务数据与消息原子提交；提交后处理失败只写入 Result。
// handler 发生 panic 时先回滚，再原样向上传播 panic。
func (o *Outbox) Transaction(ctx context.Context, handler func(*Tx) error) ([]Result, error) {
	ctx, done, err := o.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if handler == nil {
		return nil, fmt.Errorf("%w: transaction handler is nil", ErrInvalidArgument)
	}
	tx, err := o.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("outbox: begin transaction: %w", err)
	}
	defer tx.Rollback()
	bound := &Tx{o: o, tx: tx}
	defer func() { bound.mu.Lock(); bound.closed = true; bound.mu.Unlock() }()
	err = handler(bound)
	bound.mu.Lock()
	bound.closed = true
	enqueueErr := bound.err
	ids := append([]int64(nil), bound.ids...)
	bound.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if enqueueErr != nil {
		return nil, enqueueErr
	}
	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("outbox: commit transaction (outcome may be uncertain): %w", err)
	}
	// 只有提交前的错误从 error 返回，避免调用方把已提交业务当作失败重做。
	return o.dispatch(ctx, ids), nil
}

// EnqueueTx 只写入调用方的事务。调用方必须检查错误，提交后才可 Dispatch。
// tx 必须属于 New 使用的同一数据库；组件不提交或回滚外部事务。
func (o *Outbox) EnqueueTx(ctx context.Context, tx *sqlx.Tx, m Message) (int64, error) {
	ctx, done, err := o.begin(ctx)
	if err != nil {
		return 0, err
	}
	defer done()
	if tx == nil || tx.DriverName() != o.db.DriverName() {
		return 0, fmt.Errorf("%w: invalid transaction", ErrInvalidArgument)
	}
	if err = o.validateMessage(m); err != nil {
		return 0, err
	}
	id, err := o.store.insert(ctx, tx, m)
	if err != nil {
		return 0, fmt.Errorf("outbox: insert message: %w", err)
	}
	return id, nil
}

// Dispatch 尝试领取指定的已提交消息。已成功、未到重试时间或已被其他 worker
// 领取的消息不会重复回调；各消息的处理/查询错误通过对应 Result 返回。
func (o *Outbox) Dispatch(ctx context.Context, ids ...int64) ([]Result, error) {
	ctx, done, err := o.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	seen := make(map[int64]bool, len(ids))
	for _, id := range ids {
		if id <= 0 || seen[id] {
			return nil, fmt.Errorf("%w: IDs must be positive and unique", ErrInvalidArgument)
		}
		seen[id] = true
	}
	return o.dispatch(ctx, ids), nil
}

func (o *Outbox) Get(ctx context.Context, id int64) (Record, error) {
	ctx, done, err := o.begin(ctx)
	if err != nil {
		return Record{}, err
	}
	defer done()
	if id <= 0 {
		return Record{}, fmt.Errorf("%w: invalid ID", ErrInvalidArgument)
	}
	queryCtx, cancel := context.WithTimeout(ctx, o.config.OperationTimeout)
	defer cancel()
	return o.store.get(queryCtx, id)
}

// Retry 仅重置失败记录，唤醒后台扫描；不会改变消息 ID。
func (o *Outbox) Retry(ctx context.Context, id int64) error {
	ctx, done, err := o.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	if id <= 0 {
		return fmt.Errorf("%w: invalid ID", ErrInvalidArgument)
	}
	queryCtx, cancel := context.WithTimeout(ctx, o.config.OperationTimeout)
	defer cancel()
	if err = o.store.retry(queryCtx, id); err != nil {
		return err
	}
	o.notify()
	return nil
}

// Close 停止领取，取消回调并等待已有操作，最后释放节点租约。可重复调用。
// 不响应 context 的业务回调可能使本次 Close 超时，后续仍会继续等待和释放。
func (o *Outbox) Close() error {
	if o == nil || o.ctx == nil {
		return nil
	}
	o.closeOnce.Do(func() {
		o.mu.Lock()
		o.closed = true
		o.cancel()
		o.mu.Unlock()
		go func() {
			o.background.Wait()
			o.operations.Wait()
			ctx, cancel := context.WithTimeout(context.Background(), o.config.OperationTimeout)
			defer cancel()
			o.closeErr = o.store.release(ctx, o.nodeID)
			close(o.closeDone)
		}()
	})
	ctx, cancel := context.WithTimeout(context.Background(), o.config.CloseTimeout)
	defer cancel()
	select {
	case <-o.closeDone:
		return o.closeErr
	case <-ctx.Done():
		return fmt.Errorf("outbox: close timeout: %w", ctx.Err())
	}
}

func (o *Outbox) notify() {
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

func (o *Outbox) dispatch(ctx context.Context, ids []int64) []Result {
	results := make([]Result, len(ids))
	for offset := 0; offset < len(ids); {
		count, err := o.reserve(ctx, min(len(ids)-offset, o.config.BatchSize), true)
		if err != nil {
			for i := offset; i < len(ids); i++ {
				results[i] = Result{ID: ids[i], Status: StatusPending, AttemptError: err}
			}
			break
		}
		batch := ids[offset : offset+count]
		queryCtx, cancel := context.WithTimeout(ctx, o.config.OperationTimeout)
		tasks, claimErr := o.store.claimIDs(queryCtx, batch, count)
		cancel()
		if claimErr != nil {
			o.unreserve(count)
			for i, id := range batch {
				results[offset+i] = Result{ID: id, Status: StatusPending, AttemptError: claimErr}
			}
		} else {
			o.unreserve(count - len(tasks))
			processed := o.runTasks(ctx, tasks)
			byID := make(map[int64]Result, len(processed))
			for _, result := range processed {
				byID[result.ID] = result
			}
			for i, id := range batch {
				if result, ok := byID[id]; ok {
					results[offset+i] = result
					continue
				}
				queryCtx, stop := context.WithTimeout(ctx, o.config.OperationTimeout)
				record, e := o.store.get(queryCtx, id)
				stop()
				results[offset+i] = Result{ID: id, Status: record.Status, AttemptError: e}
			}
		}
		offset += count
	}
	o.notify()
	return results
}

func (o *Outbox) reserve(ctx context.Context, limit int, wait bool) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	if wait {
		select {
		case o.slots <- struct{}{}:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	} else {
		select {
		case o.slots <- struct{}{}:
		default:
			return 0, nil
		}
	}
	count := 1
	for count < limit {
		select {
		case o.slots <- struct{}{}:
			count++
		default:
			return count, nil
		}
	}
	return count, nil
}

func (o *Outbox) unreserve(count int) {
	for range count {
		<-o.slots
	}
}

func (o *Outbox) runTasks(ctx context.Context, tasks []task) []Result {
	results := make([]Result, len(tasks))
	var wg sync.WaitGroup
	for i, t := range tasks {
		wg.Add(1)
		go func() { defer wg.Done(); defer o.unreserve(1); results[i] = o.execute(ctx, t) }()
	}
	wg.Wait()
	return results
}

func (o *Outbox) execute(ctx context.Context, t task) Result {
	started := time.Now()
	handlerCtx, cancel := context.WithTimeout(ctx, o.config.HandlerTimeout)
	err := invoke(handlerCtx, o.handlers[t.Type], t.Message)
	if err == nil {
		err = handlerCtx.Err()
	}
	cancel()
	// 请求取消或 Close 后也要尝试记录结果，使用独立、有限时的数据库上下文。
	finishCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), o.config.OperationTimeout)
	defer stop()
	status, finishErr := o.store.finish(finishCtx, t, err, o.backoff(t.Attempts))
	if err != nil || finishErr != nil {
		o.config.Logger.Warn("outbox delivery failed", "message_id", t.ID, "type", t.Type, "attempts", t.Attempts, "duration", time.Since(started), "error", errors.Join(err, finishErr))
	}
	return Result{ID: t.ID, Status: status, AttemptError: errors.Join(err, finishErr)}
}

func invoke(ctx context.Context, handler Handler, message Message) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("%w: %v", ErrHandlerPanic, value)
		}
	}()
	if err = ctx.Err(); err != nil {
		return err
	}
	if handler == nil {
		return ErrNoHandler
	}
	// 传递副本，业务修改 Message 不会影响确认时使用的 ID 和尝试次数。
	return handler(ctx, &message)
}
