package scheduler

import (
	"context"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"time"

	"github.com/google/uuid"
)

// 未注册类型或 PostgreSQL 暂时不可用时，任务原样放回的延迟，不计尝试次数。
const requeueDelay = time.Second

// workItem 是一个已弹出、等待执行的任务。
type workItem struct {
	task   Task
	bucket int
	token  int64
	runAt  time.Time
}

// pollLoop 每 PollInterval 弹出到期任务；一轮弹满时立即再弹，不等下一个间隔。
func (s *Scheduler) pollLoop() {
	defer s.background.Done()
	defer close(s.jobs)
	for {
		filled := s.poll()
		if s.ctx.Err() != nil {
			return
		}
		if filled {
			continue
		}
		if !s.pause(s.config.PollInterval) {
			return
		}
	}
}

// poll 以空闲 worker 数为总配额，从轮转的起始桶开始依次弹出。
func (s *Scheduler) poll() bool {
	quota := s.reserve(s.config.WorkerCount)
	if quota == 0 {
		return false
	}
	remaining := quota
	start := int(s.nextBucket.Add(1) % uint64(s.config.Buckets))
	var leases []leaseBatch
	for i := 0; i < s.config.Buckets && remaining > 0; i++ {
		bucket := (start + i) % s.config.Buckets
		// 弹出不随 Close 取消：已经移入 processing 的任务必须交给 worker 归还或执行。
		ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), operationTimeout)
		batch, err := s.queue.pop(ctx, bucket, min(remaining, maxPopBatch), s.config.LeaseDuration)
		cancel()
		if err != nil {
			if s.ctx.Err() == nil {
				s.config.Logger.Error("scheduler pop tasks failed", "bucket", bucket, "error", err)
			}
			break
		}
		if batch.malformed > 0 {
			s.config.Logger.Error("scheduler skipped malformed redis members", "bucket", bucket, "count", batch.malformed)
		}
		if len(batch.items) == 0 {
			continue
		}
		leases = append(leases, batch)
		remaining -= len(batch.items)
	}
	if len(leases) == 0 {
		s.unreserve(quota)
		return false
	}
	popped := quota - remaining
	s.config.Metrics.Popped(popped)
	s.dispatch(leases, quota)
	return remaining == 0
}

// dispatch 先用一条语句取回所有持久任务的 payload 并过滤失效成员，再把任务交给 worker。
func (s *Scheduler) dispatch(leases []leaseBatch, quota int) {
	var ids []uuid.UUID
	for _, batch := range leases {
		for _, item := range batch.items {
			if item.data == "" {
				ids = append(ids, item.id)
			}
		}
	}
	stored := make(map[uuid.UUID]storedTask, len(ids))
	var fetchErr error
	if len(ids) > 0 {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), operationTimeout)
		tasks, err := s.store.fetch(ctx, ids)
		cancel()
		if err != nil {
			fetchErr = err
			s.config.Logger.Error("scheduler fetch tasks failed", "count", len(ids), "error", err)
		}
		for _, task := range tasks {
			stored[task.task.ID] = task
		}
	}
	dispatched := 0
	for _, batch := range leases {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), operationTimeout)
		var stale []uuid.UUID
		var delayed []requeueItem
		for _, item := range batch.items {
			if item.data != "" {
				task, err := decodeVolatile(item.id, item.data)
				if err != nil {
					s.config.Logger.Warn("scheduler decode volatile task failed", "task_id", item.id, "error", err)
					stale = append(stale, item.id)
					continue
				}
				s.jobs <- workItem{task: task, bucket: batch.bucket, token: batch.token, runAt: item.runAt}
				dispatched++
				continue
			}
			if fetchErr != nil {
				delayed = append(delayed, requeueItem{id: item.id})
				continue
			}
			task, ok := stored[item.id]
			if !ok {
				stale = append(stale, item.id)
				continue
			}
			s.jobs <- workItem{task: task.task, bucket: batch.bucket, token: batch.token, runAt: task.runAt}
			dispatched++
		}
		if len(delayed) > 0 {
			s.requeue(ctx, batch.bucket, batch.token, requeueDelay, delayed)
		}
		if len(stale) > 0 {
			s.ack(ctx, batch.bucket, batch.token, stale)
			s.config.Metrics.Stale(len(stale))
		}
		cancel()
	}
	// 未交给 worker 的任务（失效成员、读取失败放回）立即释放配额。
	s.unreserve(quota - dispatched)
}

// reserve 非阻塞地占用至多 limit 个 worker 容量。
func (s *Scheduler) reserve(limit int) int {
	count := 0
	for count < limit {
		select {
		case s.slots <- struct{}{}:
			count++
		default:
			return count
		}
	}
	return count
}

func (s *Scheduler) unreserve(count int) {
	for range count {
		<-s.slots
	}
}

func (s *Scheduler) worker() {
	defer s.background.Done()
	for item := range s.jobs {
		if s.ctx.Err() != nil {
			// Close 时已弹出但尚未开始执行的任务立即放回。
			s.abandon(item)
			<-s.slots
			continue
		}
		s.execute(item)
		<-s.slots
	}
}

func (s *Scheduler) abandon(item workItem) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), operationTimeout)
	defer cancel()
	data := ""
	if item.task.Volatile {
		encoded, err := encodeVolatile(item.task, item.task.Attempts)
		if err != nil {
			s.config.Logger.Warn("scheduler encode volatile task failed", "task_id", item.task.ID, "error", err)
			return
		}
		data = encoded
	}
	s.requeue(ctx, item.bucket, item.token, 0, []requeueItem{{id: item.task.ID, data: data}})
}

// execute 在 HandlerTimeout 约束下执行回调并写回结果。
func (s *Scheduler) execute(item workItem) {
	started := time.Now()
	handler := s.handlers.Load().get(item.task.Type)
	if handler == nil {
		// 各节点必须注册相同的回调集合；未注册时延迟放回，不计尝试次数。
		s.config.Metrics.HandlerMissing(1)
		ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), operationTimeout)
		s.requeue(ctx, item.bucket, item.token, requeueDelay, []requeueItem{{id: item.task.ID}})
		cancel()
		s.config.Logger.Warn("scheduler handler not registered", "task_id", item.task.ID, "type", item.task.Type)
		return
	}
	callErr := s.invoke(handler, item.task)
	if callErr != nil && s.handlerCtx.Err() != nil {
		// Close 等满 ShutdownTimeout 后中断了回调：立即放回，不计尝试次数。
		s.abandon(item)
		return
	}
	event := ExecEvent{
		Type: item.task.Type, Attempts: item.task.Attempts, Volatile: item.task.Volatile,
		Duration: time.Since(started), Err: callErr,
	}
	if !item.runAt.IsZero() {
		event.TriggerDelay = started.Sub(item.runAt)
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), operationTimeout)
	if item.task.Volatile {
		s.finishVolatile(ctx, item, callErr, &event)
	} else {
		s.finishStored(ctx, item, callErr, &event)
	}
	cancel()
	s.config.Metrics.Executed(event)
	if event.Err != nil {
		s.config.Logger.Warn("scheduler task failed", "task_id", item.task.ID, "type", item.task.Type,
			"attempts", item.task.Attempts, "volatile", item.task.Volatile, "duration", event.Duration, "error", event.Err)
	}
}

// finishStored 先写 PostgreSQL 再改 Redis：成功 ack，失败有剩余次数时按退避 requeue。
func (s *Scheduler) finishStored(ctx context.Context, item workItem, callErr error, event *ExecEvent) {
	if callErr == nil {
		updated, err := s.store.complete(ctx, item.task.ID)
		if err != nil {
			// 回调可能已经产生副作用：结果未知，租约到期后任务会再次执行。
			event.Status, event.Err = StatusQueued, errors.Join(callErr, err)
			s.config.Logger.Error("scheduler write success failed", "task_id", item.task.ID, "error", err)
			return
		}
		s.ack(ctx, item.bucket, item.token, []uuid.UUID{item.task.ID})
		event.Status = StatusSucceeded
		if !updated {
			s.config.Logger.Info("scheduler task already finished", "task_id", item.task.ID)
		}
		return
	}
	delay := s.backoff(item.task.Attempts + 1)
	status, updated, err := s.store.fail(ctx, item.task.ID, s.config.MaxAttempts, errorText(callErr), delay)
	if err != nil {
		event.Status, event.Err = StatusQueued, errors.Join(callErr, err)
		s.config.Logger.Error("scheduler write failure failed", "task_id", item.task.ID, "error", err)
		return
	}
	if updated && status == StatusQueued {
		s.requeue(ctx, item.bucket, item.token, delay, []requeueItem{{id: item.task.ID}})
		event.Status = StatusQueued
		return
	}
	// 行已不满足 status = 1（被取消或由其他节点写回），或次数已耗尽。
	s.ack(ctx, item.bucket, item.token, []uuid.UUID{item.task.ID})
	event.Status = StatusFailed
}

// finishVolatile 易失任务不访问 PostgreSQL：成功 ack；失败有剩余次数时更新内容并 requeue，
// 次数耗尽时 ack 丢弃。
func (s *Scheduler) finishVolatile(ctx context.Context, item workItem, callErr error, event *ExecEvent) {
	if callErr == nil {
		s.ack(ctx, item.bucket, item.token, []uuid.UUID{item.task.ID})
		event.Status = StatusSucceeded
		return
	}
	next := item.task.Attempts + 1
	if next >= s.config.MaxAttempts {
		s.ack(ctx, item.bucket, item.token, []uuid.UUID{item.task.ID})
		event.Status = StatusFailed
		s.config.Logger.Warn("scheduler volatile task discarded after max attempts",
			"task_id", item.task.ID, "type", item.task.Type, "attempts", next)
		return
	}
	data, err := encodeVolatile(item.task, next)
	if err != nil {
		s.ack(ctx, item.bucket, item.token, []uuid.UUID{item.task.ID})
		event.Status, event.Err = StatusFailed, errors.Join(callErr, err)
		return
	}
	s.requeue(ctx, item.bucket, item.token, s.backoff(next), []requeueItem{{id: item.task.ID, data: data}})
	event.Status = StatusQueued
}

// invoke 以 HandlerTimeout 调用回调，panic 转为错误，并传递任务副本。
func (s *Scheduler) invoke(handler Handler, task Task) (err error) {
	// 父级是 handlerCtx 而不是 s.ctx：Close 开始时不打断执行中的回调。
	ctx, cancel := context.WithTimeout(s.handlerCtx, s.config.HandlerTimeout)
	defer cancel()
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("scheduler: handler panic: %v", value)
		}
	}()
	if err = ctx.Err(); err != nil {
		return err
	}
	copied := task
	if err = handler(ctx, &copied); err == nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		// 回调忽略超时并返回 nil 时按超时处理，避免租约可能已失效的结果被当作成功。
		err = ctx.Err()
	}
	return err
}

// backoff 返回第 attempt 次执行失败后的等待时间：指数增长、封顶，叠加 equal jitter
// 避免大量失败任务在同一时刻再次冲击下游。
func (s *Scheduler) backoff(attempt int) time.Duration {
	delay := s.config.RetryBackoffMin
	for i := 1; i < attempt && delay < s.config.RetryBackoffMax; i++ {
		if delay > s.config.RetryBackoffMax/2 {
			delay = s.config.RetryBackoffMax
			break
		}
		delay *= 2
	}
	delay = min(delay, s.config.RetryBackoffMax)
	half := delay / 2
	return max(s.config.RetryBackoffMin, half+mathrand.N(delay-half+1))
}

// requeue 把任务放回 ready；失败只记录日志，任务留在 processing 等租约回收。
func (s *Scheduler) requeue(ctx context.Context, bucket int, token int64, delay time.Duration, items []requeueItem) {
	if _, err := s.queue.requeue(ctx, bucket, token, delay, items); err != nil {
		s.config.Logger.Warn("scheduler requeue failed", "bucket", bucket, "count", len(items), "error", err)
	}
}

// ack 移除结果已落库的任务；失败只记录日志，任务会一直留在 processing 直到租约回收。
func (s *Scheduler) ack(ctx context.Context, bucket int, token int64, ids []uuid.UUID) {
	if _, err := s.queue.ack(ctx, bucket, token, ids); err != nil {
		s.config.Logger.Warn("scheduler ack failed", "bucket", bucket, "count", len(ids), "error", err)
	}
}

// jitter 给间隔加 10% 随机抖动，避免多节点同步。
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	return d + time.Duration(mathrand.Int64N(int64(d/10)+1))
}
