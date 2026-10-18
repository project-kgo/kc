package compensator

import (
	"context"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"time"
)

// scanLoop 定时或被唤醒时领取到期任务，交给 worker 执行。
func (c *Compensator) scanLoop() {
	defer c.background.Done()
	defer close(c.jobs)
	ticker := time.NewTicker(c.config.ScanInterval)
	defer ticker.Stop()
	c.notify()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
		case <-c.wake:
		}
		c.scan()
	}
}

// scan 只按空闲 worker 数领取，任务领取后立即开始执行，不在本地排队导致租约过期；
// 领取满批时继续下一批。
func (c *Compensator) scan() {
	for c.ctx.Err() == nil {
		free := c.reserve(c.config.BatchSize)
		if free == 0 {
			// 全部 worker 忙碌，由 worker 完成后唤醒。
			c.backlog.Store(true)
			return
		}
		// 领取不随 Close 取消：语句可能已在服务端提交，此时丢弃结果会让任务空等租约到期并白耗
		// 一次执行次数。关闭期间领到的任务交给 worker，由它归还且不消耗次数。
		ctx, cancel := context.WithTimeout(context.WithoutCancel(c.ctx), c.config.OperationTimeout)
		tasks, err := c.store.claim(ctx, free, newToken())
		cancel()
		if err != nil {
			c.unreserve(free)
			if c.ctx.Err() == nil {
				c.config.Logger.Error("compensator claim failed", "error", err)
			}
			return
		}
		// 按领取到的总行数判断是否还有积压：预算耗尽的行虽不执行，也占用了本批名额。
		claimedRows := len(tasks)
		tasks = c.runnable(tasks)
		c.unreserve(free - len(tasks))
		for _, t := range tasks {
			c.jobs <- t
		}
		if claimedRows < free {
			c.backlog.Store(false)
			return
		}
		c.backlog.Store(true)
	}
}

// reserve 非阻塞地占用至多 limit 个 worker 容量。
func (c *Compensator) reserve(limit int) int {
	count := 0
	for count < limit {
		select {
		case c.slots <- struct{}{}:
			count++
		default:
			return count
		}
	}
	return count
}

func (c *Compensator) unreserve(count int) {
	for range count {
		<-c.slots
	}
}

// runnable 过滤掉领取时因预算耗尽被标记为 failed 的任务，并为它们记录日志和指标。
func (c *Compensator) runnable(tasks []claimed) []claimed {
	result := tasks[:0:0]
	for _, t := range tasks {
		if !t.exhausted {
			result = append(result, t)
			continue
		}
		err := errors.New("retry budget exhausted after lease expiration")
		c.config.Metrics.Executed(ExecEvent{Type: t.Type, Attempt: t.Attempt, Status: StatusFailed, Err: err})
		c.config.Logger.Warn("compensator task failed", "task_id", t.ID, "type", t.Type, "attempt", t.Attempt, "error", err)
	}
	return result
}

func (c *Compensator) worker() {
	defer c.background.Done()
	for t := range c.jobs {
		c.execute(t)
		<-c.slots
		if c.backlog.Load() {
			c.notify()
		}
	}
}

// execute 执行回调并按租约令牌落库结果。因组件关闭而中断的执行会归还任务且不消耗次数。
func (c *Compensator) execute(t claimed) {
	started := time.Now()
	err := c.invoke(c.ctx, &t.Task)
	ctx, cancel := context.WithTimeout(context.WithoutCancel(c.ctx), c.config.OperationTimeout)
	defer cancel()
	if errors.Is(err, context.Canceled) && c.ctx.Err() != nil {
		if abandonErr := c.store.abandon(ctx, t); abandonErr != nil {
			c.config.Logger.Warn("compensator abandon task failed", "task_id", t.ID, "error", abandonErr)
		}
		return
	}
	status, lastError, delay := c.outcome(t.Attempt, err)
	if finishErr := c.store.finish(ctx, t, status, lastError, delay); finishErr != nil {
		status, err = StatusRunning, errors.Join(err, finishErr)
	}
	duration := time.Since(started)
	c.config.Metrics.Executed(ExecEvent{Type: t.Type, Attempt: t.Attempt, Status: status, Duration: duration, Err: err})
	if err != nil {
		c.config.Logger.Warn("compensator task failed", "task_id", t.ID, "type", t.Type, "attempt", t.Attempt,
			"duration", duration, "error", err)
	}
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

// backoff 返回第 attempt 次执行失败后的等待时间：指数增长、封顶，叠加 equal jitter
// 避免大量失败任务在同一时刻再次冲击下游。
func (c *Compensator) backoff(attempt int) time.Duration {
	delay := c.config.RetryBackoffMin
	for i := 1; i < attempt && delay < c.config.RetryBackoffMax; i++ {
		if delay > c.config.RetryBackoffMax/2 {
			delay = c.config.RetryBackoffMax
			break
		}
		delay *= 2
	}
	delay = min(delay, c.config.RetryBackoffMax)
	half := delay / 2
	return max(time.Millisecond, half+mathrand.N(delay-half+1))
}

func (c *Compensator) cleanupLoop() {
	defer c.background.Done()
	ticker := time.NewTicker(c.config.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.cleanupStatus(StatusSucceeded, c.config.CleanupAfter)
			if c.config.FailedRetention > 0 {
				c.cleanupStatus(StatusFailed, c.config.FailedRetention)
			}
		}
	}
}

// maxCleanupRounds 限制单次清理的批次数，积压很大时分摊到后续周期，避免长时间占用连接。
const maxCleanupRounds = 100

// cleanupStatus 分批删除超过保留期的终态记录。各节点并发清理时跳过彼此锁定的行。
func (c *Compensator) cleanupStatus(status Status, olderThan time.Duration) {
	for range maxCleanupRounds {
		if c.ctx.Err() != nil {
			return
		}
		ctx, cancel := context.WithTimeout(c.ctx, c.config.OperationTimeout)
		deleted, err := c.store.cleanup(ctx, status, olderThan, c.config.CleanupBatchSize)
		cancel()
		if err != nil {
			if c.ctx.Err() == nil {
				c.config.Logger.Error("compensator cleanup failed", "status", status, "error", err)
			}
			return
		}
		if deleted > 0 {
			c.config.Metrics.Cleaned(status, deleted)
		}
		if deleted < int64(c.config.CleanupBatchSize) {
			return
		}
	}
}
