package compensator

import (
	"context"
	mathrand "math/rand/v2"
	"time"
)

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

// refresh 续约节点并更新本地持有的分片。本地有效期按调用前的时间保守估算，
// 心跳持续失败超过租约期后停止领取，避免与接管者长时间重叠。
func (c *Compensator) refresh(ctx context.Context) error {
	started := time.Now()
	ctx, cancel := context.WithTimeout(ctx, c.config.OperationTimeout)
	defer cancel()
	shards, err := c.store.coordinate(ctx, c.nodeID)
	if err != nil {
		return err
	}
	c.shardsMu.Lock()
	gained := len(shards) > len(c.shards)
	c.shards = shards
	c.shardsUntil = started.Add(c.config.NodeTTL - c.config.HeartbeatInterval)
	c.shardsMu.Unlock()
	c.config.Metrics.ShardsOwned(len(shards))
	if gained {
		c.notify()
	}
	return nil
}

func (c *Compensator) ownedShards() []int16 {
	c.shardsMu.RLock()
	defer c.shardsMu.RUnlock()
	if len(c.shards) == 0 || time.Now().After(c.shardsUntil) {
		return nil
	}
	return c.shards
}

func (c *Compensator) coordinateLoop() {
	defer c.background.Done()
	ticker := time.NewTicker(c.config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if err := c.refresh(c.ctx); err != nil && c.ctx.Err() == nil {
				c.config.Logger.Error("compensator coordinate failed", "node_id", c.nodeID, "error", err)
			}
		}
	}
}

func (c *Compensator) worker() {
	defer c.background.Done()
	for t := range c.jobs {
		c.execute(c.ctx, t, SourceBackground, true)
		<-c.slots
		if c.backlog.Load() {
			c.notify()
		}
	}
}

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

// scan 只按空闲 worker 数领取，避免任务在本地排队导致租约过期；领取满批时继续下一批。
func (c *Compensator) scan() {
	for c.ctx.Err() == nil {
		shards := c.ownedShards()
		if len(shards) == 0 {
			return
		}
		free := c.reserve(c.config.BatchSize)
		if free == 0 {
			// 全部 worker 忙碌，由 worker 完成后唤醒。
			c.backlog.Store(true)
			return
		}
		// 每轮轮转起点，避免靠前的分片长期优先。
		offset := int(c.cursor.Add(1) % uint64(len(shards)))
		rotated := make([]int16, 0, len(shards))
		rotated = append(append(rotated, shards[offset:]...), shards[:offset]...)
		ctx, cancel := context.WithTimeout(c.ctx, c.config.OperationTimeout)
		tasks, err := c.store.claim(ctx, rotated, free, newToken())
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
		tasks = c.runnable(tasks, SourceBackground)
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

func (c *Compensator) cleanupLoop() {
	defer c.background.Done()
	ticker := time.NewTicker(c.config.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.cleanup()
		}
	}
}

// maxCleanupRounds 限制单次清理的批次数，积压很大时分摊到后续周期，避免长时间占用连接。
const maxCleanupRounds = 100

// cleanup 只清理本节点持有分片中的终态记录，节点之间互不竞争。
func (c *Compensator) cleanup() {
	shards := c.ownedShards()
	if len(shards) == 0 {
		return
	}
	c.cleanupStatus(shards, StatusSucceeded, c.config.CleanupAfter)
	if c.config.FailedRetention > 0 {
		c.cleanupStatus(shards, StatusFailed, c.config.FailedRetention)
	}
}

func (c *Compensator) cleanupStatus(shards []int16, status Status, olderThan time.Duration) {
	for range maxCleanupRounds {
		if c.ctx.Err() != nil {
			return
		}
		ctx, cancel := context.WithTimeout(c.ctx, c.config.OperationTimeout)
		deleted, err := c.store.cleanup(ctx, shards, status, olderThan, c.config.CleanupBatchSize)
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
