package outbox

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

func (o *Outbox) backoff(attempt int) time.Duration {
	delay := o.config.RetryMin
	for i := 1; i < attempt && delay < o.config.RetryMax; i++ {
		if delay > o.config.RetryMax/2 {
			delay = o.config.RetryMax
			break
		}
		delay *= 2
	}
	delay = min(delay, o.config.RetryMax)
	// Equal jitter 避免所有故障消息在同一时刻再次冲击下游。
	return max(time.Millisecond, delay/2+time.Duration(rand.Int64N(int64(delay-delay/2)+1)))
}

func (o *Outbox) refresh(ctx context.Context) error {
	o.coordinateMu.Lock()
	defer o.coordinateMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, o.config.OperationTimeout)
	defer cancel()
	leases, err := o.store.coordinate(ctx, o.nodeID)
	if err != nil {
		return err
	}
	o.leasesMu.Lock()
	o.leases = leases
	o.leasesMu.Unlock()
	return nil
}

func (o *Outbox) owned() []shardLease {
	o.leasesMu.RLock()
	defer o.leasesMu.RUnlock()
	return append([]shardLease(nil), o.leases...)
}

func (o *Outbox) coordinateLoop() {
	defer o.background.Done()
	ticker := time.NewTicker(o.config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-o.ctx.Done():
			return
		case <-ticker.C:
			if err := o.refresh(o.ctx); err != nil {
				if o.ctx.Err() == nil {
					o.config.Logger.Error("outbox coordinate failed", "error", err)
				}
			} else {
				o.notify()
			}
		}
	}
}

func (o *Outbox) worker() {
	defer o.background.Done()
	for t := range o.jobs {
		o.execute(o.ctx, t)
		o.unreserve(1)
		o.notify()
	}
}

func (o *Outbox) scanLoop() {
	defer o.background.Done()
	defer close(o.jobs)
	ticker := time.NewTicker(o.config.ScanInterval)
	defer ticker.Stop()
	o.notify()
	for {
		select {
		case <-o.ctx.Done():
			return
		case <-ticker.C:
		case <-o.wake:
		}
		if err := o.scan(o.ctx, true); err != nil && o.ctx.Err() == nil {
			o.config.Logger.Error("outbox scan failed", "error", err)
		}
	}
}

// ScanOnce 同步处理一轮（最多 BatchSize 条）并按需清理。
// 关闭后台模式时，定期调用本方法即可完成节点续租和消息补偿。
func (o *Outbox) ScanOnce(ctx context.Context) error {
	ctx, done, err := o.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	if err = o.refresh(ctx); err != nil {
		return err
	}
	return o.scan(ctx, false)
}

func (o *Outbox) scan(ctx context.Context, async bool) error {
	leases := o.owned()
	if len(leases) == 0 {
		return nil
	}
	allLeases := leases
	queryCtx, cancel := context.WithTimeout(ctx, o.config.OperationTimeout)
	var err error
	leases, err = o.store.readyShards(queryCtx, leases)
	cancel()
	if err != nil {
		return err
	}
	if len(leases) == 0 {
		return o.cleanupIfDue(ctx, allLeases)
	}
	start := int(o.cursor.Add(1) % uint64(len(leases)))
	remaining := o.config.BatchSize
	var scanErr error
	for i := 0; i < len(leases) && remaining > 0; i++ {
		if err := ctx.Err(); err != nil {
			return errors.Join(scanErr, err)
		}
		count, err := o.reserve(ctx, remaining, false)
		if err != nil {
			return errors.Join(scanErr, err)
		}
		if count == 0 {
			break
		}
		lease := leases[(start+i)%len(leases)]
		queryCtx, cancel := context.WithTimeout(ctx, o.config.OperationTimeout)
		tasks, err := o.store.claimShard(queryCtx, lease, count)
		cancel()
		if err != nil {
			o.unreserve(count)
			if !errors.Is(err, ErrLeaseLost) {
				scanErr = errors.Join(scanErr, err)
			}
			continue
		}
		o.unreserve(count - len(tasks))
		remaining -= len(tasks)
		if async {
			for _, t := range tasks {
				o.jobs <- t
			}
		} else {
			for _, r := range o.runTasks(ctx, tasks) {
				scanErr = errors.Join(scanErr, r.AttemptError)
			}
		}
	}
	return errors.Join(scanErr, o.cleanupIfDue(ctx, allLeases))
}

func (o *Outbox) cleanupIfDue(ctx context.Context, leases []shardLease) error {
	// 本地时钟只用于控制频率，记录是否过期仍由数据库时间判断。
	now := time.Now().UnixMilli()
	previous := o.lastCleanup.Load()
	if now-previous < o.config.CleanupInterval.Milliseconds() || !o.lastCleanup.CompareAndSwap(previous, now) {
		return nil
	}
	var result error
	for _, lease := range leases {
		if err := ctx.Err(); err != nil {
			return errors.Join(result, err)
		}
		queryCtx, cancel := context.WithTimeout(ctx, o.config.OperationTimeout)
		_, err := o.store.cleanup(queryCtx, lease)
		cancel()
		if err != nil && !errors.Is(err, ErrLeaseLost) {
			result = errors.Join(result, err)
		}
	}
	return result
}
