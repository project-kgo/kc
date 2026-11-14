package scheduler

import (
	"context"
	"time"
)

// ExecEvent 描述一次回调执行及其落库后的状态。
type ExecEvent struct {
	Type         string
	Attempts     int
	Volatile     bool
	Status       Status // 落库失败时为 StatusQueued，表示结果未知、任务仍会被再次执行
	TriggerDelay time.Duration
	Duration     time.Duration
	Err          error
}

// Metrics 接收组件运行事件，用于对接 Prometheus/OpenTelemetry 等监控系统。
// 方法在热路径上同步调用，实现必须并发安全且不阻塞。
type Metrics interface {
	// Registered 在任务登记成功后调用。
	Registered(taskType string, volatile bool)
	// Enqueued 在任务写入 Redis ready 后调用，n 为实际写入的成员数。
	Enqueued(n int, volatile bool)
	// Popped 在从 ready 弹出任务后调用。
	Popped(n int)
	// Executed 在每次回调执行及结果写回后调用，覆盖成功、失败和重试。
	Executed(ExecEvent)
	// Reclaimed 在租约过期的任务被回收时调用。
	Reclaimed(n int)
	// Reconciled 在对账把缺失的任务补回 ready 后调用。
	Reconciled(n int)
	// Cancelled 在任务取消成功后调用。
	Cancelled(n int)
	// Cleaned 在每批清理后调用，rows 为本批删除的行数。
	Cleaned(status Status, rows int64)
	// Stale 在弹出时发现任务已完成或已取消、需要丢弃陈旧成员时调用。
	Stale(n int)
	// HandlerMissing 在弹出未注册类型的任务并放回时调用。
	HandlerMissing(n int)
}

type noopMetrics struct{}

func (noopMetrics) Registered(string, bool) {}
func (noopMetrics) Enqueued(int, bool)      {}
func (noopMetrics) Popped(int)              {}
func (noopMetrics) Executed(ExecEvent)      {}
func (noopMetrics) Reclaimed(int)           {}
func (noopMetrics) Reconciled(int)          {}
func (noopMetrics) Cancelled(int)           {}
func (noopMetrics) Cleaned(Status, int64)   {}
func (noopMetrics) Stale(int)               {}
func (noopMetrics) HandlerMissing(int)      {}

// Stats 是任务积压的快照，适合低频（如每 15~60 秒）采集为监控指标。
type Stats struct {
	// Pending / Queued / Failed 是 PostgreSQL 中各状态的任务数。
	Pending int64
	Queued  int64
	Failed  int64
	// ReadyDue 是 Redis ready 中已到期未弹出的数量，Processing 是执行中的数量。
	ReadyDue   int64
	Processing int64
	// OldestOverdue 是最老的逾期时长，没有逾期任务时为零值。
	OldestOverdue time.Duration
}

// Stats 查询 PostgreSQL 与 Redis 的积压情况。PostgreSQL 的统计与积压量成正比，
// 多节点部署时建议只由一处采集。
func (s *Scheduler) Stats(ctx context.Context) (Stats, error) {
	done, err := s.begin(ctx)
	if err != nil {
		return Stats{}, err
	}
	defer done()
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	stats, err := s.store.stats(ctx)
	if err != nil {
		return Stats{}, err
	}
	redisStats, err := s.queue.stats(ctx)
	if err != nil {
		return Stats{}, err
	}
	stats.ReadyDue, stats.Processing = redisStats.readyDue, redisStats.processing
	return stats, nil
}
