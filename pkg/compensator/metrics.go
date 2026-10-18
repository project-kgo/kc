package compensator

import (
	"context"
	"time"
)

// ExecEvent 描述一次回调执行及其落库后的状态。
type ExecEvent struct {
	Type     string
	Attempt  int
	Status   Status // 结果落库失败时为 StatusRunning
	Duration time.Duration
	Err      error
}

// Metrics 接收组件运行事件，用于对接 Prometheus/OpenTelemetry 等监控系统。
// 方法在热路径上同步调用，实现必须并发安全且不阻塞。
type Metrics interface {
	// Executed 在每次回调执行后调用。
	Executed(ExecEvent)
	// Cleaned 在每批清理后调用，rows 为本批删除的行数。
	Cleaned(status Status, rows int64)
}

type noopMetrics struct{}

func (noopMetrics) Executed(ExecEvent)    {}
func (noopMetrics) Cleaned(Status, int64) {}

// Stats 是全表任务积压的快照，适合低频（如每 15~60 秒）采集为监控指标。
type Stats struct {
	Pending int64
	Running int64
	Failed  int64
	// OldestDue 是最早到期的待执行任务的计划时间，没有积压时为零值。
	OldestDue time.Time
}

// Stats 统计全表的待执行、执行中和失败任务数。查询走部分索引，
// 但仍与积压量成正比，多节点部署时建议只由一处采集。
func (c *Compensator) Stats(ctx context.Context) (Stats, error) {
	done, err := c.begin(ctx)
	if err != nil {
		return Stats{}, err
	}
	defer done()
	opCtx, cancel := context.WithTimeout(ctx, c.config.OperationTimeout)
	defer cancel()
	return c.store.stats(opCtx)
}
