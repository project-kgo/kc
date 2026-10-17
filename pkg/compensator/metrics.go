package compensator

import (
	"context"
	"time"
)

// Source 标识一次执行的来源。
type Source int

const (
	// SourceFirstRun 是 ModeInTx 下事务内的首次执行。
	SourceFirstRun Source = iota
	// SourceAfterCommit 是 ModeAfterCommit 下提交后的首次执行。
	SourceAfterCommit
	// SourceDispatch 是通过 Dispatch 触发的执行。
	SourceDispatch
	// SourceBackground 是后台补偿执行。
	SourceBackground
)

func (s Source) String() string {
	switch s {
	case SourceFirstRun:
		return "first_run"
	case SourceAfterCommit:
		return "after_commit"
	case SourceDispatch:
		return "dispatch"
	case SourceBackground:
		return "background"
	default:
		return "unknown"
	}
}

// ExecEvent 描述一次回调执行及其落库后的状态。
type ExecEvent struct {
	Type     string
	Source   Source
	Attempt  int
	Status   Status // 结果落库失败时为 StatusRunning
	Duration time.Duration
	Err      error
}

// Metrics 接收组件运行事件，用于对接 Prometheus/OpenTelemetry 等监控系统。
// 方法在热路径上同步调用，实现必须并发安全且不阻塞。
type Metrics interface {
	// Executed 在每次回调执行（含首次执行和补偿）后调用。
	Executed(ExecEvent)
	// Cleaned 在每批清理后调用，rows 为本批删除的行数。
	Cleaned(status Status, rows int64)
	// ShardsOwned 在每次分片协调成功后调用，owned 为本节点持有的分片数。
	ShardsOwned(owned int)
}

type noopMetrics struct{}

func (noopMetrics) Executed(ExecEvent)    {}
func (noopMetrics) Cleaned(Status, int64) {}
func (noopMetrics) ShardsOwned(int)       {}

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
