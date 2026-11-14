package scheduler

import "context"

// reconcileLoop 定时把 PostgreSQL 中已入队但 Redis 中缺失的任务补回。
func (s *Scheduler) reconcileLoop() {
	defer s.background.Done()
	for {
		s.reconcile()
		if !s.pause(s.config.ReconcileInterval) {
			return
		}
	}
}

// reconcile 按 (run_at, id) 游标分页扫描，对每一批调用幂等的 enqueue 脚本：
// 成员已在 ready 或 processing 中时不会重复写入。
func (s *Scheduler) reconcile() {
	var cursor reconcileCursor
	for {
		if s.ctx.Err() != nil {
			return
		}
		ctx, cancel := context.WithTimeout(s.ctx, operationTimeout)
		tasks, err := s.store.reconcile(ctx, cursor, reconcileBatch)
		cancel()
		if err != nil {
			if s.ctx.Err() == nil {
				s.config.Logger.Error("scheduler reconcile tasks failed", "error", err)
			}
			return
		}
		if len(tasks) == 0 {
			return
		}
		n, err := s.enqueue(s.ctx, tasks)
		if err != nil {
			if s.ctx.Err() == nil {
				s.config.Logger.Error("scheduler requeue missing tasks failed", "count", len(tasks), "error", err)
			}
			return
		}
		if n > 0 {
			s.config.Metrics.Reconciled(n)
		}
		last := tasks[len(tasks)-1]
		cursor = reconcileCursor{runAt: last.runAt, id: last.id}
		if len(tasks) < reconcileBatch {
			return
		}
	}
}
