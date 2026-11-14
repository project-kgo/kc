package scheduler

import (
	"context"
)

// loadLoop 定时把进入窗口的 pending 任务搬进 Redis。
func (s *Scheduler) loadLoop() {
	defer s.background.Done()
	for {
		s.load()
		if !s.pause(s.config.LoadInterval) {
			return
		}
	}
}

// load 直到一轮取回的行数小于 LoadBatch。先改 PostgreSQL 再写 Redis；两步之间失败或崩溃时
// 任务处于 queued 但不在 Redis，由 reconciler 补回。
func (s *Scheduler) load() {
	for s.ctx.Err() == nil {
		ctx, cancel := context.WithTimeout(s.ctx, operationTimeout)
		tasks, err := s.store.load(ctx, s.config.LoadBatch)
		cancel()
		if err != nil {
			if s.ctx.Err() == nil {
				s.config.Logger.Error("scheduler load tasks failed", "error", err)
			}
			return
		}
		if len(tasks) > 0 {
			if _, err = s.enqueue(s.ctx, tasks); err != nil {
				if s.ctx.Err() == nil {
					s.config.Logger.Error("scheduler enqueue loaded tasks failed", "count", len(tasks), "error", err)
				}
				return
			}
		}
		if len(tasks) < s.config.LoadBatch {
			return
		}
	}
}
