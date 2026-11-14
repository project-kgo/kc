package scheduler

import (
	"context"
	"time"
)

// cleanupBatch 是 cleaner 单批删除的行数。
const cleanupBatch = 1000

// maxCleanupRounds 限制单次清理的批次数，积压很大时分摊到后续周期，避免长时间占用连接。
const maxCleanupRounds = 100

// cleanLoop 定时删除超过保留期的终态任务。
func (s *Scheduler) cleanLoop() {
	defer s.background.Done()
	for {
		s.clean()
		if !s.pause(s.config.CleanupInterval) {
			return
		}
	}
}

func (s *Scheduler) clean() {
	s.cleanStatus(StatusSucceeded, s.config.Retention)
	s.cleanStatus(StatusCancelled, s.config.Retention)
	if s.config.FailedRetention > 0 {
		s.cleanStatus(StatusFailed, s.config.FailedRetention)
	}
}

// cleanStatus 分批删除某一状态的终态任务；多个节点并发清理时跳过彼此锁定的行。
func (s *Scheduler) cleanStatus(status Status, olderThan time.Duration) {
	for range maxCleanupRounds {
		if s.ctx.Err() != nil {
			return
		}
		ctx, cancel := context.WithTimeout(s.ctx, operationTimeout)
		deleted, err := s.store.cleanup(ctx, status, olderThan, cleanupBatch)
		cancel()
		if err != nil {
			if s.ctx.Err() == nil {
				s.config.Logger.Error("scheduler cleanup failed", "status", status, "error", err)
			}
			return
		}
		if deleted > 0 {
			s.config.Metrics.Cleaned(status, deleted)
		}
		if deleted < cleanupBatch {
			return
		}
	}
}
