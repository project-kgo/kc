package scheduler

import (
	"context"

	"github.com/google/uuid"
)

// reclaimLoop 定时回收 worker 崩溃或失联留下的任务。
func (s *Scheduler) reclaimLoop() {
	defer s.background.Done()
	for {
		s.reclaim()
		if !s.pause(s.config.ReclaimInterval) {
			return
		}
	}
}

func (s *Scheduler) reclaim() {
	for bucket := range s.config.Buckets {
		if s.ctx.Err() != nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), operationTimeout)
		batch, err := s.queue.reclaim(ctx, bucket, reclaimBatch)
		if err != nil {
			cancel()
			if s.ctx.Err() == nil {
				s.config.Logger.Error("scheduler reclaim expired leases failed", "bucket", bucket, "error", err)
			}
			return
		}
		if batch.malformed > 0 {
			s.config.Logger.Error("scheduler skipped malformed redis members", "bucket", bucket, "count", batch.malformed)
		}
		if len(batch.items) == 0 {
			cancel()
			continue
		}
		s.config.Metrics.Reclaimed(len(batch.items))
		s.reclaimBatch(ctx, batch)
		cancel()
	}
}

// reclaimBatch 对同一令牌下的一批任务计一次尝试：仍有剩余次数的立即放回，其余移除。
func (s *Scheduler) reclaimBatch(ctx context.Context, batch leaseBatch) {
	var ackIDs []uuid.UUID
	var items []requeueItem
	var stored []uuid.UUID
	for _, item := range batch.items {
		if item.data == "" {
			stored = append(stored, item.id)
			continue
		}
		task, err := decodeVolatile(item.id, item.data)
		if err != nil {
			s.config.Logger.Warn("scheduler decode volatile task failed", "task_id", item.id, "error", err)
			ackIDs = append(ackIDs, item.id)
			continue
		}
		next := task.Attempts + 1
		if next >= s.config.MaxAttempts {
			s.config.Logger.Warn("scheduler volatile task discarded after lease expiration",
				"task_id", item.id, "type", task.Type, "attempts", next)
			ackIDs = append(ackIDs, item.id)
			continue
		}
		data, err := encodeVolatile(task, next)
		if err != nil {
			s.config.Logger.Warn("scheduler encode volatile task failed", "task_id", item.id, "error", err)
			ackIDs = append(ackIDs, item.id)
			continue
		}
		items = append(items, requeueItem{id: item.id, data: data})
	}
	if len(stored) > 0 {
		reclaimed, err := s.store.reclaim(ctx, stored, s.config.MaxAttempts)
		if err != nil {
			// 留在 processing，顺延后的租约到期后会再次回收。
			s.config.Logger.Error("scheduler reclaim tasks failed", "count", len(stored), "error", err)
		} else {
			queued := make(map[uuid.UUID]struct{}, len(reclaimed))
			for _, task := range reclaimed {
				if task.status == StatusQueued {
					queued[task.id] = struct{}{}
				}
			}
			for _, id := range stored {
				if _, ok := queued[id]; ok {
					items = append(items, requeueItem{id: id})
				} else {
					// 没有返回行：任务已由其他节点写回终态。
					ackIDs = append(ackIDs, id)
				}
			}
		}
	}
	s.requeue(ctx, batch.bucket, batch.token, 0, items)
	s.ack(ctx, batch.bucket, batch.token, ackIDs)
}
