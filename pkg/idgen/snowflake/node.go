package snowflake

import (
	"fmt"
	"sync"
	"time"
)

const (
	// WorkerIDBits 是 worker ID 占用的位数。
	WorkerIDBits = 10
	// SequenceBits 是同一毫秒内序列号占用的位数。
	SequenceBits = 12

	// MaxWorkerID 是允许分配的最大 worker ID。
	MaxWorkerID int64 = 1<<WorkerIDBits - 1
	// MaxSequence 是同一毫秒内允许生成的最大序列号。
	MaxSequence int64 = 1<<SequenceBits - 1

	workerIDShift  = SequenceBits
	timestampShift = SequenceBits + WorkerIDBits
	maxTimestamp   = 1<<(63-timestampShift) - 1

	// DefaultEpoch 是默认纪元：2020-01-01 00:00:00 UTC，单位为毫秒。
	DefaultEpoch int64 = 1577836800000
)

type node struct {
	mu        sync.Mutex
	timestamp int64
	workerID  int64
	sequence  int64
	epoch     int64
	now       func() time.Time
	sleep     func(time.Duration)
}

func newNode(workerID, epoch int64) (*node, error) {
	if workerID < 0 || workerID > MaxWorkerID {
		return nil, fmt.Errorf("%w: worker ID %d is outside 0-%d", ErrInvalidConfig, workerID, MaxWorkerID)
	}
	if epoch == 0 {
		epoch = DefaultEpoch
	}
	return &node{
		workerID: workerID,
		epoch:    epoch,
		now:      time.Now,
		sleep:    time.Sleep,
	}, nil
}

func (n *node) generate() (int64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	current := n.now().UnixMilli()
	if current < n.timestamp {
		return 0, fmt.Errorf("%w: current timestamp %d is behind %d", ErrClockBackwards, current, n.timestamp)
	}

	if current == n.timestamp {
		if n.sequence == MaxSequence {
			// 当前毫秒的序列号已用尽，等待时钟进入下一毫秒。
			for current <= n.timestamp {
				n.sleep(time.Millisecond)
				current = n.now().UnixMilli()
				if current < n.timestamp {
					return 0, fmt.Errorf("%w: current timestamp %d is behind %d", ErrClockBackwards, current, n.timestamp)
				}
			}
			n.sequence = 0
		} else {
			n.sequence++
		}
	} else {
		n.sequence = 0
	}

	elapsed := current - n.epoch
	if elapsed < 0 {
		return 0, fmt.Errorf("%w: epoch %d is later than current timestamp %d", ErrInvalidConfig, n.epoch, current)
	}
	if elapsed > maxTimestamp {
		return 0, fmt.Errorf("%w: elapsed timestamp %d exceeds %d", ErrTimestampOverflow, elapsed, maxTimestamp)
	}

	n.timestamp = current
	return (elapsed << timestampShift) | (n.workerID << workerIDShift) | n.sequence, nil
}

func (n *node) latestTimestamp() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.timestamp
}

func (n *node) getTimeFromID(id int64) int64 {
	return (id >> timestampShift) + n.epoch
}
