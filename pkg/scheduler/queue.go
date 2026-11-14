package scheduler

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	// pop / reclaim 单次上限，避免 unpack 参数过多。
	maxPopBatch    = 1000
	reclaimBatch   = 500
	reclaimHold    = 10 * time.Second
	reconcileBatch = 1000
)

// bucketKeys 是一个桶在 Redis 中的三个 key。花括号是 hash tag，保证同一个桶的三个 key
// 在 Redis Cluster 中落在同一个 slot。
type bucketKeys struct {
	ready, processing, data string
}

type queue struct {
	rdb     redis.UniversalClient
	keys    []bucketKeys
	scripts scriptSet
}

type scriptSet struct {
	enqueue, pop, ack, requeue, reclaim, enqueueVolatile, cancel *redis.Script
}

func (s *scriptSet) all() []*redis.Script {
	return []*redis.Script{s.enqueue, s.pop, s.ack, s.requeue, s.reclaim, s.enqueueVolatile, s.cancel}
}

// poppedItem 是弹出一个任务：data 非空表示它是易失任务。
type poppedItem struct {
	id   uuid.UUID
	data string
	// runAt 是 ready 中的到期时间（Redis 时钟），只有 pop 返回；reclaim 的结果中为零值。
	runAt time.Time
}

// leaseBatch 是同一次 pop / reclaim 返回的一批任务，它们共享一个租约令牌。
type leaseBatch struct {
	bucket int
	token  int64
	items  []poppedItem
	// malformed 是无法解码为任务 ID 的成员数，只可能来自外部写入；它们被跳过，不影响同批任务。
	malformed int
}

// requeueItem 是放回 ready 的一项；data 非空时同时覆盖易失任务的内容。
type requeueItem struct {
	id   uuid.UUID
	data string
}

func newQueue(rdb redis.UniversalClient, c Config) (*queue, error) {
	if rdb == nil {
		return nil, fmt.Errorf("%w: redis client is nil", ErrInvalidConfig)
	}
	q := &queue{rdb: rdb, keys: make([]bucketKeys, c.Buckets)}
	for i := range q.keys {
		q.keys[i] = bucketKeys{
			ready:      fmt.Sprintf("%s:{%s:%d}:ready", c.KeyPrefix, c.TableSuffix, i),
			processing: fmt.Sprintf("%s:{%s:%d}:processing", c.KeyPrefix, c.TableSuffix, i),
			data:       fmt.Sprintf("%s:{%s:%d}:data", c.KeyPrefix, c.TableSuffix, i),
		}
	}
	q.scripts = scriptSet{
		enqueue: redis.NewScript(enqueueScript), pop: redis.NewScript(popScript),
		ack: redis.NewScript(ackScript), requeue: redis.NewScript(requeueScript),
		reclaim: redis.NewScript(reclaimScript), enqueueVolatile: redis.NewScript(enqueueVolatileScript),
		cancel: redis.NewScript(cancelVolatileScript),
	}
	return q, nil
}

// load 预加载脚本，减少首轮调用的 EVALSHA 未命中。SCRIPT LOAD 不带 key，在 Cluster 客户端上
// 只会发到一个节点，所以 Cluster 下逐个主节点加载。
func (q *queue) load(ctx context.Context) error {
	loadAll := func(ctx context.Context, c redis.Scripter) error {
		for _, script := range q.scripts.all() {
			if err := script.Load(ctx, c).Err(); err != nil {
				return fmt.Errorf("scheduler: load script: %w", err)
			}
		}
		return nil
	}
	if cluster, ok := q.rdb.(*redis.ClusterClient); ok {
		return cluster.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
			return loadAll(ctx, client)
		})
	}
	return loadAll(ctx, q.rdb)
}

// bucket 用 fnv1a32 把任务 ID 映射到桶。Buckets 上线后不可直接修改。
func (q *queue) bucket(id uuid.UUID) int {
	const offset32, prime32 = 2166136261, 16777619
	hash := uint32(offset32)
	for _, b := range id {
		hash = (hash ^ uint32(b)) * prime32
	}
	return int(hash % uint32(len(q.keys)))
}

// enqueue 把任务写入 ready，已在 ready 或 processing 中的成员跳过，可安全重复调用。
func (q *queue) enqueue(ctx context.Context, tasks []queuedTask) (int, error) {
	if len(tasks) == 0 {
		return 0, nil
	}
	grouped := make([][]any, len(q.keys))
	for _, task := range tasks {
		bucket := q.bucket(task.id)
		grouped[bucket] = append(grouped[bucket], task.runAt.UnixMilli(), task.id[:])
	}
	// Pipeline 内无法在 EVALSHA 未命中时回退，而脚本缓存可能因重启、主从切换或 Cluster 中
	// 某个节点未加载而缺失，所以这里直接用 EVAL 发送脚本正文。脚本很短，每轮最多一桶一条。
	pipe := q.rdb.Pipeline()
	cmds := make([]*redis.Cmd, 0, len(q.keys))
	for bucket, args := range grouped {
		if len(args) == 0 {
			continue
		}
		keys := q.keys[bucket]
		cmds = append(cmds, q.scripts.enqueue.Eval(ctx, pipe, []string{keys.ready, keys.processing}, args...))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	total := 0
	for _, cmd := range cmds {
		n, err := cmd.Int64()
		if err != nil {
			return total, err
		}
		total += int(n)
	}
	return total, nil
}

// enqueueVolatile 一次写入易失任务的内容和到期时间。到期时间由脚本用 Redis 时钟加 delay
// 算出：到期判断也用 Redis 时钟，这样短延迟任务的触发时刻不受节点与 Redis 时钟差的影响。
func (q *queue) enqueueVolatile(ctx context.Context, id uuid.UUID, delay time.Duration, data string) error {
	keys := q.keys[q.bucket(id)]
	return q.scripts.enqueueVolatile.Run(ctx, q.rdb, []string{keys.ready, keys.data},
		max(0, delay.Milliseconds()), id[:], data).Err()
}

// pop 弹出已到期的任务并写入 processing，返回租约令牌和易失任务内容。
func (q *queue) pop(ctx context.Context, bucket, limit int, lease time.Duration) (leaseBatch, error) {
	keys := q.keys[bucket]
	cmd := q.scripts.pop.Run(ctx, q.rdb, []string{keys.ready, keys.processing, keys.data},
		limit, lease.Milliseconds())
	return readLease(cmd, bucket)
}

// ack 在令牌匹配时移除执行结果已落库的任务，并删除易失任务的内容。
func (q *queue) ack(ctx context.Context, bucket int, token int64, ids []uuid.UUID) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	keys := q.keys[bucket]
	args := make([]any, 0, len(ids)+1)
	args = append(args, token)
	for _, id := range ids {
		args = append(args, id[:])
	}
	return q.scripts.ack.Run(ctx, q.rdb, []string{keys.processing, keys.data}, args...).Int()
}

// requeue 在令牌匹配时把任务放回 ready，新的到期时间为当前 Redis 时间加 delay。
func (q *queue) requeue(ctx context.Context, bucket int, token int64, delay time.Duration, items []requeueItem) (int, error) {
	if len(items) == 0 {
		return 0, nil
	}
	keys := q.keys[bucket]
	args := make([]any, 0, 2+len(items)*2)
	args = append(args, token, delay.Milliseconds())
	for _, item := range items {
		args = append(args, item.id[:], item.data)
	}
	return q.scripts.requeue.Run(ctx, q.rdb, []string{keys.ready, keys.processing, keys.data}, args...).Int()
}

// reclaim 取出租约过期的任务并顺延一小段时间，避免多个节点同时处理同一批。
func (q *queue) reclaim(ctx context.Context, bucket, limit int) (leaseBatch, error) {
	keys := q.keys[bucket]
	cmd := q.scripts.reclaim.Run(ctx, q.rdb, []string{keys.processing, keys.data},
		limit, reclaimHold.Milliseconds())
	return readLease(cmd, bucket)
}

// cancel 在任务尚未开始执行时删除它；返回 false 表示不存在或已开始执行。
func (q *queue) cancel(ctx context.Context, id uuid.UUID) (bool, error) {
	keys := q.keys[q.bucket(id)]
	n, err := q.scripts.cancel.Run(ctx, q.rdb, []string{keys.ready, keys.data}, id[:]).Int()
	return n == 1, err
}

// redisStats 是 Redis 侧的积压快照。
type redisStats struct {
	readyDue   int64
	processing int64
}

func (q *queue) stats(ctx context.Context) (redisStats, error) {
	now := strconv.FormatInt(time.Now().UnixMilli(), 10)
	pipe := q.rdb.Pipeline()
	due := make([]*redis.IntCmd, len(q.keys))
	processing := make([]*redis.IntCmd, len(q.keys))
	for i, keys := range q.keys {
		due[i] = pipe.ZCount(ctx, keys.ready, "-inf", now)
		processing[i] = pipe.ZCard(ctx, keys.processing)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return redisStats{}, err
	}
	var stats redisStats
	for i := range q.keys {
		stats.readyDue += due[i].Val()
		stats.processing += processing[i].Val()
	}
	return stats, nil
}

func readLease(cmd *redis.Cmd, bucket int) (leaseBatch, error) {
	raw, err := cmd.Result()
	if err != nil {
		return leaseBatch{}, err
	}
	parts, ok := raw.([]any)
	if !ok || len(parts) < 3 {
		return leaseBatch{}, fmt.Errorf("scheduler: unexpected %s reply", cmd.Name())
	}
	token, ok := parts[0].(int64)
	if !ok {
		return leaseBatch{}, fmt.Errorf("scheduler: unexpected %s token", cmd.Name())
	}
	batch := leaseBatch{bucket: bucket, token: token}
	ids, _ := parts[1].([]any)
	datas, _ := parts[2].([]any)
	var scores []any
	if len(parts) > 3 {
		scores, _ = parts[3].([]any)
	}
	for i, value := range ids {
		text, _ := value.(string)
		id, err := uuid.FromBytes([]byte(text))
		if err != nil {
			// 成员已被脚本移入 processing，这里让整批失败会连累同批的正常任务，
			// 也会让 reclaimer 在该桶永久卡住，所以只跳过并计数。
			batch.malformed++
			continue
		}
		item := poppedItem{id: id}
		if i < len(datas) && datas[i] != nil {
			item.data, _ = datas[i].(string)
		}
		if i < len(scores) {
			if text, ok := scores[i].(string); ok {
				if ms, err := strconv.ParseFloat(text, 64); err == nil {
					item.runAt = time.UnixMilli(int64(ms))
				}
			}
		}
		batch.items = append(batch.items, item)
	}
	return batch, nil
}

const enqueueScript = `
local n = 0
for i = 1, #ARGV, 2 do
  if not redis.call('ZSCORE', KEYS[1], ARGV[i + 1])
     and not redis.call('ZSCORE', KEYS[2], ARGV[i + 1]) then
    redis.call('ZADD', KEYS[1], ARGV[i], ARGV[i + 1])
    n = n + 1
  end
end
return n
`

const popScript = `
-- Redis 6.2 需要显式切换到效果复制，才能在 TIME 之后执行写命令。
if redis.replicate_commands then redis.replicate_commands() end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local res = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'WITHSCORES', 'LIMIT', 0, ARGV[1])
local token = now + tonumber(ARGV[2])
local ids, scores = {}, {}
for i = 1, #res, 2 do
  ids[#ids + 1] = res[i]
  scores[#scores + 1] = res[i + 1]
  redis.call('ZREM', KEYS[1], res[i])
  redis.call('ZADD', KEYS[2], token, res[i])
end
local datas = {}
if #ids > 0 then datas = redis.call('HMGET', KEYS[3], unpack(ids)) end
-- scores 是原到期时间（毫秒），用于统计触发延迟。
return {token, ids, datas, scores}
`

const ackScript = `
local n = 0
for i = 2, #ARGV do
  local s = redis.call('ZSCORE', KEYS[1], ARGV[i])
  if s and tonumber(s) == tonumber(ARGV[1]) then
    redis.call('HDEL', KEYS[2], ARGV[i])
    n = n + redis.call('ZREM', KEYS[1], ARGV[i])
  end
end
return n
`

const requeueScript = `
-- Redis 6.2 需要显式切换到效果复制，才能在 TIME 之后执行写命令。
if redis.replicate_commands then redis.replicate_commands() end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local n = 0
for i = 3, #ARGV, 2 do
  local s = redis.call('ZSCORE', KEYS[2], ARGV[i])
  if s and tonumber(s) == tonumber(ARGV[1]) then
    redis.call('ZREM', KEYS[2], ARGV[i])
    redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[i])
    if ARGV[i + 1] ~= '' then redis.call('HSET', KEYS[3], ARGV[i], ARGV[i + 1]) end
    n = n + 1
  end
end
return n
`

const reclaimScript = `
-- Redis 6.2 需要显式切换到效果复制，才能在 TIME 之后执行写命令。
if redis.replicate_commands then redis.replicate_commands() end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, ARGV[1])
local token = now + tonumber(ARGV[2])
for _, id in ipairs(ids) do
  redis.call('ZADD', KEYS[1], token, id)
end
local datas = {}
if #ids > 0 then datas = redis.call('HMGET', KEYS[2], unpack(ids)) end
return {token, ids, datas}
`

const enqueueVolatileScript = `
-- Redis 6.2 需要显式切换到效果复制，才能在 TIME 之后执行写命令。
if redis.replicate_commands then redis.replicate_commands() end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
redis.call('ZADD', KEYS[1], now + tonumber(ARGV[1]), ARGV[2])
return 1
`

const cancelVolatileScript = `
redis.call('HDEL', KEYS[2], ARGV[1])
return redis.call('ZREM', KEYS[1], ARGV[1])
`
