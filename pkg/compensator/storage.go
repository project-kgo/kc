package compensator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// newRow 是待插入的一条任务，status/attempts/delay 由调用方按模式与首次执行结果决定。
type newRow struct {
	id       uuid.UUID
	taskType string
	payload  string
	shard    int16
	status   Status
	attempts int32
	delay    time.Duration // next_run_at = 数据库当前时间 + delay
	token    int64
	err      string
}

// claimed 是被当前执行者以 token 持有租约的任务。
type claimed struct {
	Task
	token int64
	// exhausted 表示该任务在领取时因预算耗尽被直接标记为 failed，不应执行。
	exhausted bool
}

// 调度逻辑通过该接口访问数据库，便于在单元测试中替换。
type storage interface {
	insert(ctx context.Context, tx *sqlx.Tx, rows []newRow) error
	claim(ctx context.Context, shards []int16, limit int, token int64) ([]claimed, error)
	claimIDs(ctx context.Context, ids []uuid.UUID, ownToken, token int64) ([]claimed, error)
	finish(ctx context.Context, c claimed, status Status, lastError string, delay time.Duration) error
	abandon(ctx context.Context, c claimed) error
	get(ctx context.Context, id uuid.UUID) (Record, error)
	statuses(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]Status, error)
	retry(ctx context.Context, id uuid.UUID) error
	cleanup(ctx context.Context, shards []int16, status Status, olderThan time.Duration, limit int) (int64, error)
	stats(ctx context.Context) (Stats, error)
	coordinate(ctx context.Context, node string) ([]int16, error)
	release(ctx context.Context, node string) error
}

type pgStorage struct {
	db                  *sqlx.DB
	config              resolvedConfig
	taskName, leaseName string
	tasks, leases       string // 已带 schema 并加引号的完整表名

	claimSQL, claimIDsSQL, insertSQL, finishSQL string
}

func newPGStorage(db *sqlx.DB, c resolvedConfig) (*pgStorage, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: database is nil", ErrInvalidConfig)
	}
	if name := db.DriverName(); name != "pgx" && name != "pgx/v5" {
		return nil, fmt.Errorf("%w: unsupported driver %q, only pgx is supported", ErrInvalidConfig, name)
	}
	taskName := "compensator_" + c.TableSuffix
	return &pgStorage{db: db, config: c, taskName: taskName, leaseName: taskName + "_lease"}, nil
}

// setNames 在确定 schema 后预先拼好热路径 SQL，避免每次调用重复格式化。
func (s *pgStorage) setNames(schema string) {
	s.tasks = quoteIdent(schema) + "." + quoteIdent(s.taskName)
	s.leases = quoteIdent(schema) + "." + quoteIdent(s.leaseName)
	// 以 text[] 传参再在 SQL 中转换，避免依赖驱动对 uuid 数组的二进制编码。
	s.insertSQL = `INSERT INTO ` + s.tasks + ` (id, task_type, payload, shard_id, status, attempts, next_run_at,
  lease_token, last_error, created_at, updated_at, completed_at)
SELECT u.id::uuid, u.task_type, u.payload, u.shard_id, u.status, u.attempts,
  clock_timestamp() + u.delay_ms * interval '1 millisecond', u.token, u.err, now(), now(),
  CASE WHEN u.status >= 2 THEN now() END
FROM unnest($1::text[], $2::text[], $3::text[], $4::int2[], $5::int2[], $6::int4[], $7::int8[], $8::int8[], $9::text[])
  AS u(id, task_type, payload, shard_id, status, attempts, delay_ms, token, err)`
	// 领取：按分片逐个走 (shard_id, next_run_at) 部分索引，SKIP LOCKED 保证多执行者互不阻塞；
	// 外层 LIMIT 使后续分片的 LATERAL 子查询惰性执行。已耗尽预算的过期 running 任务直接转为 failed。
	s.claimSQL = `WITH picked AS (
  SELECT c.id FROM unnest($1::int2[]) AS s(shard_id)
  CROSS JOIN LATERAL (
    SELECT id FROM ` + s.tasks + `
    WHERE shard_id = s.shard_id AND status < 2 AND next_run_at <= now()
    ORDER BY next_run_at LIMIT $2
    FOR UPDATE SKIP LOCKED
  ) c
  LIMIT $2
)
UPDATE ` + s.tasks + ` t SET ` + claimSet("t.attempts >= $4", "t.attempts + 1") + `
FROM picked WHERE t.id = picked.id
RETURNING t.id, t.task_type, t.payload, t.attempts, t.status`
	// 按 ID 领取：本进程登记且仍持有初始令牌的任务（AfterCommit 首次执行，不再增加次数），
	// 或已经到期的任务。
	s.claimIDsSQL = `WITH picked AS (
  SELECT id FROM ` + s.tasks + `
  WHERE id = ANY($1::text[]::uuid[]) AND ((status = 1 AND lease_token = $2::bigint) OR (status < 2 AND next_run_at <= now()))
  FOR UPDATE SKIP LOCKED
)
UPDATE ` + s.tasks + ` t SET ` + claimSet("(t.lease_token <> $2 AND t.attempts >= $4)",
		"CASE WHEN t.lease_token = $2 THEN t.attempts ELSE t.attempts + 1 END") + `
FROM picked WHERE t.id = picked.id
RETURNING t.id, t.task_type, t.payload, t.attempts, t.status`
	s.finishSQL = `UPDATE ` + s.tasks + ` SET status = $3::int2, lease_token = 0, last_error = $4, updated_at = now(),
  next_run_at = CASE WHEN $3::int2 = 0 THEN now() + $5::bigint * interval '1 millisecond' ELSE next_run_at END,
  completed_at = CASE WHEN $3::int2 >= 2 THEN now() END
WHERE id = $1::text::uuid AND status = 1 AND lease_token = $2`
}

func (s *pgStorage) insert(ctx context.Context, tx *sqlx.Tx, rows []newRow) error {
	n := len(rows)
	ids, types, payloads, errs := make([]string, n), make([]string, n), make([]string, n), make([]string, n)
	shards, statuses := make([]int16, n), make([]int16, n)
	attempts := make([]int32, n)
	delays, tokens := make([]int64, n), make([]int64, n)
	for i, r := range rows {
		ids[i], types[i], payloads[i], errs[i] = r.id.String(), r.taskType, r.payload, r.err
		shards[i], statuses[i], attempts[i] = r.shard, int16(r.status), r.attempts
		delays[i], tokens[i] = r.delay.Milliseconds(), r.token
	}
	_, err := tx.ExecContext(ctx, s.insertSQL, ids, types, payloads, shards, statuses, attempts, delays, tokens, errs)
	return err
}

func (s *pgStorage) readClaimed(rows *sql.Rows, token int64) ([]claimed, error) {
	defer rows.Close()
	var result []claimed
	for rows.Next() {
		var (
			c      claimed
			status int16
		)
		if err := rows.Scan(&c.ID, &c.Type, &c.Payload, &c.Attempt, &status); err != nil {
			return nil, err
		}
		c.token = token
		c.exhausted = Status(status) != StatusRunning
		result = append(result, c)
	}
	return result, rows.Err()
}

func (s *pgStorage) claim(ctx context.Context, shards []int16, limit int, token int64) ([]claimed, error) {
	if len(shards) == 0 || limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, s.claimSQL, shards, limit, token, s.config.retries+1, s.config.LeaseDuration.Milliseconds())
	if err != nil {
		return nil, err
	}
	return s.readClaimed(rows, token)
}

func (s *pgStorage) claimIDs(ctx context.Context, ids []uuid.UUID, ownToken, token int64) ([]claimed, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	text := make([]string, len(ids))
	for i, id := range ids {
		text[i] = id.String()
	}
	rows, err := s.db.QueryContext(ctx, s.claimIDsSQL, text, ownToken, token, s.config.retries+1, s.config.LeaseDuration.Milliseconds())
	if err != nil {
		return nil, err
	}
	return s.readClaimed(rows, token)
}

func (s *pgStorage) finish(ctx context.Context, c claimed, status Status, lastError string, delay time.Duration) error {
	result, err := s.db.ExecContext(ctx, s.finishSQL, c.ID.String(), c.token, int16(status), lastError, delay.Milliseconds())
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil {
		return err
	} else if n != 1 {
		return ErrLeaseLost
	}
	return nil
}

// abandon 归还尚未开始执行的任务，不消耗执行次数，用于关闭时的本地积压。
func (s *pgStorage) abandon(ctx context.Context, c claimed) error {
	_, err := s.db.ExecContext(ctx, "UPDATE "+s.tasks+` SET status = 0, attempts = attempts - 1, lease_token = 0,
  next_run_at = now(), updated_at = now() WHERE id = $1::text::uuid AND status = 1 AND lease_token = $2`, c.ID.String(), c.token)
	return err
}

func (s *pgStorage) get(ctx context.Context, id uuid.UUID) (Record, error) {
	var (
		r         Record
		shard     int16
		status    int16
		completed sql.NullTime
	)
	err := s.db.QueryRowContext(ctx, `SELECT id, task_type, payload, shard_id, status, attempts, next_run_at,
  last_error, created_at, updated_at, completed_at FROM `+s.tasks+` WHERE id = $1::text::uuid`, id.String()).
		Scan(&r.ID, &r.Type, &r.Payload, &shard, &status, &r.Attempts, &r.NextRunAt, &r.LastError, &r.CreatedAt, &r.UpdatedAt, &completed)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	if err != nil {
		return Record{}, err
	}
	r.Attempt, r.ShardID, r.Status, r.CompletedAt = r.Attempts, int(shard), Status(status), completed.Time
	return r, nil
}

func (s *pgStorage) statuses(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]Status, error) {
	text := make([]string, len(ids))
	for i, id := range ids {
		text[i] = id.String()
	}
	rows, err := s.db.QueryContext(ctx, "SELECT id, status FROM "+s.tasks+" WHERE id = ANY($1::text[]::uuid[])", text)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[uuid.UUID]Status, len(ids))
	for rows.Next() {
		var (
			id     uuid.UUID
			status int16
		)
		if err = rows.Scan(&id, &status); err != nil {
			return nil, err
		}
		result[id] = Status(status)
	}
	return result, rows.Err()
}

func (s *pgStorage) retry(ctx context.Context, id uuid.UUID) error {
	result, err := s.db.ExecContext(ctx, "UPDATE "+s.tasks+` SET status = 0, attempts = 0, lease_token = 0, last_error = '',
  next_run_at = now(), completed_at = NULL, updated_at = now() WHERE id = $1::text::uuid AND status = 3`, id.String())
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil {
		return err
	} else if n == 1 {
		return nil
	}
	if _, err = s.get(ctx, id); err != nil {
		return err
	}
	return ErrNotFailed
}

// cleanup 只删除本节点持有分片中的终态记录，走 (shard_id, status, completed_at) 部分索引。
func (s *pgStorage) cleanup(ctx context.Context, shards []int16, status Status, olderThan time.Duration, limit int) (int64, error) {
	if len(shards) == 0 {
		return 0, nil
	}
	result, err := s.db.ExecContext(ctx, "DELETE FROM "+s.tasks+" WHERE id IN (SELECT id FROM "+s.tasks+
		` WHERE shard_id = ANY($1::int2[]) AND status = $2::int2 AND completed_at < now() - $3::bigint * interval '1 millisecond' LIMIT $4)`,
		shards, int16(status), olderThan.Milliseconds(), limit)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// stats 用 due_idx 统计未完成任务，用 done_idx 统计失败任务。
func (s *pgStorage) stats(ctx context.Context) (Stats, error) {
	var (
		st     Stats
		oldest sql.NullTime
	)
	err := s.db.QueryRowContext(ctx, `SELECT
  count(*) FILTER (WHERE status = 0), count(*) FILTER (WHERE status = 1), min(next_run_at) FILTER (WHERE status = 0),
  (SELECT count(*) FROM `+s.tasks+` WHERE status = 3)
FROM `+s.tasks+` WHERE status < 2`).Scan(&st.Pending, &st.Running, &oldest, &st.Failed)
	st.OldestDue = oldest.Time
	return st, err
}

// coordinate 续约节点心跳并调整本节点持有的分片，使各节点持有数量趋于 ceil(64/节点数)。
// 只修改自己的行或用 SKIP LOCKED 抢占过期行，节点之间不会互相等待或死锁。
func (s *pgStorage) coordinate(ctx context.Context, node string) ([]int16, error) {
	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	ttl := s.config.NodeTTL.Milliseconds()
	if _, err = tx.ExecContext(ctx, "INSERT INTO "+s.leases+` (kind, key, owner_id, expires_at)
  VALUES (1, $1, $1, now() + $2::bigint * interval '1 millisecond')
  ON CONFLICT (kind, key) DO UPDATE SET expires_at = EXCLUDED.expires_at`, node, ttl); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM "+s.leases+" WHERE (kind, key) IN (SELECT kind, key FROM "+s.leases+
		" WHERE kind = 1 AND expires_at < now() FOR UPDATE SKIP LOCKED)"); err != nil {
		return nil, err
	}
	var nodes int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM "+s.leases+" WHERE kind = 1 AND expires_at >= now()").Scan(&nodes); err != nil {
		return nil, err
	}
	target := (shardCount + max(nodes, 1) - 1) / max(nodes, 1)
	owned, err := queryShards(ctx, tx, "UPDATE "+s.leases+` SET expires_at = now() + $2::bigint * interval '1 millisecond'
  WHERE kind = 0 AND owner_id = $1 RETURNING key`, node, ttl)
	if err != nil {
		return nil, err
	}
	slices.Sort(owned)
	if len(owned) > target {
		excess := owned[target:]
		keys := make([]string, len(excess))
		for i, shard := range excess {
			keys[i] = strconv.Itoa(int(shard))
		}
		if _, err = tx.ExecContext(ctx, "UPDATE "+s.leases+` SET owner_id = '', expires_at = '-infinity'
  WHERE kind = 0 AND owner_id = $1 AND key = ANY($2::text[])`, node, keys); err != nil {
			return nil, err
		}
		owned = owned[:target]
	} else if len(owned) < target {
		acquired, err := queryShards(ctx, tx, "UPDATE "+s.leases+` SET owner_id = $1, expires_at = now() + $2::bigint * interval '1 millisecond'
  WHERE kind = 0 AND key IN (SELECT key FROM `+s.leases+` WHERE kind = 0 AND expires_at < now()
    ORDER BY random() LIMIT $3 FOR UPDATE SKIP LOCKED) RETURNING key`, node, ttl, target-len(owned))
		if err != nil {
			return nil, err
		}
		owned = append(owned, acquired...)
		slices.Sort(owned)
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return owned, nil
}

func queryShards(ctx context.Context, tx *sqlx.Tx, query string, args ...any) ([]int16, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var shards []int16
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			return nil, err
		}
		shard, err := strconv.ParseInt(key, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("compensator: invalid shard key %q: %w", key, err)
		}
		shards = append(shards, int16(shard))
	}
	return shards, rows.Err()
}

func (s *pgStorage) release(ctx context.Context, node string) error {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE "+s.leases+" SET owner_id = '', expires_at = '-infinity' WHERE kind = 0 AND owner_id = $1", node); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM "+s.leases+" WHERE kind = 1 AND key = $1", node); err != nil {
		return err
	}
	return tx.Commit()
}

// claimSet 生成领取时的 SET 子句：$3 为新令牌，$4 为总执行预算，$5 为租约毫秒数。
// exhausted 为真的行是预算已耗尽却因崩溃停留在 running 的任务，直接转为 failed。
func claimSet(exhausted, nextAttempts string) string {
	return `status = CASE WHEN ` + exhausted + ` THEN 3 ELSE 1 END,
  attempts = CASE WHEN ` + exhausted + ` THEN t.attempts ELSE ` + nextAttempts + ` END,
  lease_token = CASE WHEN ` + exhausted + ` THEN 0 ELSE $3::bigint END,
  next_run_at = CASE WHEN ` + exhausted + ` THEN t.next_run_at ELSE now() + $5::bigint * interval '1 millisecond' END,
  last_error = CASE WHEN ` + exhausted + ` THEN 'retry budget exhausted after lease expiration' ELSE t.last_error END,
  completed_at = CASE WHEN ` + exhausted + ` THEN now() END,
  updated_at = now()`
}

// errorText 截断错误信息，避免异常长的错误写入数据库。
func errorText(err error) string {
	if err == nil {
		return ""
	}
	text := strings.ToValidUTF8(err.Error(), "�")
	if len(text) <= 2048 {
		return text
	}
	text = text[:2048]
	for !utf8.ValidString(text) {
		text = text[:len(text)-1]
	}
	return text
}
