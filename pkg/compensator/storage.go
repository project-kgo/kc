package compensator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// claimed 是被当前执行者以 token 持有租约的任务。
type claimed struct {
	Task
	token int64
	// exhausted 表示该任务在领取时因预算耗尽被直接标记为 failed，不应执行。
	exhausted bool
}

// 调度逻辑通过该接口访问数据库，便于在单元测试中替换。
type storage interface {
	insert(ctx context.Context, tx *sqlx.Tx, tasks []Task) error
	// claim 只领取 types 中的任务类型。
	claim(ctx context.Context, limit int, token int64, types []string) ([]claimed, error)
	finish(ctx context.Context, c claimed, status Status, lastError string, delay time.Duration) error
	abandon(ctx context.Context, c claimed) error
	get(ctx context.Context, id uuid.UUID) (Record, error)
	retry(ctx context.Context, id uuid.UUID) error
	cleanup(ctx context.Context, status Status, olderThan time.Duration, limit int) (int64, error)
	stats(ctx context.Context) (Stats, error)
}

type pgStorage struct {
	db       *sqlx.DB
	config   resolvedConfig
	taskName string
	tasks    string // 已带 schema 并加引号的完整表名

	insertSQL, claimSQL, finishSQL string
}

func newPGStorage(db *sqlx.DB, c resolvedConfig) (*pgStorage, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: database is nil", ErrInvalidConfig)
	}
	if name := db.DriverName(); name != "pgx" && name != "pgx/v5" {
		return nil, fmt.Errorf("%w: unsupported driver %q, only pgx is supported", ErrInvalidConfig, name)
	}
	return &pgStorage{db: db, config: c, taskName: "compensator_" + c.TableSuffix}, nil
}

// setNames 在确定 schema 后预先拼好热路径 SQL，避免每次调用重复格式化。
func (s *pgStorage) setNames(schema string) {
	s.tasks = quoteIdent(schema) + "." + quoteIdent(s.taskName)
	// 以 text[] 传参再在 SQL 中转换，避免依赖驱动对 uuid 数组的二进制编码。
	s.insertSQL = `INSERT INTO ` + s.tasks + ` (id, task_type, payload, status, attempts, next_run_at, created_at, updated_at)
SELECT u.id::uuid, u.task_type, u.payload, 0, 0, now(), now(), now()
FROM unnest($1::text[], $2::text[], $3::text[]) AS u(id, task_type, payload)`
	// 领取：走 due_idx 取最早到期的任务，SKIP LOCKED 使各节点并发领取互不阻塞。
	// 只领取本节点注册了回调的类型，滚动发布新增类型时旧节点不会领取后因缺少回调而失败。
	// 写入新令牌和租约到期时间后语句即提交，回调在事务外执行。
	// 预算已耗尽却因崩溃停留在 running 的任务直接转为 failed。
	const exhausted = `t.attempts >= $3`
	s.claimSQL = `WITH picked AS (
  SELECT id FROM ` + s.tasks + ` WHERE status < 2 AND next_run_at <= now() AND task_type = ANY($5::text[])
  ORDER BY next_run_at LIMIT $1
  FOR UPDATE SKIP LOCKED
)
UPDATE ` + s.tasks + ` t SET
  status = CASE WHEN ` + exhausted + ` THEN 3 ELSE 1 END,
  attempts = CASE WHEN ` + exhausted + ` THEN t.attempts ELSE t.attempts + 1 END,
  lease_token = CASE WHEN ` + exhausted + ` THEN 0 ELSE $2::bigint END,
  next_run_at = CASE WHEN ` + exhausted + ` THEN t.next_run_at ELSE now() + $4::bigint * interval '1 millisecond' END,
  last_error = CASE WHEN ` + exhausted + ` THEN 'retry budget exhausted after lease expiration' ELSE t.last_error END,
  completed_at = CASE WHEN ` + exhausted + ` THEN now() END,
  updated_at = now()
FROM picked WHERE t.id = picked.id
RETURNING t.id, t.task_type, t.payload, t.attempts, t.status`
	s.finishSQL = `UPDATE ` + s.tasks + ` SET status = $3::int2, lease_token = 0, last_error = $4, updated_at = now(),
  next_run_at = CASE WHEN $3::int2 = 0 THEN now() + $5::bigint * interval '1 millisecond' ELSE next_run_at END,
  completed_at = CASE WHEN $3::int2 >= 2 THEN now() END
WHERE id = $1::text::uuid AND status = 1 AND lease_token = $2`
}

// insert 在业务事务内写入立即可领取的待执行任务。
func (s *pgStorage) insert(ctx context.Context, tx *sqlx.Tx, tasks []Task) error {
	ids, types, payloads := make([]string, len(tasks)), make([]string, len(tasks)), make([]string, len(tasks))
	for i, t := range tasks {
		ids[i], types[i], payloads[i] = t.ID.String(), t.Type, t.Payload
	}
	_, err := tx.ExecContext(ctx, s.insertSQL, ids, types, payloads)
	return err
}

func (s *pgStorage) claim(ctx context.Context, limit int, token int64, types []string) ([]claimed, error) {
	rows, err := s.db.QueryContext(ctx, s.claimSQL, limit, token, s.config.retries+1, s.config.LeaseDuration.Milliseconds(), types)
	if err != nil {
		return nil, err
	}
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

// finish 按令牌落库执行结果；令牌不匹配说明租约已被接管，旧结果不会覆盖新状态。
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

// abandon 归还因组件关闭而中断的任务，不消耗执行次数。
func (s *pgStorage) abandon(ctx context.Context, c claimed) error {
	_, err := s.db.ExecContext(ctx, "UPDATE "+s.tasks+` SET status = 0, attempts = attempts - 1, lease_token = 0,
  next_run_at = now(), updated_at = now() WHERE id = $1::text::uuid AND status = 1 AND lease_token = $2`, c.ID.String(), c.token)
	return err
}

func (s *pgStorage) get(ctx context.Context, id uuid.UUID) (Record, error) {
	var (
		r         Record
		status    int16
		completed sql.NullTime
	)
	err := s.db.QueryRowContext(ctx, `SELECT id, task_type, payload, status, attempts, next_run_at,
  last_error, created_at, updated_at, completed_at FROM `+s.tasks+` WHERE id = $1::text::uuid`, id.String()).
		Scan(&r.ID, &r.Type, &r.Payload, &status, &r.Attempts, &r.NextRunAt, &r.LastError, &r.CreatedAt, &r.UpdatedAt, &completed)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	if err != nil {
		return Record{}, err
	}
	r.Attempt, r.Status, r.CompletedAt = r.Attempts, Status(status), completed.Time
	return r, nil
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

// cleanup 删除超过保留期的终态记录，走 done_idx。候选行加 FOR UPDATE SKIP LOCKED：
// 多个节点并发清理时互不阻塞；Retry 正在修改的行被跳过，已提交的修改会让该行
// 按新版本重新判断条件，因此不会误删刚被重置为 pending 的任务。
func (s *pgStorage) cleanup(ctx context.Context, status Status, olderThan time.Duration, limit int) (int64, error) {
	result, err := s.db.ExecContext(ctx, "DELETE FROM "+s.tasks+" WHERE id IN (SELECT id FROM "+s.tasks+
		` WHERE status = $1::int2 AND completed_at < now() - $2::bigint * interval '1 millisecond'
  LIMIT $3 FOR UPDATE SKIP LOCKED)`, int16(status), olderThan.Milliseconds(), limit)
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
