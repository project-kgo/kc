package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// insertTask 是一条待写入的任务，status 在登记时按是否近期任务确定。
type insertTask struct {
	task   Task
	status Status
}

// queuedTask 是已处于 queued、需要写入 Redis ready 的任务。
type queuedTask struct {
	id    uuid.UUID
	runAt time.Time
}

// storedTask 是弹出后从 PostgreSQL 取回的任务内容。
type storedTask struct {
	task  Task
	runAt time.Time
}

// reclaimedTask 是租约回收后仍留在 queued 的任务；其余状态不再放回 Redis。
type reclaimedTask struct {
	id     uuid.UUID
	status Status
}

// reconcileCursor 是 reconciler 的 (run_at, id) 游标。
type reconcileCursor struct {
	runAt time.Time
	id    uuid.UUID
}

// 调度逻辑通过该接口访问数据库，便于在集成测试中注入故障。
type storage interface {
	insert(ctx context.Context, exec sqlx.ExtContext, tasks []insertTask) ([]queuedTask, error)
	load(ctx context.Context, limit int) ([]queuedTask, error)
	fetch(ctx context.Context, ids []uuid.UUID) ([]storedTask, error)
	complete(ctx context.Context, id uuid.UUID) (bool, error)
	fail(ctx context.Context, id uuid.UUID, maxAttempts int, lastError string, delay time.Duration) (Status, bool, error)
	cancel(ctx context.Context, id uuid.UUID) error
	retry(ctx context.Context, id uuid.UUID) error
	get(ctx context.Context, id uuid.UUID) (Record, error)
	reclaim(ctx context.Context, ids []uuid.UUID, maxAttempts int) ([]reclaimedTask, error)
	reconcile(ctx context.Context, cursor reconcileCursor, limit int) ([]queuedTask, error)
	cleanup(ctx context.Context, status Status, olderThan time.Duration, limit int) (int64, error)
	stats(ctx context.Context) (Stats, error)
}

type pgStorage struct {
	db        *sqlx.DB
	config    Config
	tableName string
	tasks     string // 已带 schema 并加引号的完整表名

	insertSQL, loadSQL, fetchSQL, completeSQL, failSQL, cancelSQL, retrySQL, getSQL,
	reclaimSQL, reconcileSQL, cleanupSQL, statsSQL, statusSQL string
}

func newPGStorage(db *sqlx.DB, c Config) (*pgStorage, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: database is nil", ErrInvalidConfig)
	}
	if name := db.DriverName(); name != "pgx" && name != "pgx/v5" {
		return nil, fmt.Errorf("%w: unsupported driver %q, only pgx is supported", ErrInvalidConfig, name)
	}
	return &pgStorage{db: db, config: c, tableName: "scheduler_tasks_" + c.TableSuffix}, nil
}

// setNames 在确定 schema 后预先拼好热路径 SQL，避免每次调用重复格式化。
func (s *pgStorage) setNames(schema string) {
	s.tasks = quoteIdent(schema) + "." + quoteIdent(s.tableName)
	// 以 text[] 传参再在 SQL 中转换，避免依赖驱动对 uuid 数组的二进制编码。
	const anyID = `id IN (SELECT u::uuid FROM unnest($1::text[]) AS u)`
	s.insertSQL = `INSERT INTO ` + s.tasks + ` (id, task_type, payload, status, run_at, created_at, updated_at)
SELECT u.id::uuid, u.task_type, u.payload, u.status::smallint,
  CASE WHEN u.run_at = '' THEN now() ELSE u.run_at::timestamptz END, now(), now()
FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[]) AS u(id, task_type, payload, run_at, status)
RETURNING id, status, run_at`
	s.loadSQL = `UPDATE ` + s.tasks + ` SET status = 1, updated_at = now()
WHERE id IN (
  SELECT id FROM ` + s.tasks + `
  WHERE status = 0 AND run_at < now() + $1::bigint * interval '1 millisecond'
  ORDER BY run_at LIMIT $2
  FOR UPDATE SKIP LOCKED)
RETURNING id, run_at`
	// 第 2 步同时承担取 payload 和过滤失效成员两个职责。
	s.fetchSQL = `SELECT id, task_type, payload, attempts, run_at FROM ` + s.tasks + `
WHERE ` + anyID + ` AND status = 1`
	s.completeSQL = `UPDATE ` + s.tasks + ` SET status = 2, completed_at = now(), updated_at = now()
WHERE id = $1::text::uuid AND status = 1`
	s.failSQL = `UPDATE ` + s.tasks + ` SET attempts = attempts + 1, last_error = $2, updated_at = now(),
  status = CASE WHEN attempts + 1 >= $3 THEN 3 ELSE 1 END,
  run_at = CASE WHEN attempts + 1 >= $3 THEN run_at ELSE now() + $4::bigint * interval '1 millisecond' END,
  completed_at = CASE WHEN attempts + 1 >= $3 THEN now() END
WHERE id = $1::text::uuid AND status = 1
RETURNING status`
	s.cancelSQL = `UPDATE ` + s.tasks + ` SET status = 4, completed_at = now(), updated_at = now()
WHERE id = $1::text::uuid AND status IN (0, 1)`
	s.retrySQL = `UPDATE ` + s.tasks + ` SET status = 0, attempts = 0, last_error = '', run_at = now(),
  completed_at = NULL, updated_at = now()
WHERE id = $1::text::uuid AND status = 3`
	s.getSQL = `SELECT id, task_type, payload, status, attempts, last_error, run_at, created_at, updated_at, completed_at
FROM ` + s.tasks + ` WHERE id = $1::text::uuid`
	s.reclaimSQL = `UPDATE ` + s.tasks + ` SET attempts = attempts + 1, updated_at = now(),
  last_error = CASE WHEN attempts + 1 >= $2 THEN 'lease expired' ELSE last_error END,
  status = CASE WHEN attempts + 1 >= $2 THEN 3 ELSE 1 END,
  completed_at = CASE WHEN attempts + 1 >= $2 THEN now() END
WHERE ` + anyID + ` AND status = 1
RETURNING id, status`
	s.reconcileSQL = `SELECT id, run_at FROM ` + s.tasks + `
WHERE status = 1 AND run_at < now() - $1::bigint * interval '1 millisecond'
  AND (run_at, id) > ($2::timestamptz, $3::text::uuid)
ORDER BY run_at, id LIMIT $4`
	// status >= 2 是字面量条件：status = $1 是参数，通用计划下计划器无法据此选用终态部分索引。
	s.cleanupSQL = `DELETE FROM ` + s.tasks + ` WHERE id IN (
  SELECT id FROM ` + s.tasks + `
  WHERE status = $1::smallint AND status >= 2
    AND completed_at < now() - $2::bigint * interval '1 millisecond'
  ORDER BY completed_at LIMIT $3
  FOR UPDATE SKIP LOCKED)`
	s.statsSQL = `SELECT count(*) FILTER (WHERE status = 0), count(*) FILTER (WHERE status = 1),
  count(*) FILTER (WHERE status = 3),
  COALESCE((GREATEST(extract(epoch FROM now() - (min(run_at) FILTER (WHERE status = 1))), 0) * 1000)::bigint, 0)
FROM ` + s.tasks + ` WHERE status IN (0, 1, 3)`
	s.statusSQL = `SELECT status FROM ` + s.tasks + ` WHERE id = $1::text::uuid`
}

// insert 写入任务。RunAt 为零时以数据库时钟取 now()，表示提交后立即可入队；
// RETURNING 同时给出需要写入 Redis 的 queued 行及其数据库侧到期时间。
func (s *pgStorage) insert(ctx context.Context, exec sqlx.ExtContext, tasks []insertTask) ([]queuedTask, error) {
	ids := make([]string, len(tasks))
	types := make([]string, len(tasks))
	payloads := make([]string, len(tasks))
	runAts := make([]string, len(tasks))
	statuses := make([]string, len(tasks))
	for i, t := range tasks {
		ids[i], types[i], payloads[i] = t.task.ID.String(), t.task.Type, t.task.Payload
		statuses[i] = strconv.Itoa(int(t.status))
		if !t.task.RunAt.IsZero() {
			runAts[i] = t.task.RunAt.UTC().Format(time.RFC3339Nano)
		}
	}
	rows, err := exec.QueryxContext(ctx, s.insertSQL, ids, types, payloads, runAts, statuses)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var queued []queuedTask
	for rows.Next() {
		var (
			task queuedTask
			st   int16
		)
		if err = rows.Scan(&task.id, &st, &task.runAt); err != nil {
			return nil, err
		}
		if Status(st) == StatusQueued {
			queued = append(queued, task)
		}
	}
	return queued, rows.Err()
}

// load 把进入窗口的 pending 任务改为 queued，一条语句完成抢占和状态标记。
func (s *pgStorage) load(ctx context.Context, limit int) ([]queuedTask, error) {
	return s.queryQueued(ctx, s.loadSQL, s.config.Window.Milliseconds(), limit)
}

func (s *pgStorage) fetch(ctx context.Context, ids []uuid.UUID) ([]storedTask, error) {
	rows, err := s.db.QueryxContext(ctx, s.fetchSQL, uuidTexts(ids))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []storedTask
	for rows.Next() {
		var task storedTask
		if err = rows.Scan(&task.task.ID, &task.task.Type, &task.task.Payload, &task.task.Attempts, &task.runAt); err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

// complete 只在任务仍为 queued 时置为成功；返回 false 说明状态已被取消或其它节点写回。
func (s *pgStorage) complete(ctx context.Context, id uuid.UUID) (bool, error) {
	result, err := s.db.ExecContext(ctx, s.completeSQL, id.String())
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// fail 记录一次失败：有剩余次数时推后 run_at 并保持 queued，耗尽时置为 failed。
// 返回写入后的状态；updated 为 false 表示没有行满足 status = 1。
func (s *pgStorage) fail(ctx context.Context, id uuid.UUID, maxAttempts int, lastError string, delay time.Duration) (Status, bool, error) {
	var status int16
	err := s.db.QueryRowxContext(ctx, s.failSQL, id.String(), lastError, maxAttempts, delay.Milliseconds()).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return Status(status), true, nil
}

// cancel 把 pending / queued 任务改为 cancelled；不存在或已是终态时返回对应的错误。
func (s *pgStorage) cancel(ctx context.Context, id uuid.UUID) error {
	result, err := s.db.ExecContext(ctx, s.cancelSQL, id.String())
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	return s.existence(ctx, id)
}

// retry 把失败任务重置为 pending 并清零尝试次数，由 loader 重新入队。
func (s *pgStorage) retry(ctx context.Context, id uuid.UUID) error {
	result, err := s.db.ExecContext(ctx, s.retrySQL, id.String())
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	return s.existence(ctx, id)
}

// existence 把「没有任何行受影响」细分为任务不存在与任务已是终态。
func (s *pgStorage) existence(ctx context.Context, id uuid.UUID) error {
	var status int16
	if err := s.db.QueryRowxContext(ctx, s.statusSQL, id.String()).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	return ErrFinished
}

func (s *pgStorage) get(ctx context.Context, id uuid.UUID) (Record, error) {
	var (
		r         Record
		status    int16
		completed sql.NullTime
	)
	err := s.db.QueryRowxContext(ctx, s.getSQL, id.String()).Scan(&r.ID, &r.Type, &r.Payload, &status,
		&r.Attempts, &r.LastError, &r.RunAt, &r.CreatedAt, &r.UpdatedAt, &completed)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	if err != nil {
		return Record{}, err
	}
	r.Status, r.CompletedAt = Status(status), completed.Time
	return r, nil
}

// reclaim 把租约过期的任务计一次尝试；仍有剩余的保持 queued，由调用方立即放回。
func (s *pgStorage) reclaim(ctx context.Context, ids []uuid.UUID, maxAttempts int) ([]reclaimedTask, error) {
	rows, err := s.db.QueryxContext(ctx, s.reclaimSQL, uuidTexts(ids), maxAttempts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []reclaimedTask
	for rows.Next() {
		var (
			task   reclaimedTask
			status int16
		)
		if err = rows.Scan(&task.id, &status); err != nil {
			return nil, err
		}
		task.status = Status(status)
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

// reconcile 按 (run_at, id) 游标分页扫描逾期未完成的任务。
func (s *pgStorage) reconcile(ctx context.Context, cursor reconcileCursor, limit int) ([]queuedTask, error) {
	return s.queryQueued(ctx, s.reconcileSQL, s.config.ReconcileGrace.Milliseconds(),
		cursor.runAt, cursor.id.String(), limit)
}

func (s *pgStorage) cleanup(ctx context.Context, status Status, olderThan time.Duration, limit int) (int64, error) {
	result, err := s.db.ExecContext(ctx, s.cleanupSQL, int16(status), olderThan.Milliseconds(), limit)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *pgStorage) stats(ctx context.Context) (Stats, error) {
	var (
		stats   Stats
		overdue int64
	)
	if err := s.db.QueryRowxContext(ctx, s.statsSQL).Scan(&stats.Pending, &stats.Queued, &stats.Failed, &overdue); err != nil {
		return Stats{}, err
	}
	stats.OldestOverdue = time.Duration(overdue) * time.Millisecond
	return stats, nil
}

func (s *pgStorage) queryQueued(ctx context.Context, query string, args ...any) ([]queuedTask, error) {
	rows, err := s.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []queuedTask
	for rows.Next() {
		var task queuedTask
		if err = rows.Scan(&task.id, &task.runAt); err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

func uuidTexts(ids []uuid.UUID) []string {
	values := make([]string, len(ids))
	for i, id := range ids {
		values[i] = id.String()
	}
	return values
}

// errorText 截断错误信息到 1024 字节，避免异常长的错误写入数据库。
func errorText(err error) string {
	if err == nil {
		return ""
	}
	text := strings.ToValidUTF8(err.Error(), "�")
	if len(text) <= 1024 {
		return text
	}
	text = text[:1024]
	for !utf8.ValidString(text) {
		text = text[:len(text)-1]
	}
	return text
}
