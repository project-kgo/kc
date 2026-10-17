package outbox

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jmoiron/sqlx"
)

type task struct {
	Record
	LeaseToken string `db:"lease_token"`
}
type shardLease struct {
	ShardID int    `db:"shard_id"`
	OwnerID string `db:"owner_id"`
	Token   string `db:"lease_token"`
	Until   int64  `db:"lease_until"`
}

// 将调度与 SQL 隔离，故障处理和生命周期测试无需依赖真实数据库。
type storage interface {
	insert(context.Context, *sqlx.Tx, Message) (int64, error)
	get(context.Context, int64) (Record, error)
	claimIDs(context.Context, []int64, int) ([]task, error)
	claimShard(context.Context, shardLease, int) ([]task, error)
	readyShards(context.Context, []shardLease) ([]shardLease, error)
	finish(context.Context, task, error, time.Duration) (Status, error)
	retry(context.Context, int64) error
	coordinate(context.Context, string) ([]shardLease, error)
	cleanup(context.Context, shardLease) (int64, error)
	release(context.Context, string) error
}

type sqlStorage struct {
	db                              *sqlx.DB
	config                          resolvedConfig
	postgres                        bool
	schema, messages, nodes, shards string
}

func newSQLStorage(db *sqlx.DB, c resolvedConfig) (*sqlStorage, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: database is nil", ErrInvalidConfig)
	}
	if db.DriverName() != "pgx" && db.DriverName() != "mysql" {
		return nil, fmt.Errorf("%w: unsupported driver %q", ErrInvalidConfig, db.DriverName())
	}
	suffix := ""
	if c.TableSuffix != "" {
		suffix = "_" + c.TableSuffix
	}
	return &sqlStorage{db: db, config: c, postgres: db.DriverName() == "pgx", messages: "outbox" + suffix, nodes: "outbox_nodes" + suffix, shards: "outbox_shards" + suffix}, nil
}

func (s *sqlStorage) bind(query string) string { return s.db.Rebind(query) }

func (s *sqlStorage) nowExpression() string {
	if s.postgres {
		return "FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT"
	}
	return "CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS SIGNED)"
}

func (s *sqlStorage) now(ctx context.Context, exec sqlExecutor) (int64, error) {
	var now int64
	err := exec.QueryRowxContext(ctx, "SELECT "+s.nowExpression()).Scan(&now)
	return now, err
}

func (s *sqlStorage) fields() string {
	payload := "payload"
	if s.postgres {
		payload = "payload::text AS payload"
	}
	return "id,message_type," + payload + ",shard_id,status,attempts,next_attempt_at,lease_token,lease_until,last_error,created_at,updated_at,completed_at"
}

func readRows[T any](ctx context.Context, exec sqlExecutor, query string, args ...any) ([]T, error) {
	rows, err := exec.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []T
	for rows.Next() {
		var v T
		if err = rows.StructScan(&v); err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	return values, rows.Err()
}

func (s *sqlStorage) insert(ctx context.Context, tx *sqlx.Tx, m Message) (int64, error) {
	value := "?"
	if s.postgres {
		value = "CAST(? AS JSONB)"
	}
	now := s.nowExpression()
	query := fmt.Sprintf(`INSERT INTO %s (message_type,payload,shard_id,status,attempts,next_attempt_at,
		lease_token,lease_until,last_error,created_at,updated_at,completed_at)
		VALUES (?,%s,?,0,0,%s,'',0,'',%s,%s,0)`, s.table(s.messages), value, now, now, now)
	// 分片在插入时确定并持久化，不依赖节点数量，避免扩缩容搬迁消息。
	args := []any{m.Type, m.Payload, mathrand.IntN(shardCount)}
	if s.postgres {
		var id int64
		err := tx.QueryRowxContext(ctx, s.bind(query+" RETURNING id"), args...).Scan(&id)
		return id, err
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (s *sqlStorage) get(ctx context.Context, id int64) (Record, error) {
	var row task
	err := s.db.QueryRowxContext(ctx, s.bind("SELECT "+s.fields()+" FROM "+s.table(s.messages)+" WHERE id=?"), id).StructScan(&row)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	return row.Record, err
}

func (s *sqlStorage) begin(ctx context.Context) (*sqlx.Tx, error) {
	return s.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

func (s *sqlStorage) lockShard(ctx context.Context, tx *sqlx.Tx, lease shardLease) (int64, error) {
	var current shardLease
	err := tx.QueryRowxContext(ctx, s.bind("SELECT shard_id,owner_id,lease_token,lease_until FROM "+s.table(s.shards)+" WHERE shard_id=? FOR UPDATE"), lease.ShardID).StructScan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrLeaseLost
	}
	if err != nil {
		return 0, err
	}
	// 在获得行锁后读取数据库时间，不能使用等待锁之前的过期判断。
	now, err := s.now(ctx, tx)
	if err != nil {
		return 0, err
	}
	if current.OwnerID != lease.OwnerID || current.Token != lease.Token || current.Until <= now {
		return 0, ErrLeaseLost
	}
	return now, nil
}

func (s *sqlStorage) claimIDs(ctx context.Context, ids []int64, limit int) ([]task, error) {
	if len(ids) == 0 || limit <= 0 {
		return nil, nil
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	now, err := s.now(ctx, tx)
	if err != nil {
		return nil, err
	}
	query, args, err := sqlx.In("SELECT "+s.fields()+" FROM "+s.table(s.messages)+
		" WHERE id IN (?) AND ((status=0 AND next_attempt_at<=?) OR (status=1 AND lease_until<=?)) ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED", ids, now, now, limit)
	if err != nil {
		return nil, err
	}
	rows, err := readRows[task](ctx, tx, s.bind(query), args...)
	if err != nil {
		return nil, err
	}
	claimed, err := s.markClaimed(ctx, tx, rows, now)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return claimed, nil
}

func (s *sqlStorage) claimShard(ctx context.Context, lease shardLease, limit int) ([]task, error) {
	if limit <= 0 {
		return nil, nil
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	now, err := s.lockShard(ctx, tx, lease)
	if err != nil {
		return nil, err
	}
	// 分开查询两种状态，让两个扫描都能直接使用各自的时间索引。
	rows, err := readRows[task](ctx, tx, s.bind("SELECT "+s.fields()+" FROM "+s.table(s.messages)+
		" WHERE shard_id=? AND status=1 AND lease_until<=? ORDER BY lease_until,id LIMIT ? FOR UPDATE SKIP LOCKED"), lease.ShardID, now, limit)
	if err != nil {
		return nil, err
	}
	if len(rows) < limit {
		pending, e := readRows[task](ctx, tx, s.bind("SELECT "+s.fields()+" FROM "+s.table(s.messages)+
			" WHERE shard_id=? AND status=0 AND next_attempt_at<=? ORDER BY next_attempt_at,id LIMIT ? FOR UPDATE SKIP LOCKED"), lease.ShardID, now, limit-len(rows))
		if e != nil {
			return nil, e
		}
		rows = append(rows, pending...)
	}
	claimed, err := s.markClaimed(ctx, tx, rows, now)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return claimed, nil
}

// 先用元数据索引找出有到期任务的分片，避免空闲时每轮开启 64 个事务。
func (s *sqlStorage) readyShards(ctx context.Context, leases []shardLease) ([]shardLease, error) {
	if len(leases) == 0 {
		return nil, nil
	}
	now, err := s.now(ctx, s.db)
	if err != nil {
		return nil, err
	}
	ids := make([]int, len(leases))
	for i, lease := range leases {
		ids[i] = lease.ShardID
	}
	// 使用数据库时间的参数值，使 PostgreSQL 能把时间范围也放进 Index Cond；
	// 直接在范围条件中调用 volatile 的 clock_timestamp 会退化为逐行过滤。
	query, args, err := sqlx.In("SELECT sh.shard_id FROM "+s.table(s.shards)+" sh WHERE sh.shard_id IN (?) AND ("+
		"EXISTS (SELECT 1 FROM "+s.table(s.messages)+" m WHERE m.shard_id=sh.shard_id AND m.status=0 AND m.next_attempt_at<=?) OR "+
		"EXISTS (SELECT 1 FROM "+s.table(s.messages)+" m WHERE m.shard_id=sh.shard_id AND m.status=1 AND m.lease_until<=?))", ids, now, now)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryxContext(ctx, s.bind(query), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ready := make(map[int]bool)
	for rows.Next() {
		var id int
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ready[id] = true
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	var result []shardLease
	for _, lease := range leases {
		if ready[lease.ShardID] {
			result = append(result, lease)
		}
	}
	return result, nil
}

func (s *sqlStorage) markClaimed(ctx context.Context, tx *sqlx.Tx, rows []task, now int64) ([]task, error) {
	var exhausted, ids []int64
	var claimed []task
	token := rand.Text()
	for _, row := range rows {
		if row.Attempts >= s.config.retries+1 {
			exhausted = append(exhausted, row.ID)
			continue
		}
		row.Status = StatusProcessing
		row.Attempts++
		row.LeaseToken = token
		row.LeaseUntil = now + s.config.TaskLease.Milliseconds()
		row.UpdatedAt = now
		claimed = append(claimed, row)
		ids = append(ids, row.ID)
	}
	if len(exhausted) > 0 {
		query, args, err := sqlx.In("UPDATE "+s.table(s.messages)+" SET status=3,lease_token='',lease_until=0,updated_at=?,last_error=? WHERE id IN (?)", now, "delivery budget exhausted after lease expiration", exhausted)
		if err != nil {
			return nil, err
		}
		if _, err = tx.ExecContext(ctx, s.bind(query), args...); err != nil {
			return nil, err
		}
	}
	if len(ids) > 0 {
		query, args, err := sqlx.In("UPDATE "+s.table(s.messages)+" SET status=1,attempts=attempts+1,lease_token=?,lease_until=?,updated_at=? WHERE id IN (?)", token, now+s.config.TaskLease.Milliseconds(), now, ids)
		if err != nil {
			return nil, err
		}
		if _, err = tx.ExecContext(ctx, s.bind(query), args...); err != nil {
			return nil, err
		}
	}
	return claimed, nil
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	text := strings.ToValidUTF8(err.Error(), "�")
	if len(text) <= 4096 {
		return text
	}
	text = text[:4096]
	for !utf8.ValidString(text) {
		text = text[:len(text)-1]
	}
	return text
}

func (s *sqlStorage) finish(ctx context.Context, t task, handlerErr error, delay time.Duration) (Status, error) {
	status := StatusSucceeded
	lastError := ""
	completed := s.nowExpression()
	next := "0"
	if handlerErr != nil {
		status = StatusPending
		lastError = errorText(handlerErr)
		completed = "0"
		next = fmt.Sprintf("(%s + %d)", s.nowExpression(), delay.Milliseconds())
		if t.Attempts >= s.config.retries+1 {
			status = StatusFailed
			next = "0"
		}
	}
	query := fmt.Sprintf("UPDATE %s SET status=?,last_error=?,lease_token='',lease_until=0,updated_at=%s,completed_at=%s,next_attempt_at=%s WHERE id=? AND status=1 AND lease_token=? AND lease_until>%s",
		s.table(s.messages), s.nowExpression(), completed, next, s.nowExpression())
	result, err := s.db.ExecContext(ctx, s.bind(query), status, lastError, t.ID, t.LeaseToken)
	if err != nil {
		return StatusProcessing, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return StatusProcessing, err
	}
	if count != 1 {
		return StatusProcessing, ErrLeaseLost
	}
	return status, nil
}

func (s *sqlStorage) retry(ctx context.Context, id int64) error {
	query := fmt.Sprintf("UPDATE %s SET status=0,attempts=0,next_attempt_at=%s,lease_token='',lease_until=0,last_error='',updated_at=%s,completed_at=0 WHERE id=? AND status=3", s.table(s.messages), s.nowExpression(), s.nowExpression())
	result, err := s.db.ExecContext(ctx, s.bind(query), id)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 1 {
		return nil
	}
	if _, err = s.get(ctx, id); err != nil {
		return err
	}
	return ErrNotFailed
}

// assignment 精确均分固定分片，排序在 Go 中执行，避免数据库排序规则差异。
func assignment(nodes []string, node string) []int {
	nodes = append([]string(nil), nodes...)
	sort.Strings(nodes)
	index := sort.SearchStrings(nodes, node)
	if index == len(nodes) || nodes[index] != node {
		return nil
	}
	var shards []int
	for shard := index; shard < shardCount; shard += len(nodes) {
		shards = append(shards, shard)
	}
	return shards
}

func (s *sqlStorage) coordinate(ctx context.Context, node string) ([]shardLease, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	now, err := s.now(ctx, tx)
	if err != nil {
		return nil, err
	}
	until := now + s.config.ShardLease.Milliseconds()
	query := "INSERT INTO " + s.table(s.nodes) + " (node_id,lease_until) VALUES (?,?)"
	if s.postgres {
		query += " ON CONFLICT (node_id) DO UPDATE SET lease_until=EXCLUDED.lease_until"
	} else {
		query += " ON DUPLICATE KEY UPDATE lease_until=VALUES(lease_until)"
	}
	if _, err = tx.ExecContext(ctx, s.bind(query), node, until); err != nil {
		return nil, err
	}
	rows, err := tx.QueryxContext(ctx, s.bind("SELECT node_id FROM "+s.table(s.nodes)+" WHERE lease_until>?"), now)
	if err != nil {
		return nil, err
	}
	var nodes []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		nodes = append(nodes, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	desired := assignment(nodes, node)
	query = "UPDATE " + s.table(s.shards) + " SET owner_id='',lease_token='',lease_until=0 WHERE owner_id=?"
	args := []any{node}
	if len(desired) > 0 {
		query += " AND shard_id NOT IN (?)"
		query, args, err = sqlx.In(query, node, desired)
		if err != nil {
			return nil, err
		}
	}
	if _, err = tx.ExecContext(ctx, s.bind(query), args...); err != nil {
		return nil, err
	}
	// 先提交释放阶段，避免节点 A/B 在交换分片时各自持有对方要领取的锁。
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	tx, err = s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	now, err = s.now(ctx, tx)
	if err != nil {
		return nil, err
	}
	until = now + s.config.ShardLease.Milliseconds()
	if len(desired) > 0 {
		query, args, err = sqlx.In("UPDATE "+s.table(s.shards)+" SET lease_until=? WHERE owner_id=? AND lease_until>? AND shard_id IN (?)", until, node, now, desired)
		if err != nil {
			return nil, err
		}
		if _, err = tx.ExecContext(ctx, s.bind(query), args...); err != nil {
			return nil, err
		}
		query, args, err = sqlx.In("UPDATE "+s.table(s.shards)+" SET owner_id=?,lease_token=?,lease_until=? WHERE lease_until<=? AND shard_id IN (?)", node, rand.Text(), until, now, desired)
		if err != nil {
			return nil, err
		}
		if _, err = tx.ExecContext(ctx, s.bind(query), args...); err != nil {
			return nil, err
		}
	}
	leases, err := readRows[shardLease](ctx, tx, s.bind("SELECT shard_id,owner_id,lease_token,lease_until FROM "+s.table(s.shards)+" WHERE owner_id=? AND lease_until>? ORDER BY shard_id"), node, now)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, s.bind("DELETE FROM "+s.table(s.nodes)+" WHERE lease_until<=?"), now); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return leases, nil
}

func (s *sqlStorage) cleanup(ctx context.Context, lease shardLease) (int64, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	now, err := s.lockShard(ctx, tx, lease)
	if err != nil {
		return 0, err
	}
	rows, err := tx.QueryxContext(ctx, s.bind("SELECT id FROM "+s.table(s.messages)+" WHERE shard_id=? AND status=2 AND completed_at<? ORDER BY completed_at,id LIMIT ? FOR UPDATE SKIP LOCKED"), lease.ShardID, now-s.config.Retention.Milliseconds(), s.config.CleanupBatchSize)
	if err != nil {
		return 0, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	if len(ids) > 0 {
		query, args, err := sqlx.In("DELETE FROM "+s.table(s.messages)+" WHERE id IN (?)", ids)
		if err != nil {
			return 0, err
		}
		if _, err = tx.ExecContext(ctx, s.bind(query), args...); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return int64(len(ids)), nil
}

func (s *sqlStorage) release(ctx context.Context, node string) error {
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, s.bind("UPDATE "+s.table(s.shards)+" SET owner_id='',lease_token='',lease_until=0 WHERE owner_id=?"), node); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, s.bind("DELETE FROM "+s.table(s.nodes)+" WHERE node_id=?"), node); err != nil {
		return err
	}
	return tx.Commit()
}
