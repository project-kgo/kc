package snowflake

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
)

const (
	defaultTableName        = "snowflake_workers"
	defaultHeartbeat        = 3 * time.Second
	defaultWorkerTimeout    = 15 * time.Second
	defaultSafetyThreshold  = 10 * time.Second
	defaultCloseTimeout     = 5 * time.Second
	maxStartupClockRollback = 5 * time.Second
	workerAllocationRetries = 3
	maxIdentifierLength     = 63
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type resolvedConfig struct {
	namespace string
	tableName string
	table     string
	epoch     int64
}

type allocation struct {
	workerID      int64
	lastTimestamp int64
	leaseToken    string
}

type storage struct {
	db    *sqlx.DB
	table string
}

func resolveConfig(config Config, now time.Time) (resolvedConfig, error) {
	tableName := config.TableName
	if tableName == "" {
		tableName = defaultTableName
	}
	if err := validateIdentifier("table name", tableName); err != nil {
		return resolvedConfig{}, err
	}
	if config.Namespace != "" {
		if err := validateIdentifier("namespace", config.Namespace); err != nil {
			return resolvedConfig{}, err
		}
	}

	epoch := config.Epoch
	if epoch == 0 {
		epoch = DefaultEpoch
	}
	current := now.UnixMilli()
	if epoch < 0 || epoch > current {
		return resolvedConfig{}, fmt.Errorf("%w: epoch %d must be between 0 and current timestamp %d", ErrInvalidConfig, epoch, current)
	}
	if current-epoch > maxTimestamp {
		return resolvedConfig{}, fmt.Errorf("%w: epoch %d has exhausted the timestamp bits", ErrInvalidConfig, epoch)
	}

	table := tableName
	if config.Namespace != "" {
		table = config.Namespace + "." + tableName
	}
	return resolvedConfig{
		namespace: config.Namespace,
		tableName: tableName,
		table:     table,
		epoch:     epoch,
	}, nil
}

func validateIdentifier(field, value string) error {
	if len(value) > maxIdentifierLength || !identifierPattern.MatchString(value) {
		return fmt.Errorf("%w: %s %q is not a safe SQL identifier", ErrInvalidConfig, field, value)
	}
	return nil
}

func newStorage(db *sqlx.DB, table string) (*storage, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: database is nil", ErrInvalidConfig)
	}
	switch db.DriverName() {
	case "mysql", "pgx":
	default:
		return nil, fmt.Errorf("%w: SQL driver %q is not supported", ErrInvalidConfig, db.DriverName())
	}
	return &storage{db: db, table: table}, nil
}

func (s *storage) rebind(query string) string {
	return s.db.Rebind(query)
}

func (s *storage) ensureTable(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		worker_id INTEGER NOT NULL PRIMARY KEY,
		last_timestamp BIGINT NOT NULL,
		ip_address VARCHAR(45) NOT NULL,
		updated_at BIGINT NOT NULL,
		lease_token VARCHAR(64) NOT NULL
	)`, s.table)
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("create worker table: %w", err)
	}

	// CREATE TABLE IF NOT EXISTS 不会校验既有表结构，这里提前给出明确错误。
	checkQuery := fmt.Sprintf(
		"SELECT worker_id, last_timestamp, ip_address, updated_at, lease_token FROM %s WHERE 1 = 0",
		s.table,
	)
	if _, err := s.db.ExecContext(ctx, checkQuery); err != nil {
		return fmt.Errorf("validate worker table: %w", err)
	}
	return nil
}

func (s *storage) allocateWorker(ctx context.Context) (allocation, error) {
	var lastErr error
	for attempt := 0; attempt < workerAllocationRetries; attempt++ {
		allocated, retry, err := s.allocateWorkerOnce(ctx)
		if err == nil {
			return allocated, nil
		}
		lastErr = err
		if !retry && !isRetryableTransaction(err) {
			return allocation{}, err
		}
	}
	return allocation{}, fmt.Errorf("allocate worker after %d attempts: %w", workerAllocationRetries, lastErr)
}

func (s *storage) allocateWorkerOnce(ctx context.Context) (allocated allocation, retry bool, err error) {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return allocation{}, false, fmt.Errorf("begin worker allocation: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) && err == nil {
			err = fmt.Errorf("rollback worker allocation: %w", rollbackErr)
		}
	}()

	now := time.Now()
	leaseToken, err := newLeaseToken()
	if err != nil {
		return allocation{}, false, err
	}
	ipAddress := localIP()

	globalTimestamp, err := s.maxTimestamp(ctx, tx)
	if err != nil {
		return allocation{}, false, err
	}

	var workerID, workerTimestamp int64
	reclaimQuery := s.rebind(fmt.Sprintf(
		"SELECT worker_id, last_timestamp FROM %s WHERE updated_at < ? ORDER BY updated_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED",
		s.table,
	))
	err = tx.QueryRowxContext(ctx, reclaimQuery, now.Add(-defaultWorkerTimeout).UnixMilli()).Scan(&workerID, &workerTimestamp)
	if err == nil {
		updateQuery := s.rebind(fmt.Sprintf(
			"UPDATE %s SET last_timestamp = ?, ip_address = ?, updated_at = ?, lease_token = ? WHERE worker_id = ?",
			s.table,
		))
		if _, err = tx.ExecContext(ctx, updateQuery, now.UnixMilli(), ipAddress, now.UnixMilli(), leaseToken, workerID); err != nil {
			return allocation{}, false, fmt.Errorf("reclaim worker %d: %w", workerID, err)
		}
		if err = tx.Commit(); err != nil {
			return allocation{}, false, fmt.Errorf("commit reclaimed worker %d: %w", workerID, err)
		}
		return allocation{
			workerID:      workerID,
			lastTimestamp: max(workerTimestamp, globalTimestamp),
			leaseToken:    leaseToken,
		}, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return allocation{}, false, fmt.Errorf("find expired worker: %w", err)
	}

	var maximumWorkerID int64
	maxWorkerQuery := fmt.Sprintf("SELECT worker_id FROM %s ORDER BY worker_id DESC LIMIT 1 FOR UPDATE", s.table)
	err = tx.QueryRowxContext(ctx, maxWorkerQuery).Scan(&maximumWorkerID)
	nextWorkerID := int64(0)
	switch {
	case err == nil:
		nextWorkerID = maximumWorkerID + 1
	case errors.Is(err, sql.ErrNoRows):
		// 空表从 worker 0 开始分配。
	default:
		return allocation{}, false, fmt.Errorf("find maximum worker ID: %w", err)
	}
	if nextWorkerID > MaxWorkerID {
		return allocation{}, false, ErrNoWorkerID
	}

	insertQuery := s.rebind(fmt.Sprintf(
		"INSERT INTO %s (worker_id, last_timestamp, ip_address, updated_at, lease_token) VALUES (?, ?, ?, ?, ?)",
		s.table,
	))
	if _, err = tx.ExecContext(ctx, insertQuery, nextWorkerID, now.UnixMilli(), ipAddress, now.UnixMilli(), leaseToken); err != nil {
		if isDuplicateKey(err) {
			return allocation{}, true, fmt.Errorf("worker %d was allocated concurrently: %w", nextWorkerID, err)
		}
		return allocation{}, false, fmt.Errorf("insert worker %d: %w", nextWorkerID, err)
	}
	if err = tx.Commit(); err != nil {
		if isDuplicateKey(err) {
			return allocation{}, true, fmt.Errorf("worker %d was allocated concurrently: %w", nextWorkerID, err)
		}
		return allocation{}, false, fmt.Errorf("commit worker %d: %w", nextWorkerID, err)
	}
	return allocation{workerID: nextWorkerID, lastTimestamp: globalTimestamp, leaseToken: leaseToken}, false, nil
}

func (s *storage) maxTimestamp(ctx context.Context, tx *sqlx.Tx) (int64, error) {
	query := fmt.Sprintf("SELECT COALESCE(MAX(last_timestamp), 0) FROM %s", s.table)
	var timestamp int64
	if err := tx.QueryRowxContext(ctx, query).Scan(&timestamp); err != nil {
		return 0, fmt.Errorf("read maximum timestamp: %w", err)
	}
	return timestamp, nil
}

func (s *storage) heartbeat(ctx context.Context, workerID int64, leaseToken string, timestamp int64) error {
	now := time.Now().UnixMilli()
	if timestamp < now {
		timestamp = now
	}
	query := s.rebind(fmt.Sprintf(
		"UPDATE %s SET last_timestamp = ?, updated_at = ? WHERE worker_id = ? AND lease_token = ?",
		s.table,
	))
	result, err := s.db.ExecContext(ctx, query, timestamp, now, workerID, leaseToken)
	if err != nil {
		return fmt.Errorf("update worker heartbeat: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read heartbeat affected rows: %w", err)
	}
	if rows != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *storage) release(ctx context.Context, workerID int64, leaseToken string, timestamp int64) error {
	now := time.Now().UnixMilli()
	if timestamp < now {
		timestamp = now
	}
	query := s.rebind(fmt.Sprintf(
		"UPDATE %s SET last_timestamp = ?, updated_at = 0 WHERE worker_id = ? AND lease_token = ?",
		s.table,
	))
	if _, err := s.db.ExecContext(ctx, query, timestamp, workerID, leaseToken); err != nil {
		return fmt.Errorf("release worker %d: %w", workerID, err)
	}
	return nil
}

func newLeaseToken() (string, error) {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("create lease token: %w", err)
	}
	return hex.EncodeToString(buffer), nil
}

func isDuplicateKey(err error) bool {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return true
	}
	var postgresErr *pgconn.PgError
	return errors.As(err, &postgresErr) && postgresErr.Code == "23505"
}

func isRetryableTransaction(err error) bool {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		// 1205: lock wait timeout；1213: deadlock。
		return mysqlErr.Number == 1205 || mysqlErr.Number == 1213
	}
	var postgresErr *pgconn.PgError
	if errors.As(err, &postgresErr) {
		// 40001: serialization failure；40P01: deadlock detected。
		return postgresErr.Code == "40001" || postgresErr.Code == "40P01"
	}
	return false
}

func localIP() string {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return "unknown"
	}
	for _, address := range addresses {
		ipNet, ok := address.(*net.IPNet)
		if ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
			return ipNet.IP.String()
		}
	}
	return "unknown"
}

func validateResourceName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%w: resource name is empty", ErrInvalidConfig)
	}
	return nil
}
