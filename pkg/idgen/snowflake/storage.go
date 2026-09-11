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
	workerAllocationRetries = 8
	allocationRetryBase     = 10 * time.Millisecond
	allocationRetryMax      = 250 * time.Millisecond
	maxIdentifierLength     = 63

	coordinatorWorkerID   int64 = -1
	coordinatorUpdatedAt  int64 = 1<<63 - 1
	coordinatorIPAddress        = "metadata"
	coordinatorLeaseToken       = "snowflake-coordinator-v1"
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
	db         *sqlx.DB
	driverName string
	namespace  string
	tableName  string
	table      string
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

func newStorage(db *sqlx.DB, config resolvedConfig) (*storage, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: database is nil", ErrInvalidConfig)
	}
	driverName := db.DriverName()
	switch driverName {
	case "mysql", "pgx":
	default:
		return nil, fmt.Errorf("%w: SQL driver %q is not supported", ErrInvalidConfig, driverName)
	}
	metadataNamespace := config.namespace
	metadataTableName := config.tableName
	if driverName == "pgx" {
		// PostgreSQL 未加引号的标识符会统一折叠为小写。
		metadataNamespace = strings.ToLower(metadataNamespace)
		metadataTableName = strings.ToLower(metadataTableName)
	}
	return &storage{
		db:         db,
		driverName: driverName,
		namespace:  metadataNamespace,
		tableName:  metadataTableName,
		table:      config.table,
	}, nil
}

func (s *storage) rebind(query string) string {
	return s.db.Rebind(query)
}

func (s *storage) allocateWorker(ctx context.Context, epoch int64) (allocation, error) {
	var lastErr error
	for attempt := 0; attempt < workerAllocationRetries; attempt++ {
		allocated, retry, err := s.allocateWorkerOnce(ctx, epoch)
		if err == nil {
			return allocated, nil
		}
		lastErr = err
		if !retry && !isRetryableTransaction(err) {
			return allocation{}, err
		}
		if attempt == workerAllocationRetries-1 {
			break
		}
		if err := waitAllocationRetry(ctx, attempt); err != nil {
			return allocation{}, err
		}
	}
	return allocation{}, fmt.Errorf("allocate worker after %d attempts: %w", workerAllocationRetries, lastErr)
}

func waitAllocationRetry(ctx context.Context, attempt int) error {
	delay := allocationRetryBase << attempt
	if delay > allocationRetryMax {
		delay = allocationRetryMax
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *storage) allocateWorkerOnce(ctx context.Context, epoch int64) (allocated allocation, retry bool, err error) {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return allocation{}, false, fmt.Errorf("begin worker allocation: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) && err == nil {
			err = fmt.Errorf("rollback worker allocation: %w", rollbackErr)
		}
	}()

	if err = s.lockCoordinator(ctx, tx, epoch); err != nil {
		return allocation{}, false, err
	}
	databaseNow, err := s.databaseNow(ctx, tx)
	if err != nil {
		return allocation{}, false, err
	}
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
		"SELECT worker_id, last_timestamp FROM %s WHERE worker_id BETWEEN ? AND ? AND updated_at < ? ORDER BY updated_at ASC LIMIT 1 FOR UPDATE",
		s.table,
	))
	err = tx.QueryRowxContext(
		ctx,
		reclaimQuery,
		int64(0),
		MaxWorkerID,
		databaseNow-defaultWorkerTimeout.Milliseconds(),
	).Scan(&workerID, &workerTimestamp)
	if err == nil {
		barrierTimestamp := max(workerTimestamp, globalTimestamp, databaseNow)
		updateQuery := s.rebind(fmt.Sprintf(
			"UPDATE %s SET last_timestamp = ?, ip_address = ?, updated_at = ?, lease_token = ? WHERE worker_id = ?",
			s.table,
		))
		if _, err = tx.ExecContext(ctx, updateQuery, barrierTimestamp, ipAddress, databaseNow, leaseToken, workerID); err != nil {
			return allocation{}, false, fmt.Errorf("reclaim worker %d: %w", workerID, err)
		}
		if err = tx.Commit(); err != nil {
			return allocation{}, isDuplicateKey(err), fmt.Errorf("commit reclaimed worker %d: %w", workerID, err)
		}
		return allocation{workerID: workerID, lastTimestamp: barrierTimestamp, leaseToken: leaseToken}, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return allocation{}, false, fmt.Errorf("find expired worker: %w", err)
	}

	nextWorkerID, err := s.findAvailableWorkerID(ctx, tx)
	if err != nil {
		return allocation{}, false, err
	}
	barrierTimestamp := max(globalTimestamp, databaseNow)
	insertQuery := s.rebind(fmt.Sprintf(
		"INSERT INTO %s (worker_id, last_timestamp, ip_address, updated_at, lease_token) VALUES (?, ?, ?, ?, ?)",
		s.table,
	))
	if _, err = tx.ExecContext(ctx, insertQuery, nextWorkerID, barrierTimestamp, ipAddress, databaseNow, leaseToken); err != nil {
		if isDuplicateKey(err) {
			return allocation{}, true, fmt.Errorf("worker %d was allocated concurrently: %w", nextWorkerID, err)
		}
		return allocation{}, false, fmt.Errorf("insert worker %d: %w", nextWorkerID, err)
	}
	if err = tx.Commit(); err != nil {
		return allocation{}, isDuplicateKey(err), fmt.Errorf("commit worker %d: %w", nextWorkerID, err)
	}
	return allocation{workerID: nextWorkerID, lastTimestamp: barrierTimestamp, leaseToken: leaseToken}, false, nil
}

func (s *storage) lockCoordinator(ctx context.Context, tx *sqlx.Tx, epoch int64) error {
	query := s.rebind(fmt.Sprintf(
		"SELECT last_timestamp, lease_token FROM %s WHERE worker_id = ? FOR UPDATE",
		s.table,
	))
	var storedEpoch int64
	var leaseToken string
	if err := tx.QueryRowxContext(ctx, query, coordinatorWorkerID).Scan(&storedEpoch, &leaseToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: coordinator row is missing", ErrInvalidConfig)
		}
		return fmt.Errorf("lock coordinator row: %w", err)
	}
	if leaseToken != coordinatorLeaseToken {
		return fmt.Errorf("%w: worker ID %d is reserved for snowflake metadata", ErrInvalidConfig, coordinatorWorkerID)
	}
	if storedEpoch != epoch {
		return fmt.Errorf("%w: configured epoch %d does not match stored epoch %d", ErrInvalidConfig, epoch, storedEpoch)
	}
	return nil
}

func (s *storage) findAvailableWorkerID(ctx context.Context, tx *sqlx.Tx) (int64, error) {
	query := s.rebind(fmt.Sprintf(
		"SELECT worker_id FROM %s WHERE worker_id BETWEEN ? AND ? ORDER BY worker_id ASC",
		s.table,
	))
	rows, err := tx.QueryxContext(ctx, query, int64(0), MaxWorkerID)
	if err != nil {
		return 0, fmt.Errorf("list allocated worker IDs: %w", err)
	}
	defer rows.Close()

	nextWorkerID := int64(0)
	for rows.Next() {
		var workerID int64
		if err := rows.Scan(&workerID); err != nil {
			return 0, fmt.Errorf("scan allocated worker ID: %w", err)
		}
		if workerID == nextWorkerID {
			nextWorkerID++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate allocated worker IDs: %w", err)
	}
	if nextWorkerID > MaxWorkerID {
		return 0, ErrNoWorkerID
	}
	return nextWorkerID, nil
}

func (s *storage) maxTimestamp(ctx context.Context, tx *sqlx.Tx) (int64, error) {
	query := s.rebind(fmt.Sprintf(
		"SELECT COALESCE(MAX(last_timestamp), 0) FROM %s WHERE worker_id BETWEEN ? AND ?",
		s.table,
	))
	var timestamp int64
	if err := tx.QueryRowxContext(ctx, query, int64(0), MaxWorkerID).Scan(&timestamp); err != nil {
		return 0, fmt.Errorf("read maximum timestamp: %w", err)
	}
	return timestamp, nil
}

func (s *storage) databaseNow(ctx context.Context, tx *sqlx.Tx) (int64, error) {
	var timestamp int64
	if err := tx.QueryRowxContext(ctx, "SELECT "+s.databaseNowExpression()).Scan(&timestamp); err != nil {
		return 0, fmt.Errorf("read database timestamp: %w", err)
	}
	return timestamp, nil
}

func (s *storage) databaseNowExpression() string {
	if s.driverName == "pgx" {
		return "FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT"
	}
	return "CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS SIGNED)"
}

func (s *storage) heartbeat(ctx context.Context, workerID int64, leaseToken string, timestamp int64) error {
	databaseNow := s.databaseNowExpression()
	query := s.rebind(fmt.Sprintf(
		"UPDATE %s SET last_timestamp = GREATEST(last_timestamp, ?, %s), updated_at = %s WHERE worker_id BETWEEN 0 AND %d AND worker_id = ? AND lease_token = ?",
		s.table,
		databaseNow,
		databaseNow,
		MaxWorkerID,
	))
	result, err := s.db.ExecContext(ctx, query, timestamp, workerID, leaseToken)
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
	databaseNow := s.databaseNowExpression()
	query := s.rebind(fmt.Sprintf(
		"UPDATE %s SET last_timestamp = GREATEST(last_timestamp, ?, %s), updated_at = 0 WHERE worker_id BETWEEN 0 AND %d AND worker_id = ? AND lease_token = ?",
		s.table,
		databaseNow,
		MaxWorkerID,
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
