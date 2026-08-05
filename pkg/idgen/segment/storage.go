package segment

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
)

const (
	defaultTableName    = "id_generator"
	maxBizTagLength     = 128
	maxIdentifierLength = 63
	databaseRetries     = 3
)

type dialect uint8

const (
	dialectPostgreSQL dialect = iota + 1
	dialectMySQL
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type resolvedConfig struct {
	table string
}

type sqlStorage struct {
	db      *sqlx.DB
	table   string
	dialect dialect
}

func resolveConfig(config Config) (resolvedConfig, error) {
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
	table := tableName
	if config.Namespace != "" {
		table = config.Namespace + "." + tableName
	}
	return resolvedConfig{table: table}, nil
}

func validateIdentifier(field, value string) error {
	if len(value) > maxIdentifierLength || !identifierPattern.MatchString(value) {
		return fmt.Errorf("%w: %s %q is not a safe SQL identifier", ErrInvalidConfig, field, value)
	}
	return nil
}

func validateBizTag(bizTag string) error {
	if !utf8.ValidString(bizTag) || strings.TrimSpace(bizTag) == "" || utf8.RuneCountInString(bizTag) > maxBizTagLength {
		return fmt.Errorf("%w: biz tag must contain 1-%d valid UTF-8 characters", ErrInvalidConfig, maxBizTagLength)
	}
	return nil
}

func validateResourceName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%w: resource name is empty", ErrInvalidConfig)
	}
	return nil
}

func detectDialect(driverName string) (dialect, error) {
	switch driverName {
	case "pgx":
		return dialectPostgreSQL, nil
	case "mysql":
		return dialectMySQL, nil
	default:
		return 0, fmt.Errorf("%w: SQL driver %q is not supported", ErrInvalidConfig, driverName)
	}
}

func newSQLStorage(db *sqlx.DB, table string) (*sqlStorage, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: database is nil", ErrInvalidConfig)
	}
	databaseDialect, err := detectDialect(db.DriverName())
	if err != nil {
		return nil, err
	}
	return &sqlStorage{db: db, table: table, dialect: databaseDialect}, nil
}

func (s *sqlStorage) rebind(query string) string {
	return s.db.Rebind(query)
}

func (s *sqlStorage) ensureTable(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		biz_tag VARCHAR(128) NOT NULL PRIMARY KEY,
		max_id BIGINT NOT NULL,
		step INTEGER NOT NULL,
		description VARCHAR(256) NULL,
		update_time BIGINT NOT NULL
	)`, s.table)
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("create segment table: %w", err)
	}

	// IF NOT EXISTS 不会校验既有结构，构造阶段先验证全部必需列。
	checkQuery := fmt.Sprintf(
		"SELECT biz_tag, max_id, step, description, update_time FROM %s WHERE 1 = 0",
		s.table,
	)
	rows, err := s.db.QueryxContext(ctx, checkQuery)
	if err != nil {
		return fmt.Errorf("validate segment table: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close segment table validation rows: %w", err)
	}
	return nil
}

func (s *sqlStorage) initTag(ctx context.Context, bizTag string, startID int64, step int32) error {
	switch s.dialect {
	case dialectPostgreSQL:
		return s.retry(ctx, func() error {
			query := s.rebind(fmt.Sprintf(`INSERT INTO %s (biz_tag, max_id, step, update_time)
				VALUES (?, ?, ?, ?)
				ON CONFLICT (biz_tag) DO UPDATE
				SET step = EXCLUDED.step, update_time = EXCLUDED.update_time`, s.table))
			if _, err := s.db.ExecContext(ctx, query, bizTag, startID, step, time.Now().UnixMilli()); err != nil {
				return fmt.Errorf("initialize PostgreSQL biz tag %q: %w", bizTag, err)
			}
			return nil
		}, false)
	case dialectMySQL:
		return s.retry(ctx, func() error {
			return s.initMySQLTagOnce(ctx, bizTag, startID, step)
		}, true)
	default:
		return fmt.Errorf("%w: unknown SQL dialect", ErrInvalidConfig)
	}
}

func (s *sqlStorage) initMySQLTagOnce(ctx context.Context, bizTag string, startID int64, step int32) (err error) {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin MySQL tag initialization: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) && err == nil {
			err = fmt.Errorf("rollback MySQL tag initialization: %w", rollbackErr)
		}
	}()

	selectQuery := s.rebind(fmt.Sprintf("SELECT max_id FROM %s WHERE biz_tag = ? FOR UPDATE", s.table))
	var ignoredMaxID int64
	err = tx.QueryRowxContext(ctx, selectQuery, bizTag).Scan(&ignoredMaxID)
	switch {
	case err == nil:
		updateQuery := s.rebind(fmt.Sprintf("UPDATE %s SET step = ?, update_time = ? WHERE biz_tag = ?", s.table))
		if _, err = tx.ExecContext(ctx, updateQuery, step, time.Now().UnixMilli(), bizTag); err != nil {
			return fmt.Errorf("update MySQL biz tag %q: %w", bizTag, err)
		}
	case errors.Is(err, sql.ErrNoRows):
		insertQuery := s.rebind(fmt.Sprintf(
			"INSERT INTO %s (biz_tag, max_id, step, update_time) VALUES (?, ?, ?, ?)",
			s.table,
		))
		if _, err = tx.ExecContext(ctx, insertQuery, bizTag, startID, step, time.Now().UnixMilli()); err != nil {
			return fmt.Errorf("insert MySQL biz tag %q: %w", bizTag, err)
		}
	default:
		return fmt.Errorf("lock MySQL biz tag %q: %w", bizTag, err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit MySQL tag initialization: %w", err)
	}
	return nil
}

func (s *sqlStorage) fetchSegment(ctx context.Context, bizTag string) (*Segment, error) {
	switch s.dialect {
	case dialectPostgreSQL:
		var segment *Segment
		err := s.retry(ctx, func() error {
			fetched, err := s.fetchPostgreSQLSegment(ctx, bizTag)
			if err == nil {
				segment = fetched
			}
			return err
		}, false)
		return segment, err
	case dialectMySQL:
		var segment *Segment
		err := s.retry(ctx, func() error {
			fetched, err := s.fetchMySQLSegmentOnce(ctx, bizTag)
			if err == nil {
				segment = fetched
			}
			return err
		}, false)
		return segment, err
	default:
		return nil, fmt.Errorf("%w: unknown SQL dialect", ErrInvalidConfig)
	}
}

func (s *sqlStorage) fetchPostgreSQLSegment(ctx context.Context, bizTag string) (*Segment, error) {
	query := s.rebind(fmt.Sprintf(`UPDATE %s
		SET max_id = max_id + step, update_time = ?
		WHERE biz_tag = ? AND step > 0
		RETURNING max_id, step`, s.table))
	var maxID int64
	var step int32
	err := s.db.QueryRowxContext(ctx, query, time.Now().UnixMilli(), bizTag).Scan(&maxID, &step)
	if err == nil {
		return makeSegment(maxID, step)
	}
	if isNumericOverflow(err) {
		return nil, fmt.Errorf("%w for biz tag %q", ErrIDOverflow, bizTag)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("update PostgreSQL segment for %q: %w", bizTag, err)
	}

	// UPDATE 无返回行时，区分记录不存在与数据库中保存了非法步长。
	checkQuery := s.rebind(fmt.Sprintf("SELECT step FROM %s WHERE biz_tag = ?", s.table))
	if checkErr := s.db.QueryRowxContext(ctx, checkQuery, bizTag).Scan(&step); errors.Is(checkErr, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: %q", ErrBizTagNotFound, bizTag)
	} else if checkErr != nil {
		return nil, fmt.Errorf("check PostgreSQL biz tag %q: %w", bizTag, checkErr)
	}
	return nil, fmt.Errorf("%w: database step for %q must be positive", ErrInvalidConfig, bizTag)
}

func (s *sqlStorage) fetchMySQLSegmentOnce(ctx context.Context, bizTag string) (segment *Segment, err error) {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin MySQL segment allocation: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) && err == nil {
			err = fmt.Errorf("rollback MySQL segment allocation: %w", rollbackErr)
		}
	}()

	selectQuery := s.rebind(fmt.Sprintf("SELECT max_id, step FROM %s WHERE biz_tag = ? FOR UPDATE", s.table))
	var maxID int64
	var step int32
	if err = tx.QueryRowxContext(ctx, selectQuery, bizTag).Scan(&maxID, &step); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: %q", ErrBizTagNotFound, bizTag)
		}
		return nil, fmt.Errorf("lock MySQL biz tag %q: %w", bizTag, err)
	}
	if step <= 0 {
		return nil, fmt.Errorf("%w: database step for %q must be positive", ErrInvalidConfig, bizTag)
	}
	if maxID > math.MaxInt64-int64(step) {
		return nil, fmt.Errorf("%w for biz tag %q", ErrIDOverflow, bizTag)
	}
	newMaxID := maxID + int64(step)
	updateQuery := s.rebind(fmt.Sprintf("UPDATE %s SET max_id = ?, update_time = ? WHERE biz_tag = ?", s.table))
	if _, err = tx.ExecContext(ctx, updateQuery, newMaxID, time.Now().UnixMilli(), bizTag); err != nil {
		return nil, fmt.Errorf("update MySQL segment for %q: %w", bizTag, err)
	}
	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit MySQL segment allocation: %w", err)
	}
	return makeSegment(newMaxID, step)
}

func makeSegment(maxID int64, step int32) (*Segment, error) {
	if step <= 0 {
		return nil, fmt.Errorf("%w: segment step must be positive", ErrInvalidConfig)
	}
	start := maxID - int64(step) + 1
	return &Segment{Start: start, End: maxID, Current: start}, nil
}

func (s *sqlStorage) retry(ctx context.Context, operation func() error, retryDuplicate bool) error {
	var lastErr error
	for attempt := 0; attempt < databaseRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		lastErr = operation()
		if lastErr == nil {
			return nil
		}
		if !isRetryableTransaction(lastErr) && !(retryDuplicate && isDuplicateKey(lastErr)) {
			return lastErr
		}
	}
	return fmt.Errorf("database operation failed after %d attempts: %w", databaseRetries, lastErr)
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
		return mysqlErr.Number == 1205 || mysqlErr.Number == 1213
	}
	var postgresErr *pgconn.PgError
	if errors.As(err, &postgresErr) {
		return postgresErr.Code == "40001" || postgresErr.Code == "40P01"
	}
	return false
}

func isNumericOverflow(err error) bool {
	var postgresErr *pgconn.PgError
	return errors.As(err, &postgresErr) && postgresErr.Code == "22003"
}
