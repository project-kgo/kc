package idempotency

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"regexp"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
)

const (
	defaultTableName    = "idempotency_records"
	partitionCount      = 64
	maxIdentifierLength = 63
	partitionSuffixLen  = len("_p63")
)

type dialect uint8

const (
	dialectPostgreSQL dialect = iota + 1
	dialectMySQL
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type resolvedConfig struct {
	schemaName string
	tableName  string
	table      string
}

type sqlStorage struct {
	db      *sqlx.DB
	dialect dialect
	config  resolvedConfig
}

func resolveConfig(config Config) (resolvedConfig, error) {
	tableName := config.TableName
	if tableName == "" {
		tableName = defaultTableName
	}
	if err := validateIdentifier("table name", tableName, maxIdentifierLength-partitionSuffixLen); err != nil {
		return resolvedConfig{}, err
	}
	if config.SchemaName != "" {
		if err := validateIdentifier("schema name", config.SchemaName, maxIdentifierLength); err != nil {
			return resolvedConfig{}, err
		}
	}
	table := tableName
	if config.SchemaName != "" {
		table = config.SchemaName + "." + tableName
	}
	return resolvedConfig{schemaName: config.SchemaName, tableName: tableName, table: table}, nil
}

func validateIdentifier(field, value string, maximum int) error {
	if len(value) > maximum || !identifierPattern.MatchString(value) {
		return fmt.Errorf("%w: %s %q is not a safe SQL identifier", ErrInvalidConfig, field, value)
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

func newSQLStorage(db *sqlx.DB, config resolvedConfig) (*sqlStorage, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: database is nil", ErrInvalidConfig)
	}
	databaseDialect, err := detectDialect(db.DriverName())
	if err != nil {
		return nil, err
	}
	return &sqlStorage{db: db, dialect: databaseDialect, config: config}, nil
}

func (s *sqlStorage) ensureTable(ctx context.Context) error {
	exists, err := s.tableExists(ctx)
	if err != nil {
		return err
	}
	switch s.dialect {
	case dialectPostgreSQL:
		if exists {
			return s.validatePostgreSQLTable(ctx)
		}
		if err := s.ensurePostgreSQLTable(ctx); err != nil {
			return err
		}
		return s.validatePostgreSQLTable(ctx)
	case dialectMySQL:
		if exists {
			return s.validateMySQLTable(ctx)
		}
		if err := s.ensureMySQLTable(ctx); err != nil {
			return err
		}
		return s.validateMySQLTable(ctx)
	default:
		return fmt.Errorf("%w: unknown SQL dialect", ErrInvalidConfig)
	}
}

func (s *sqlStorage) tableExists(ctx context.Context) (bool, error) {
	schema, err := s.resolveSchemaName(ctx)
	if err != nil {
		return false, err
	}
	query := s.db.Rebind(`SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = ? AND table_name = ?`)
	var count int
	if err := s.db.QueryRowContext(ctx, query, schema, s.config.tableName).Scan(&count); err != nil {
		return false, fmt.Errorf("inspect idempotency table existence: %w", err)
	}
	return count > 0, nil
}

func (s *sqlStorage) ensurePostgreSQLTable(ctx context.Context) error {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin PostgreSQL schema transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	table := s.qualifiedTableName()
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		namespace VARCHAR(128) NOT NULL,
		idempotency_key VARCHAR(256) NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (namespace, idempotency_key)
	) PARTITION BY HASH (namespace, idempotency_key)`, table)
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("create PostgreSQL parent table: %w", err)
	}

	for remainder := range partitionCount {
		partition := s.qualifiedPartitionName(remainder)
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s PARTITION OF %s
			FOR VALUES WITH (MODULUS %d, REMAINDER %d)`, partition, table, partitionCount, remainder)
		if _, err := tx.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("create PostgreSQL partition %d: %w", remainder, err)
		}
	}

	index := s.qualifiedIndexName()
	query = fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (created_at)", index, table)
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("create PostgreSQL created_at index: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit PostgreSQL schema transaction: %w", err)
	}
	return nil
}

func (s *sqlStorage) ensureMySQLTable(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		namespace VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
		idempotency_key VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
		created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
		PRIMARY KEY (namespace, idempotency_key),
		KEY idx_created_at (created_at)
	) ENGINE=InnoDB
	PARTITION BY KEY ALGORITHM=2 (namespace, idempotency_key) PARTITIONS 64`, s.qualifiedTableName())
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("create MySQL partitioned table: %w", err)
	}
	return nil
}

func (s *sqlStorage) insert(ctx context.Context, tx *sqlx.Tx, namespace, idempotencyKey string) error {
	query := s.db.Rebind(fmt.Sprintf(
		"INSERT INTO %s (namespace, idempotency_key) VALUES (?, ?)",
		s.qualifiedTableName(),
	))
	_, err := tx.ExecContext(ctx, query, namespace, idempotencyKey)
	return err
}

func (s *sqlStorage) qualifiedPartitionName(remainder int) string {
	name := fmt.Sprintf("%s_p%02d", s.config.tableName, remainder)
	return s.qualifiedIdentifier(name)
}

func (s *sqlStorage) qualifiedIndexName() string {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(s.config.tableName))
	// PostgreSQL 会自动把索引创建在数据表所在 Schema，索引名不能带 Schema 前缀。
	return s.quoteIdentifier(fmt.Sprintf("idempotency_created_%08x", hasher.Sum32()))
}

func (s *sqlStorage) qualifiedTableName() string {
	return s.qualifiedIdentifier(s.config.tableName)
}

func (s *sqlStorage) qualifiedIdentifier(name string) string {
	qualified := s.quoteIdentifier(name)
	if s.config.schemaName != "" {
		return s.quoteIdentifier(s.config.schemaName) + "." + qualified
	}
	return qualified
}

func (s *sqlStorage) quoteIdentifier(name string) string {
	if s.dialect == dialectMySQL {
		return "`" + name + "`"
	}
	return `"` + name + `"`
}

func normalizeIdentifierList(value string) string {
	value = strings.ToLower(value)
	value = strings.ReplaceAll(value, "`", "")
	value = strings.ReplaceAll(value, `"`, "")
	value = strings.ReplaceAll(value, " ", "")
	value = strings.ReplaceAll(value, "\n", "")
	value = strings.ReplaceAll(value, "\t", "")
	return value
}

func isDuplicateKey(err error) bool {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return true
	}
	var postgresErr *pgconn.PgError
	return errors.As(err, &postgresErr) && postgresErr.Code == "23505"
}
