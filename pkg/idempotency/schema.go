package idempotency

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type columnMetadata struct {
	name       string
	dataType   string
	nullable   string
	maxLength  int64
	precision  int64
	defaultSQL string
	collation  string
}

var hashPartitionBoundPattern = regexp.MustCompile(`(?i)^FOR VALUES WITH \(modulus ([0-9]+), remainder ([0-9]+)\)$`)

func (s *sqlStorage) validatePostgreSQLTable(ctx context.Context) error {
	schema, err := s.resolveSchemaName(ctx)
	if err != nil {
		return err
	}
	columns, err := s.loadColumns(ctx, schema)
	if err != nil {
		return err
	}
	if err := validatePostgreSQLColumns(columns); err != nil {
		return err
	}
	if err := s.validatePrimaryKey(ctx, schema); err != nil {
		return err
	}
	if err := s.validatePostgreSQLPartitions(ctx, schema); err != nil {
		return err
	}
	return s.validateCreatedAtIndex(ctx, schema)
}

func (s *sqlStorage) validateMySQLTable(ctx context.Context) error {
	schema, err := s.resolveSchemaName(ctx)
	if err != nil {
		return err
	}
	columns, err := s.loadColumns(ctx, schema)
	if err != nil {
		return err
	}
	if err := validateMySQLColumns(columns); err != nil {
		return err
	}
	if err := s.validatePrimaryKey(ctx, schema); err != nil {
		return err
	}
	if err := s.validateMySQLPartitions(ctx, schema); err != nil {
		return err
	}
	return s.validateCreatedAtIndex(ctx, schema)
}

func (s *sqlStorage) resolveSchemaName(ctx context.Context) (string, error) {
	if s.config.schemaName != "" {
		return s.config.schemaName, nil
	}
	var schema sql.NullString
	query := "SELECT DATABASE()"
	if s.dialect == dialectPostgreSQL {
		query = "SELECT current_schema()"
	}
	if err := s.db.QueryRowContext(ctx, query).Scan(&schema); err != nil {
		return "", fmt.Errorf("resolve current schema: %w", err)
	}
	if !schema.Valid || schema.String == "" {
		return "", fmt.Errorf("%w: current schema is empty", ErrInvalidConfig)
	}
	return schema.String, nil
}

func (s *sqlStorage) loadColumns(ctx context.Context, schema string) ([]columnMetadata, error) {
	query := s.db.Rebind(`SELECT column_name, data_type, is_nullable,
		COALESCE(character_maximum_length, 0), COALESCE(datetime_precision, -1),
		COALESCE(column_default, ''), COALESCE(collation_name, '')
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position`)
	rows, err := s.db.QueryContext(ctx, query, schema, s.config.tableName)
	if err != nil {
		return nil, fmt.Errorf("inspect table columns: %w", err)
	}
	defer rows.Close()

	columns := make([]columnMetadata, 0, 3)
	for rows.Next() {
		var column columnMetadata
		if err := rows.Scan(
			&column.name,
			&column.dataType,
			&column.nullable,
			&column.maxLength,
			&column.precision,
			&column.defaultSQL,
			&column.collation,
		); err != nil {
			return nil, fmt.Errorf("scan table column metadata: %w", err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate table columns: %w", err)
	}
	return columns, nil
}

func validatePostgreSQLColumns(columns []columnMetadata) error {
	if len(columns) != 3 {
		return fmt.Errorf("%w: PostgreSQL table must contain exactly 3 columns, got %d", ErrInvalidConfig, len(columns))
	}
	if !matchesTextColumn(columns[0], "namespace", maxNamespaceLength, "character varying", "") {
		return incompatibleColumn("PostgreSQL", columns[0])
	}
	if !matchesTextColumn(columns[1], "idempotency_key", maxIdempotencyKeyLength, "character varying", "") {
		return incompatibleColumn("PostgreSQL", columns[1])
	}
	created := columns[2]
	if created.name != "created_at" || created.dataType != "timestamp with time zone" || created.nullable != "NO" || created.precision != 6 ||
		!strings.Contains(strings.ToLower(created.defaultSQL), "current_timestamp") {
		return incompatibleColumn("PostgreSQL", created)
	}
	return nil
}

func validateMySQLColumns(columns []columnMetadata) error {
	if len(columns) != 3 {
		return fmt.Errorf("%w: MySQL table must contain exactly 3 columns, got %d", ErrInvalidConfig, len(columns))
	}
	if !matchesTextColumn(columns[0], "namespace", maxNamespaceLength, "varchar", "utf8mb4_0900_bin") {
		return incompatibleColumn("MySQL", columns[0])
	}
	if !matchesTextColumn(columns[1], "idempotency_key", maxIdempotencyKeyLength, "varchar", "utf8mb4_0900_bin") {
		return incompatibleColumn("MySQL", columns[1])
	}
	created := columns[2]
	if created.name != "created_at" || created.dataType != "datetime" || created.nullable != "NO" || created.precision != 6 ||
		!strings.Contains(strings.ToLower(created.defaultSQL), "current_timestamp") {
		return incompatibleColumn("MySQL", created)
	}
	return nil
}

func matchesTextColumn(column columnMetadata, name string, length int, dataType, collation string) bool {
	return column.name == name && column.dataType == dataType && column.nullable == "NO" &&
		column.maxLength == int64(length) && (collation == "" || strings.EqualFold(column.collation, collation))
}

func incompatibleColumn(database string, column columnMetadata) error {
	return fmt.Errorf("%w: incompatible %s column %q", ErrInvalidConfig, database, column.name)
}

func (s *sqlStorage) validatePrimaryKey(ctx context.Context, schema string) error {
	query := s.db.Rebind(`SELECT k.column_name
		FROM information_schema.table_constraints AS t
		JOIN information_schema.key_column_usage AS k
		  ON k.constraint_schema = t.constraint_schema
		 AND k.constraint_name = t.constraint_name
		 AND k.table_schema = t.table_schema
		 AND k.table_name = t.table_name
		WHERE t.table_schema = ? AND t.table_name = ? AND t.constraint_type = 'PRIMARY KEY'
		ORDER BY k.ordinal_position`)
	rows, err := s.db.QueryContext(ctx, query, schema, s.config.tableName)
	if err != nil {
		return fmt.Errorf("inspect primary key: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("scan primary key metadata: %w", err)
		}
		columns = append(columns, name)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate primary key metadata: %w", err)
	}
	if len(columns) != 2 || columns[0] != "namespace" || columns[1] != "idempotency_key" {
		return fmt.Errorf("%w: primary key must be (namespace, idempotency_key)", ErrInvalidConfig)
	}
	return nil
}

func (s *sqlStorage) validatePostgreSQLPartitions(ctx context.Context, schema string) error {
	query := s.db.Rebind(`SELECT p.partstrat, pg_get_partkeydef(c.oid)
		FROM pg_catalog.pg_partitioned_table AS p
		JOIN pg_catalog.pg_class AS c ON c.oid = p.partrelid
		JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		WHERE n.nspname = ? AND c.relname = ?`)
	var strategy, keyDefinition string
	if err := s.db.QueryRowContext(ctx, query, schema, s.config.tableName).Scan(&strategy, &keyDefinition); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("%w: PostgreSQL table is not partitioned", ErrInvalidConfig)
		}
		return fmt.Errorf("inspect PostgreSQL partition key: %w", err)
	}
	if strategy != "h" || normalizeIdentifierList(keyDefinition) != "hash(namespace,idempotency_key)" {
		return fmt.Errorf("%w: PostgreSQL partition key must be HASH(namespace, idempotency_key)", ErrInvalidConfig)
	}

	query = s.db.Rebind(`SELECT pg_get_expr(child.relpartbound, child.oid)
		FROM pg_catalog.pg_inherits AS inheritance
		JOIN pg_catalog.pg_class AS parent ON parent.oid = inheritance.inhparent
		JOIN pg_catalog.pg_namespace AS parent_ns ON parent_ns.oid = parent.relnamespace
		JOIN pg_catalog.pg_class AS child ON child.oid = inheritance.inhrelid
		WHERE parent_ns.nspname = ? AND parent.relname = ?`)
	rows, err := s.db.QueryContext(ctx, query, schema, s.config.tableName)
	if err != nil {
		return fmt.Errorf("inspect PostgreSQL partitions: %w", err)
	}
	defer rows.Close()

	remainders := make(map[int]struct{}, partitionCount)
	for rows.Next() {
		var bound string
		if err := rows.Scan(&bound); err != nil {
			return fmt.Errorf("scan PostgreSQL partition bound: %w", err)
		}
		matches := hashPartitionBoundPattern.FindStringSubmatch(bound)
		if len(matches) != 3 || matches[1] != strconv.Itoa(partitionCount) {
			return fmt.Errorf("%w: unexpected PostgreSQL partition bound %q", ErrInvalidConfig, bound)
		}
		remainder, err := strconv.Atoi(matches[2])
		if err != nil || remainder < 0 || remainder >= partitionCount {
			return fmt.Errorf("%w: invalid PostgreSQL partition remainder %q", ErrInvalidConfig, matches[2])
		}
		remainders[remainder] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate PostgreSQL partitions: %w", err)
	}
	if len(remainders) != partitionCount {
		return fmt.Errorf("%w: PostgreSQL table must have %d hash partitions, got %d", ErrInvalidConfig, partitionCount, len(remainders))
	}
	return nil
}

func (s *sqlStorage) validateMySQLPartitions(ctx context.Context, schema string) error {
	query := s.db.Rebind(`SELECT partition_method, partition_expression
		FROM information_schema.partitions
		WHERE table_schema = ? AND table_name = ? AND partition_name IS NOT NULL
		ORDER BY partition_ordinal_position`)
	rows, err := s.db.QueryContext(ctx, query, schema, s.config.tableName)
	if err != nil {
		return fmt.Errorf("inspect MySQL partitions: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var method, expression string
		if err := rows.Scan(&method, &expression); err != nil {
			return fmt.Errorf("scan MySQL partition metadata: %w", err)
		}
		if !strings.EqualFold(method, "KEY") || normalizeIdentifierList(expression) != "namespace,idempotency_key" {
			return fmt.Errorf("%w: MySQL partition key must be KEY(namespace, idempotency_key)", ErrInvalidConfig)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate MySQL partitions: %w", err)
	}
	if count != partitionCount {
		return fmt.Errorf("%w: MySQL table must have %d KEY partitions, got %d", ErrInvalidConfig, partitionCount, count)
	}
	return nil
}

func (s *sqlStorage) validateCreatedAtIndex(ctx context.Context, schema string) error {
	var count int
	if s.dialect == dialectPostgreSQL {
		query := s.db.Rebind(`SELECT COUNT(*) FROM pg_catalog.pg_indexes
			WHERE schemaname = ? AND tablename = ? AND indexdef LIKE '%(created_at)%'`)
		if err := s.db.QueryRowContext(ctx, query, schema, s.config.tableName).Scan(&count); err != nil {
			return fmt.Errorf("inspect PostgreSQL created_at index: %w", err)
		}
	} else {
		query := s.db.Rebind(`SELECT COUNT(*) FROM information_schema.statistics
			WHERE table_schema = ? AND table_name = ? AND column_name = 'created_at'`)
		if err := s.db.QueryRowContext(ctx, query, schema, s.config.tableName).Scan(&count); err != nil {
			return fmt.Errorf("inspect MySQL created_at index: %w", err)
		}
	}
	if count == 0 {
		return fmt.Errorf("%w: created_at index is missing", ErrInvalidConfig)
	}
	return nil
}
