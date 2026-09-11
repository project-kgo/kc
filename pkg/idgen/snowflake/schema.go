package snowflake

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

type workerColumn struct {
	name      string
	dataType  string
	nullable  string
	maxLength int64
}

func (s *storage) ensureTable(ctx context.Context, epoch int64) error {
	rangeConstraint := s.derivedIdentifier("_worker_id_range")
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		worker_id INTEGER NOT NULL PRIMARY KEY,
		last_timestamp BIGINT NOT NULL,
		ip_address VARCHAR(45) NOT NULL,
		updated_at BIGINT NOT NULL,
		lease_token VARCHAR(64) NOT NULL,
		CONSTRAINT %s CHECK (worker_id = -1 OR worker_id BETWEEN 0 AND %d)
	)`, s.table, rangeConstraint, MaxWorkerID)
	if _, err := s.db.ExecContext(ctx, query); err != nil && !isDuplicateKey(err) {
		return fmt.Errorf("create worker table: %w", err)
	}

	schema, err := s.resolveSchemaName(ctx)
	if err != nil {
		return err
	}
	if err := s.validateWorkerColumns(ctx, schema); err != nil {
		return err
	}
	if err := s.validateWorkerData(ctx); err != nil {
		return err
	}
	if err := s.ensureWorkerIDUnique(ctx, schema); err != nil {
		return err
	}
	if err := s.ensureWorkerIDRangeConstraint(ctx, schema, rangeConstraint); err != nil {
		return err
	}
	return s.ensureCoordinator(ctx, epoch)
}

func (s *storage) resolveSchemaName(ctx context.Context) (string, error) {
	if s.namespace != "" {
		return s.namespace, nil
	}
	query := "SELECT DATABASE()"
	if s.driverName == "pgx" {
		query = "SELECT current_schema()"
	}
	var schema sql.NullString
	if err := s.db.QueryRowContext(ctx, query).Scan(&schema); err != nil {
		return "", fmt.Errorf("resolve current schema: %w", err)
	}
	if !schema.Valid || schema.String == "" {
		return "", fmt.Errorf("%w: current schema is empty", ErrInvalidConfig)
	}
	return schema.String, nil
}

func (s *storage) validateWorkerColumns(ctx context.Context, schema string) error {
	query := s.rebind(`SELECT column_name, data_type, is_nullable,
		COALESCE(character_maximum_length, 0)
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position`)
	rows, err := s.db.QueryContext(ctx, query, schema, s.tableName)
	if err != nil {
		return fmt.Errorf("inspect worker table columns: %w", err)
	}
	defer rows.Close()

	var columns []workerColumn
	for rows.Next() {
		var column workerColumn
		if err := rows.Scan(&column.name, &column.dataType, &column.nullable, &column.maxLength); err != nil {
			return fmt.Errorf("scan worker table column: %w", err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate worker table columns: %w", err)
	}
	if len(columns) != 5 {
		return fmt.Errorf("%w: worker table must contain exactly 5 columns, got %d", ErrInvalidConfig, len(columns))
	}

	integerType := "int"
	if s.driverName == "pgx" {
		integerType = "integer"
	}
	expected := []workerColumn{
		{name: "worker_id", dataType: integerType, nullable: "NO"},
		{name: "last_timestamp", dataType: "bigint", nullable: "NO"},
		{name: "ip_address", dataType: "varchar", nullable: "NO", maxLength: 45},
		{name: "updated_at", dataType: "bigint", nullable: "NO"},
		{name: "lease_token", dataType: "varchar", nullable: "NO", maxLength: 64},
	}
	if s.driverName == "pgx" {
		expected[2].dataType = "character varying"
		expected[4].dataType = "character varying"
	}
	for index := range expected {
		actual := columns[index]
		want := expected[index]
		if actual.name != want.name || !strings.EqualFold(actual.dataType, want.dataType) || actual.nullable != want.nullable ||
			(want.maxLength != 0 && actual.maxLength != want.maxLength) {
			return fmt.Errorf("%w: incompatible worker table column %q", ErrInvalidConfig, actual.name)
		}
	}
	return nil
}

func (s *storage) validateWorkerData(ctx context.Context) error {
	query := s.rebind(fmt.Sprintf(
		"SELECT worker_id FROM %s WHERE worker_id < ? OR worker_id > ? LIMIT 1",
		s.table,
	))
	var workerID int64
	err := s.db.QueryRowContext(ctx, query, coordinatorWorkerID, MaxWorkerID).Scan(&workerID)
	if err == nil {
		return fmt.Errorf("%w: worker table contains invalid worker ID %d", ErrInvalidConfig, workerID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("validate worker ID range: %w", err)
	}

	duplicateQuery := fmt.Sprintf(
		"SELECT worker_id FROM %s GROUP BY worker_id HAVING COUNT(*) > 1 LIMIT 1",
		s.table,
	)
	err = s.db.QueryRowContext(ctx, duplicateQuery).Scan(&workerID)
	if err == nil {
		return fmt.Errorf("%w: worker table contains duplicate worker ID %d", ErrInvalidConfig, workerID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("validate duplicate worker IDs: %w", err)
	}
	return nil
}

func (s *storage) ensureWorkerIDUnique(ctx context.Context, schema string) error {
	unique, err := s.hasWorkerIDUniqueConstraint(ctx, schema)
	if err != nil {
		return err
	}
	if unique {
		return nil
	}

	constraintName := s.derivedIdentifier("_worker_id_unique")
	query := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (worker_id)", s.table, constraintName)
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		unique, inspectErr := s.hasWorkerIDUniqueConstraint(ctx, schema)
		if inspectErr != nil {
			return errors.Join(fmt.Errorf("add worker ID unique constraint: %w", err), inspectErr)
		}
		if !unique {
			return fmt.Errorf("add worker ID unique constraint: %w", err)
		}
	}
	return nil
}

func (s *storage) hasWorkerIDUniqueConstraint(ctx context.Context, schema string) (bool, error) {
	query := s.rebind(`SELECT t.constraint_name, k.column_name
		FROM information_schema.table_constraints AS t
		JOIN information_schema.key_column_usage AS k
		  ON k.constraint_schema = t.constraint_schema
		 AND k.constraint_name = t.constraint_name
		 AND k.table_schema = t.table_schema
		 AND k.table_name = t.table_name
		WHERE t.table_schema = ? AND t.table_name = ?
		  AND t.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
		ORDER BY t.constraint_name, k.ordinal_position`)
	rows, err := s.db.QueryContext(ctx, query, schema, s.tableName)
	if err != nil {
		return false, fmt.Errorf("inspect worker ID unique constraint: %w", err)
	}
	defer rows.Close()

	constraints := make(map[string][]string)
	for rows.Next() {
		var constraintName, columnName string
		if err := rows.Scan(&constraintName, &columnName); err != nil {
			return false, fmt.Errorf("scan worker ID unique constraint: %w", err)
		}
		constraints[constraintName] = append(constraints[constraintName], columnName)
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate worker ID unique constraints: %w", err)
	}
	for _, columns := range constraints {
		if len(columns) == 1 && columns[0] == "worker_id" {
			return true, nil
		}
	}
	return false, nil
}

func (s *storage) ensureWorkerIDRangeConstraint(ctx context.Context, schema, constraintName string) error {
	exists, err := s.hasNamedCheckConstraint(ctx, schema, constraintName)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	query := fmt.Sprintf(
		"ALTER TABLE %s ADD CONSTRAINT %s CHECK (worker_id = -1 OR worker_id BETWEEN 0 AND %d)",
		s.table,
		constraintName,
		MaxWorkerID,
	)
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		exists, inspectErr := s.hasNamedCheckConstraint(ctx, schema, constraintName)
		if inspectErr != nil {
			return errors.Join(fmt.Errorf("add worker ID range constraint: %w", err), inspectErr)
		}
		if !exists {
			return fmt.Errorf("add worker ID range constraint: %w", err)
		}
	}
	return nil
}

func (s *storage) hasNamedCheckConstraint(ctx context.Context, schema, constraintName string) (bool, error) {
	query := s.rebind(`SELECT COUNT(*) FROM information_schema.table_constraints
		WHERE table_schema = ? AND table_name = ? AND constraint_name = ? AND constraint_type = 'CHECK'`)
	var count int
	if err := s.db.QueryRowContext(ctx, query, schema, s.tableName, constraintName).Scan(&count); err != nil {
		return false, fmt.Errorf("inspect worker ID range constraint: %w", err)
	}
	return count > 0, nil
}

func (s *storage) ensureCoordinator(ctx context.Context, epoch int64) error {
	insertQuery := fmt.Sprintf(
		"INSERT INTO %s (worker_id, last_timestamp, ip_address, updated_at, lease_token) VALUES (?, ?, ?, ?, ?)",
		s.table,
	)
	if s.driverName == "pgx" {
		insertQuery += " ON CONFLICT (worker_id) DO NOTHING"
	} else {
		insertQuery = "INSERT IGNORE" + strings.TrimPrefix(insertQuery, "INSERT")
	}
	insertQuery = s.rebind(insertQuery)
	if _, err := s.db.ExecContext(
		ctx,
		insertQuery,
		coordinatorWorkerID,
		epoch,
		coordinatorIPAddress,
		coordinatorUpdatedAt,
		coordinatorLeaseToken,
	); err != nil {
		return fmt.Errorf("create coordinator row: %w", err)
	}

	query := s.rebind(fmt.Sprintf(
		"SELECT last_timestamp, ip_address, updated_at, lease_token FROM %s WHERE worker_id = ?",
		s.table,
	))
	var storedEpoch, updatedAt int64
	var ipAddress, leaseToken string
	if err := s.db.QueryRowContext(ctx, query, coordinatorWorkerID).Scan(
		&storedEpoch,
		&ipAddress,
		&updatedAt,
		&leaseToken,
	); err != nil {
		return fmt.Errorf("read coordinator row: %w", err)
	}
	if ipAddress != coordinatorIPAddress || updatedAt != coordinatorUpdatedAt || leaseToken != coordinatorLeaseToken {
		return fmt.Errorf("%w: worker ID %d is reserved for snowflake metadata", ErrInvalidConfig, coordinatorWorkerID)
	}
	if storedEpoch != epoch {
		return fmt.Errorf("%w: configured epoch %d does not match stored epoch %d", ErrInvalidConfig, epoch, storedEpoch)
	}
	return nil
}

func (s *storage) derivedIdentifier(suffix string) string {
	candidate := s.tableName + suffix
	if len(candidate) <= maxIdentifierLength {
		return candidate
	}
	digest := sha256.Sum256([]byte(candidate))
	hash := hex.EncodeToString(digest[:4])
	prefixLength := maxIdentifierLength - len(suffix) - len(hash) - 1
	return s.tableName[:prefixLength] + "_" + hash + suffix
}
