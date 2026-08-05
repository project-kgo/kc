package idempotency

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
)

func TestResolveConfig(t *testing.T) {
	resolved, err := resolveConfig(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.table != defaultTableName || resolved.tableName != defaultTableName {
		t.Fatalf("default table = %+v, want %q", resolved, defaultTableName)
	}

	resolved, err = resolveConfig(Config{SchemaName: "infra", TableName: "requests"})
	if err != nil || resolved.table != "infra.requests" {
		t.Fatalf("qualified table = (%+v, %v), want infra.requests", resolved, err)
	}

	invalid := []Config{
		{SchemaName: "infra.prod"},
		{TableName: "request-key"},
		{TableName: "requests;drop"},
		{TableName: strings.Repeat("a", maxIdentifierLength-partitionSuffixLen+1)},
	}
	for _, config := range invalid {
		if _, err := resolveConfig(config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("resolveConfig(%+v) error = %v, want ErrInvalidConfig", config, err)
		}
	}
}

func TestConstructorsRejectInvalidInputs(t *testing.T) {
	if _, err := New(nil, nil, Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil context error = %v, want ErrInvalidConfig", err)
	}
	if _, err := New(context.Background(), nil, Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil database error = %v, want ErrInvalidConfig", err)
	}
	if _, err := NewFromResource(context.Background(), "", Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty resource name error = %v, want ErrInvalidConfig", err)
	}
	if _, err := NewFromResource(context.Background(), "idempotency-test-missing", Config{}); !errors.Is(err, ErrResourceNotFound) {
		t.Fatalf("missing resource error = %v, want ErrResourceNotFound", err)
	}

	var executor *Executor
	if err := executor.Execute(context.Background(), "orders", "1", func(context.Context, *sqlx.Tx) error { return nil }); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil executor error = %v, want ErrInvalidArgument", err)
	}
}

func TestValidateExecutionArguments(t *testing.T) {
	handler := Handler(func(context.Context, *sqlx.Tx) error { return nil })
	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name      string
		ctx       context.Context
		namespace string
		key       string
		handler   Handler
	}{
		{name: "nil context", namespace: "orders", key: "1", handler: handler},
		{name: "blank namespace", ctx: context.Background(), namespace: "  ", key: "1", handler: handler},
		{name: "blank key", ctx: context.Background(), namespace: "orders", key: "\t", handler: handler},
		{name: "invalid UTF-8", ctx: context.Background(), namespace: string([]byte{0xff}), key: "1", handler: handler},
		{name: "namespace too long", ctx: context.Background(), namespace: strings.Repeat("幂", maxNamespaceLength+1), key: "1", handler: handler},
		{name: "key too long", ctx: context.Background(), namespace: "orders", key: strings.Repeat("等", maxIdempotencyKeyLength+1), handler: handler},
		{name: "nil handler", ctx: context.Background(), namespace: "orders", key: "1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateExecutionArguments(test.ctx, test.namespace, test.key, test.handler); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("error = %v, want ErrInvalidArgument", err)
			}
		})
	}
	if err := validateExecutionArguments(canceledContext, "orders", "1", handler); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v, want context.Canceled", err)
	}

	if err := validateExecutionArguments(context.Background(), "订单", "Key-1", handler); err != nil {
		t.Fatalf("valid arguments rejected: %v", err)
	}
}

func TestDetectDialectAndDuplicateKey(t *testing.T) {
	if got, err := detectDialect("pgx"); err != nil || got != dialectPostgreSQL {
		t.Fatalf("detectDialect(pgx) = (%v, %v)", got, err)
	}
	if got, err := detectDialect("mysql"); err != nil || got != dialectMySQL {
		t.Fatalf("detectDialect(mysql) = (%v, %v)", got, err)
	}
	if _, err := detectDialect("sqlite3"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unsupported dialect error = %v, want ErrInvalidConfig", err)
	}

	if !isDuplicateKey(fmt.Errorf("wrapped: %w", &mysql.MySQLError{Number: 1062})) {
		t.Fatal("MySQL 1062 should be recognized as duplicate key")
	}
	if !isDuplicateKey(fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "23505"})) {
		t.Fatal("PostgreSQL 23505 should be recognized as duplicate key")
	}
	if isDuplicateKey(&mysql.MySQLError{Number: 1213}) || isDuplicateKey(&pgconn.PgError{Code: "40001"}) {
		t.Fatal("non-duplicate database errors must not be recognized as conflicts")
	}
}

func TestDialectIdentifierQuoting(t *testing.T) {
	config := resolvedConfig{schemaName: "Infra", tableName: "Requests"}
	postgres := &sqlStorage{dialect: dialectPostgreSQL, config: config}
	if got := postgres.qualifiedTableName(); got != `"Infra"."Requests"` {
		t.Fatalf("PostgreSQL qualified table = %q", got)
	}
	if got := postgres.qualifiedPartitionName(3); got != `"Infra"."Requests_p03"` {
		t.Fatalf("PostgreSQL partition = %q", got)
	}

	mysqlStorage := &sqlStorage{dialect: dialectMySQL, config: config}
	if got := mysqlStorage.qualifiedTableName(); got != "`Infra`.`Requests`" {
		t.Fatalf("MySQL qualified table = %q", got)
	}
}

func TestColumnValidation(t *testing.T) {
	postgres := []columnMetadata{
		{name: "namespace", dataType: "character varying", nullable: "NO", maxLength: 128},
		{name: "idempotency_key", dataType: "character varying", nullable: "NO", maxLength: 256},
		{name: "created_at", dataType: "timestamp with time zone", nullable: "NO", precision: 6, defaultSQL: "CURRENT_TIMESTAMP"},
	}
	if err := validatePostgreSQLColumns(postgres); err != nil {
		t.Fatalf("valid PostgreSQL columns rejected: %v", err)
	}
	postgres[1].maxLength = 255
	if err := validatePostgreSQLColumns(postgres); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid PostgreSQL columns error = %v", err)
	}

	mysqlColumns := []columnMetadata{
		{name: "namespace", dataType: "varchar", nullable: "NO", maxLength: 128, collation: "utf8mb4_0900_bin"},
		{name: "idempotency_key", dataType: "varchar", nullable: "NO", maxLength: 256, collation: "utf8mb4_0900_bin"},
		{name: "created_at", dataType: "datetime", nullable: "NO", precision: 6, defaultSQL: "CURRENT_TIMESTAMP(6)"},
	}
	if err := validateMySQLColumns(mysqlColumns); err != nil {
		t.Fatalf("valid MySQL columns rejected: %v", err)
	}
	mysqlColumns[0].collation = "utf8mb4_general_ci"
	if err := validateMySQLColumns(mysqlColumns); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid MySQL columns error = %v", err)
	}
}

func TestNormalizePartitionDefinition(t *testing.T) {
	if got := normalizeIdentifierList(`HASH ("namespace", "idempotency_key")`); got != "hash(namespace,idempotency_key)" {
		t.Fatalf("normalized PostgreSQL key = %q", got)
	}
	if got := normalizeIdentifierList("`namespace`, `idempotency_key`"); got != "namespace,idempotency_key" {
		t.Fatalf("normalized MySQL key = %q", got)
	}
	matches := hashPartitionBoundPattern.FindStringSubmatch("FOR VALUES WITH (modulus 64, remainder 63)")
	if len(matches) != 3 || matches[1] != "64" || matches[2] != "63" {
		t.Fatalf("partition bound matches = %v", matches)
	}
}
