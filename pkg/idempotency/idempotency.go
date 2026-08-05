// Package idempotency 提供基于 SQL 唯一键的事务幂等执行能力。
package idempotency

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jmoiron/sqlx"
	"github.com/project-kgo/kc/pkg/resource"
)

const (
	maxNamespaceLength      = 128
	maxIdempotencyKeyLength = 256
)

var (
	// ErrConflict 表示相同 namespace 和 idempotency key 已经成功提交。
	ErrConflict = errors.New("idempotency: conflict")
	// ErrInvalidArgument 表示执行参数无效。
	ErrInvalidArgument = errors.New("idempotency: invalid argument")
	// ErrInvalidConfig 表示配置或数据库表结构不符合要求。
	ErrInvalidConfig = errors.New("idempotency: invalid config")
	// ErrResourceNotFound 表示指定名称的 SQL 资源不存在。
	ErrResourceNotFound = errors.New("idempotency: SQL resource not found")
)

// Config 配置幂等表。SchemaName 为空时使用当前数据库或 PostgreSQL search_path。
type Config struct {
	SchemaName string
	TableName  string
}

// Handler 在幂等记录所在的同一事务中执行业务逻辑。
type Handler func(context.Context, *sqlx.Tx) error

// Executor 管理幂等记录和业务事务。它不持有数据库连接的生命周期。
type Executor struct {
	db      *sqlx.DB
	storage *sqlStorage
}

// New 使用调用方提供的 SQLx 连接创建执行器，并初始化 64 个 Hash 分区。
func New(ctx context.Context, db *sqlx.DB, config Config) (*Executor, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidConfig)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: context is already done: %w", ErrInvalidConfig, err)
	}
	resolved, err := resolveConfig(config)
	if err != nil {
		return nil, err
	}
	storage, err := newSQLStorage(db, resolved)
	if err != nil {
		return nil, err
	}
	if err := storage.ensureTable(ctx); err != nil {
		return nil, fmt.Errorf("idempotency: ensure table %q: %w", resolved.table, err)
	}
	return &Executor{db: db, storage: storage}, nil
}

// NewFromResource 从 resource 中获取指定名称的 *sqlx.DB 并创建执行器。
func NewFromResource(ctx context.Context, name string, config Config) (*Executor, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%w: resource name is empty", ErrInvalidConfig)
	}
	db, ok := resource.Get[*sqlx.DB](name)
	if !ok || db == nil {
		return nil, fmt.Errorf("%w: %q", ErrResourceNotFound, name)
	}
	return New(ctx, db, config)
}

// Execute 在新事务中先写入幂等记录，再执行 Handler。
// Handler 成功时提交，失败或 panic 时回滚。
func (e *Executor) Execute(ctx context.Context, namespace, idempotencyKey string, handler Handler) (err error) {
	if e == nil || e.db == nil || e.storage == nil {
		return fmt.Errorf("%w: executor is nil", ErrInvalidArgument)
	}
	if err := validateExecutionArguments(ctx, namespace, idempotencyKey, handler); err != nil {
		return err
	}

	tx, err := e.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin idempotency transaction: %w", err)
	}
	defer func() {
		rollbackErr := tx.Rollback()
		if rollbackErr == nil || errors.Is(rollbackErr, sql.ErrTxDone) {
			return
		}
		wrapped := fmt.Errorf("rollback idempotency transaction: %w", rollbackErr)
		if err == nil {
			err = wrapped
			return
		}
		err = errors.Join(err, wrapped)
	}()

	if err = e.storage.insert(ctx, tx, namespace, idempotencyKey); err != nil {
		if isDuplicateKey(err) {
			return fmt.Errorf("%w: namespace %q", ErrConflict, namespace)
		}
		return fmt.Errorf("insert idempotency record: %w", err)
	}
	if err = handler(ctx, tx); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit idempotency transaction: %w", err)
	}
	return nil
}

func validateExecutionArguments(ctx context.Context, namespace, idempotencyKey string, handler Handler) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateText("namespace", namespace, maxNamespaceLength); err != nil {
		return err
	}
	if err := validateText("idempotency key", idempotencyKey, maxIdempotencyKeyLength); err != nil {
		return err
	}
	if handler == nil {
		return fmt.Errorf("%w: handler is nil", ErrInvalidArgument)
	}
	return nil
}

func validateText(field, value string, maximum int) error {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" {
		return fmt.Errorf("%w: %s must be non-blank valid UTF-8", ErrInvalidArgument, field)
	}
	if utf8.RuneCountInString(value) > maximum {
		return fmt.Errorf("%w: %s exceeds %d characters", ErrInvalidArgument, field, maximum)
	}
	return nil
}
