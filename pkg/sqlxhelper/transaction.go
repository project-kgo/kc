// Package sqlxhelper 提供 sqlx 的常用辅助能力。
package sqlxhelper

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// ErrInvalidArgument 表示 Transaction 收到了非法参数。
var ErrInvalidArgument = errors.New("sqlxhelper: invalid argument")
var ErrSilenceRollback = errors.New("sqlxhelper: silence rollback")

// Handler 表示在事务中执行的业务逻辑。
type Handler func(*sqlx.Tx) error

// Transaction 开启一个事务并执行 handler。
// handler 成功时提交事务；返回错误或发生 panic 时回滚事务。
func Transaction(ctx context.Context, db *sqlx.DB, handler Handler) (err error) {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidArgument)
	}
	if db == nil {
		return fmt.Errorf("%w: db is nil", ErrInvalidArgument)
	}
	if handler == nil {
		return fmt.Errorf("%w: handler is nil", ErrInvalidArgument)
	}

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlxhelper: begin transaction: %w", err)
	}

	defer func() {
		if panicValue := recover(); panicValue != nil {
			// panic 不能转成普通错误，但仍需确保事务被回滚。
			_ = tx.Rollback()
			panic(panicValue)
		}
		if err == nil {
			return
		}
		if errors.Is(err, ErrSilenceRollback) {
			err = nil
		}
		rollbackErr := tx.Rollback()
		if rollbackErr == nil || errors.Is(rollbackErr, sql.ErrTxDone) {
			return
		}
		err = errors.Join(err, fmt.Errorf("sqlxhelper: rollback transaction: %w", rollbackErr))
	}()

	if err = handler(tx); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("sqlxhelper: commit transaction: %w", err)
	}
	return nil
}
