package infra

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/project-kgo/kc/pkg/resource"
)

func validateSQLConfig(config DataConfig) error {
	if strings.TrimSpace(config.DSN) == "" {
		return fmt.Errorf("%w: sql dsn is empty", ErrInvalidConfig)
	}
	// 提前解析 DSN，但不把可能含有凭证的解析错误原文返回给调用方。
	switch config.Type {
	case DataTypeMySQL:
		if _, err := mysql.ParseDSN(config.DSN); err != nil {
			return fmt.Errorf("%w: invalid mysql dsn", ErrInvalidConfig)
		}
	case DataTypePostgreSQL:
		if _, err := pgx.ParseConfig(config.DSN); err != nil {
			return fmt.Errorf("%w: invalid postgres dsn", ErrInvalidConfig)
		}
	}
	if config.SQL == nil {
		return nil
	}
	sqlConfig := config.SQL
	if sqlConfig.MaxOpenConns < 0 || sqlConfig.MaxIdleConns < 0 || sqlConfig.ConnMaxLifetime < 0 || sqlConfig.ConnMaxIdleTime < 0 {
		return fmt.Errorf("%w: sql pool option is negative", ErrInvalidConfig)
	}
	return nil
}

func prepareSQL(ctx context.Context, name string, config DataConfig) (preparedClient, error) {
	driver := "mysql"
	if config.Type == DataTypePostgreSQL {
		driver = "pgx"
	}
	db, err := sqlx.Open(driver, config.DSN)
	if err != nil {
		return preparedClient{}, fmt.Errorf("create sql client: %w", err)
	}

	if sqlConfig := config.SQL; sqlConfig != nil {
		if sqlConfig.MaxOpenConns > 0 {
			db.SetMaxOpenConns(sqlConfig.MaxOpenConns)
		}
		if sqlConfig.MaxIdleConns > 0 {
			db.SetMaxIdleConns(sqlConfig.MaxIdleConns)
		}
		if sqlConfig.ConnMaxLifetime > 0 {
			db.SetConnMaxLifetime(sqlConfig.ConnMaxLifetime)
		}
		if sqlConfig.ConnMaxIdleTime > 0 {
			db.SetConnMaxIdleTime(sqlConfig.ConnMaxIdleTime)
		}
	}

	if !config.SkipCheck {
		checkCtx, cancel := checkContext(ctx, config.CheckTimeout)
		err = db.PingContext(checkCtx)
		cancel()
		if err != nil {
			return preparedClient{}, errors.Join(fmt.Errorf("sql connection check: %w", err), db.Close())
		}
	}

	return preparedClient{
		name:   name,
		module: "data",
		typ:    string(config.Type),
		commit: func() error {
			return resource.Set(name, db)
		},
		close: func(context.Context) error {
			return db.Close()
		},
	}, nil
}
