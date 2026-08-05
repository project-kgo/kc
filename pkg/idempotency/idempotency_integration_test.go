package idempotency

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/project-kgo/kc/pkg/resource"
)

func TestPostgreSQLIntegration(t *testing.T) {
	runSQLIntegration(t, "pgx", "TEST_POSTGRES_DSN")
}

func TestMySQLIntegration(t *testing.T) {
	runSQLIntegration(t, "mysql", "TEST_MYSQL_DSN")
}

func runSQLIntegration(t *testing.T, driver, environment string) {
	t.Helper()
	dsn := os.Getenv(environment)
	if dsn == "" {
		t.Skipf("跳过 %s 集成测试：未设置 %s", driver, environment)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := sqlx.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("连接 %s 数据库: %v", driver, err)
	}

	suffix := time.Now().UnixNano()
	tableName := fmt.Sprintf("idempotency_it_%d", suffix)
	markerTable := fmt.Sprintf("idempotency_marker_%d", suffix)
	badTable := fmt.Sprintf("idempotency_bad_%d", suffix)
	cleanupTable := func(table string) {
		query := "DROP TABLE IF EXISTS " + table
		if driver == "pgx" {
			query += " CASCADE"
		}
		_, _ = db.ExecContext(context.Background(), query)
	}
	t.Cleanup(func() {
		cleanupTable(badTable)
		cleanupTable(markerTable)
		cleanupTable(tableName)
	})

	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (marker VARCHAR(128) NOT NULL PRIMARY KEY)", markerTable)); err != nil {
		t.Fatalf("创建业务标记表: %v", err)
	}
	executor, err := New(ctx, db, Config{TableName: tableName})
	if err != nil {
		t.Fatal(err)
	}

	resourceName := fmt.Sprintf("idempotency-integration-%s-%d", driver, suffix)
	if err := resource.Set(resourceName, db); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFromResource(ctx, resourceName, Config{TableName: tableName}); err != nil {
		t.Fatalf("NewFromResource: %v", err)
	}

	insertMarker := func(ctx context.Context, tx *sqlx.Tx, marker string) error {
		_, err := tx.ExecContext(ctx, db.Rebind(fmt.Sprintf("INSERT INTO %s (marker) VALUES (?)", markerTable)), marker)
		return err
	}

	if err := executor.Execute(ctx, "orders", "success", func(ctx context.Context, tx *sqlx.Tx) error {
		return insertMarker(ctx, tx, "success")
	}); err != nil {
		t.Fatalf("首次执行: %v", err)
	}
	assertRowCount(t, db, tableName, "namespace = ? AND idempotency_key = ?", []any{"orders", "success"}, 1)
	assertRowCount(t, db, markerTable, "marker = ?", []any{"success"}, 1)

	called := false
	err = executor.Execute(ctx, "orders", "success", func(context.Context, *sqlx.Tx) error {
		called = true
		return nil
	})
	if !errors.Is(err, ErrConflict) || called {
		t.Fatalf("重复执行 = (err %v, called %v), want ErrConflict/false", err, called)
	}

	businessErr := errors.New("business rejected")
	err = executor.Execute(ctx, "orders", "rollback", func(ctx context.Context, tx *sqlx.Tx) error {
		if err := insertMarker(ctx, tx, "rolled-back"); err != nil {
			return err
		}
		return businessErr
	})
	if !errors.Is(err, businessErr) || errors.Is(err, ErrConflict) {
		t.Fatalf("业务失败 = %v, want original business error", err)
	}
	assertRowCount(t, db, tableName, "namespace = ? AND idempotency_key = ?", []any{"orders", "rollback"}, 0)
	assertRowCount(t, db, markerTable, "marker = ?", []any{"rolled-back"}, 0)
	if err := executor.Execute(ctx, "orders", "rollback", func(ctx context.Context, tx *sqlx.Tx) error {
		return insertMarker(ctx, tx, "retry-success")
	}); err != nil {
		t.Fatalf("回滚后重试: %v", err)
	}

	for _, namespace := range []string{"tenant-a", "tenant-b"} {
		namespace := namespace
		if err := executor.Execute(ctx, namespace, "shared", func(ctx context.Context, tx *sqlx.Tx) error {
			return insertMarker(ctx, tx, namespace)
		}); err != nil {
			t.Fatalf("独立 namespace %q: %v", namespace, err)
		}
	}

	t.Run("concurrent conflict", func(t *testing.T) {
		const workers = 12
		var handlerCalls atomic.Int32
		var successes atomic.Int32
		var conflicts atomic.Int32
		var waitGroup sync.WaitGroup
		start := make(chan struct{})
		for range workers {
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				<-start
				err := executor.Execute(ctx, "concurrent", "same-key", func(context.Context, *sqlx.Tx) error {
					handlerCalls.Add(1)
					return nil
				})
				switch {
				case err == nil:
					successes.Add(1)
				case errors.Is(err, ErrConflict):
					conflicts.Add(1)
				default:
					t.Errorf("并发执行: %v", err)
				}
			}()
		}
		close(start)
		waitGroup.Wait()
		if successes.Load() != 1 || conflicts.Load() != workers-1 || handlerCalls.Load() != 1 {
			t.Fatalf("success/conflict/handler = %d/%d/%d", successes.Load(), conflicts.Load(), handlerCalls.Load())
		}
	})

	t.Run("waiter takes over after rollback", func(t *testing.T) {
		firstEntered := make(chan struct{})
		releaseFirst := make(chan struct{})
		firstDone := make(chan error, 1)
		secondDone := make(chan error, 1)
		go func() {
			firstDone <- executor.Execute(ctx, "waiting", "rollback-key", func(context.Context, *sqlx.Tx) error {
				close(firstEntered)
				<-releaseFirst
				return businessErr
			})
		}()
		<-firstEntered
		go func() {
			secondDone <- executor.Execute(ctx, "waiting", "rollback-key", func(context.Context, *sqlx.Tx) error {
				return nil
			})
		}()
		// 给第二个事务留出进入唯一键等待的时间。
		time.Sleep(100 * time.Millisecond)
		close(releaseFirst)
		if err := <-firstDone; !errors.Is(err, businessErr) {
			t.Fatalf("第一个事务 = %v, want business error", err)
		}
		if err := <-secondDone; err != nil {
			t.Fatalf("等待事务接管失败: %v", err)
		}
	})

	// 二进制排序语义下大小写不同的键必须可以分别提交。
	for _, key := range []string{"Case-Key", "case-key"} {
		if err := executor.Execute(ctx, "case-sensitive", key, func(context.Context, *sqlx.Tx) error { return nil }); err != nil {
			t.Fatalf("大小写键 %q: %v", key, err)
		}
	}

	createIncompatibleTable(t, ctx, db, driver, badTable)
	if _, err := New(ctx, db, Config{TableName: badTable}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("不兼容表初始化错误 = %v, want ErrInvalidConfig", err)
	}
}

func assertRowCount(t *testing.T, db *sqlx.DB, table, condition string, arguments []any, expected int) {
	t.Helper()
	query := db.Rebind(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", table, condition))
	var count int
	if err := db.QueryRowContext(context.Background(), query, arguments...).Scan(&count); err != nil {
		t.Fatalf("查询 %s: %v", table, err)
	}
	if count != expected {
		t.Fatalf("%s row count = %d, want %d", table, count, expected)
	}
}

func createIncompatibleTable(t *testing.T, ctx context.Context, db *sqlx.DB, driver, table string) {
	t.Helper()
	createdType := "TIMESTAMPTZ"
	if driver == "mysql" {
		createdType = "DATETIME(6)"
	}
	query := fmt.Sprintf(`CREATE TABLE %s (
		namespace VARCHAR(128) NOT NULL,
		idempotency_key VARCHAR(256) NOT NULL,
		created_at %s NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`, table, createdType)
	if _, err := db.ExecContext(ctx, query); err != nil {
		t.Fatalf("创建不兼容表: %v", err)
	}
}
