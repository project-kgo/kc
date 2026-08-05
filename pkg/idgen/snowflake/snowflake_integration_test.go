package snowflake

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

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

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, err := sqlx.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("连接 %s 数据库: %v", driver, err)
	}

	tableName := fmt.Sprintf("snowflake_it_%s_%d", driver, time.Now().UnixNano())
	config := Config{TableName: tableName, Epoch: time.Now().Add(-time.Hour).UnixMilli()}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+tableName)
	})

	resourceName := fmt.Sprintf("snowflake-integration-%s-%d", driver, time.Now().UnixNano())
	if err := resource.Set(resourceName, db); err != nil {
		t.Fatal(err)
	}

	// 并发从空表初始化，覆盖唯一键冲突重试和两种构造方式。
	const generatorCount = 4
	generators := make([]*Snowflake, generatorCount)
	errorsChannel := make(chan error, generatorCount)
	var createGroup sync.WaitGroup
	for index := range generatorCount {
		createGroup.Add(1)
		go func() {
			defer createGroup.Done()
			var generator *Snowflake
			var createErr error
			if index == 0 {
				generator, createErr = NewFromResource(ctx, resourceName, config)
			} else {
				generator, createErr = New(ctx, db, config)
			}
			if createErr != nil {
				errorsChannel <- createErr
				return
			}
			generators[index] = generator
		}()
	}
	createGroup.Wait()
	close(errorsChannel)
	for createErr := range errorsChannel {
		t.Error(createErr)
	}
	if t.Failed() {
		for _, generator := range generators {
			if generator != nil {
				_ = generator.Close()
			}
		}
		t.FailNow()
	}
	for _, generator := range generators {
		generator := generator
		t.Cleanup(func() { _ = generator.Close() })
	}

	workerIDs := make(map[int64]struct{}, generatorCount)
	for _, generator := range generators {
		workerIDs[generator.GetWorkerID()] = struct{}{}
	}
	if len(workerIDs) != generatorCount {
		t.Fatalf("unique worker IDs = %d, want %d", len(workerIDs), generatorCount)
	}

	const idsPerGenerator = 500
	ids := make(chan int64, generatorCount*idsPerGenerator)
	errorsChannel = make(chan error, generatorCount*idsPerGenerator)
	var generateGroup sync.WaitGroup
	for _, generator := range generators {
		for range idsPerGenerator {
			generateGroup.Add(1)
			go func() {
				defer generateGroup.Done()
				id, generateErr := generator.Generate()
				if generateErr != nil {
					errorsChannel <- generateErr
					return
				}
				ids <- id
			}()
		}
	}
	generateGroup.Wait()
	close(ids)
	close(errorsChannel)
	for generateErr := range errorsChannel {
		t.Error(generateErr)
	}
	uniqueIDs := make(map[int64]struct{}, generatorCount*idsPerGenerator)
	for id := range ids {
		uniqueIDs[id] = struct{}{}
	}
	if len(uniqueIDs) != generatorCount*idsPerGenerator {
		t.Fatalf("unique generated IDs = %d, want %d", len(uniqueIDs), generatorCount*idsPerGenerator)
	}

	// 主动关闭会把租约立即标记为可回收，新实例应拿到相同 worker ID。
	victim := generators[0]
	victimWorkerID := victim.GetWorkerID()
	if err := victim.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, err := New(ctx, db, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replacement.Close() })
	if replacement.GetWorkerID() != victimWorkerID {
		t.Fatalf("reclaimed worker ID = %d, want %d", replacement.GetWorkerID(), victimWorkerID)
	}

	// 模拟旧租约被另一实例接管，旧 token 的心跳必须失败。
	stealQuery := db.Rebind(fmt.Sprintf("UPDATE %s SET lease_token = ? WHERE worker_id = ?", tableName))
	if _, err := db.ExecContext(ctx, stealQuery, "replacement-token", replacement.GetWorkerID()); err != nil {
		t.Fatal(err)
	}
	if err := replacement.updateHeartbeat(); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale lease heartbeat error = %v, want ErrLeaseLost", err)
	}
	replacement.state.CompareAndSwap(stateActive, stateLeaseLost)
	if _, err := replacement.Generate(); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale generator error = %v, want ErrLeaseLost", err)
	}

	// Snowflake.Close 不能关闭借用的共享数据库连接。
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("database was closed by Snowflake.Close: %v", err)
	}
}
