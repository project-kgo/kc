package snowflake

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
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

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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
	const generatorCount = 16
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
	if _, exists := workerIDs[coordinatorWorkerID]; exists {
		t.Fatal("coordinator row must not be allocated as a worker")
	}
	assertCoordinatorRow(t, ctx, db, tableName, config.Epoch)

	if _, err := New(ctx, db, Config{TableName: tableName, Epoch: config.Epoch + 1}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("epoch mismatch error = %v, want ErrInvalidConfig", err)
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
	victimLastID, err := victim.Generate()
	if err != nil {
		t.Fatal(err)
	}
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
	replacementFirstID, err := replacement.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if replacementFirstID <= victimLastID {
		t.Fatalf("first ID after worker reuse = %d, must be greater than %d", replacementFirstID, victimLastID)
	}

	// 删除一个中间 worker 行，分配器应填补空洞而不是继续使用最大 ID 加一。
	holeVictim := generators[1]
	holeWorkerID := holeVictim.GetWorkerID()
	if err := holeVictim.Close(); err != nil {
		t.Fatal(err)
	}
	deleteQuery := db.Rebind(fmt.Sprintf("DELETE FROM %s WHERE worker_id = ?", tableName))
	if _, err := db.ExecContext(ctx, deleteQuery, holeWorkerID); err != nil {
		t.Fatal(err)
	}
	holeReplacement, err := New(ctx, db, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = holeReplacement.Close() })
	if holeReplacement.GetWorkerID() != holeWorkerID {
		t.Fatalf("worker ID after filling gap = %d, want %d", holeReplacement.GetWorkerID(), holeWorkerID)
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

	testWorkerCapacityAndGap(t, ctx, db, driver)
	testExistingTableMigration(t, ctx, db, driver)
	testInvalidWorkerTableRejected(t, ctx, db, driver)
}

func assertCoordinatorRow(t *testing.T, ctx context.Context, db *sqlx.DB, tableName string, epoch int64) {
	t.Helper()
	query := db.Rebind(fmt.Sprintf(
		"SELECT last_timestamp, ip_address, updated_at, lease_token FROM %s WHERE worker_id = ?",
		tableName,
	))
	var storedEpoch, updatedAt int64
	var ipAddress, leaseToken string
	if err := db.QueryRowContext(ctx, query, coordinatorWorkerID).Scan(
		&storedEpoch,
		&ipAddress,
		&updatedAt,
		&leaseToken,
	); err != nil {
		t.Fatal(err)
	}
	if storedEpoch != epoch || ipAddress != coordinatorIPAddress || updatedAt != coordinatorUpdatedAt || leaseToken != coordinatorLeaseToken {
		t.Fatalf("unexpected coordinator row: epoch=%d ip=%q updated_at=%d token=%q", storedEpoch, ipAddress, updatedAt, leaseToken)
	}
}

func testWorkerCapacityAndGap(t *testing.T, ctx context.Context, db *sqlx.DB, driver string) {
	t.Helper()
	tableName := fmt.Sprintf("snowflake_capacity_%s_%d", driver, time.Now().UnixNano())
	config := Config{TableName: tableName, Epoch: time.Now().Add(-time.Hour).UnixMilli()}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+tableName)
	})

	bootstrap, err := New(ctx, db, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE worker_id >= 0", tableName)); err != nil {
		t.Fatal(err)
	}

	values := make([]string, 0, MaxWorkerID+1)
	arguments := make([]any, 0, (MaxWorkerID+1)*5)
	timestamp := time.Now().UnixMilli()
	for workerID := int64(0); workerID <= MaxWorkerID; workerID++ {
		values = append(values, "(?, ?, ?, ?, ?)")
		arguments = append(arguments, workerID, timestamp, "test", coordinatorUpdatedAt, "capacity")
	}
	insertQuery := db.Rebind(fmt.Sprintf(
		"INSERT INTO %s (worker_id, last_timestamp, ip_address, updated_at, lease_token) VALUES %s",
		tableName,
		strings.Join(values, ","),
	))
	if _, err := db.ExecContext(ctx, insertQuery, arguments...); err != nil {
		t.Fatal(err)
	}

	if _, err := New(ctx, db, config); !errors.Is(err, ErrNoWorkerID) {
		t.Fatalf("full worker table error = %v, want ErrNoWorkerID", err)
	}
	deleteQuery := db.Rebind(fmt.Sprintf("DELETE FROM %s WHERE worker_id = ?", tableName))
	if _, err := db.ExecContext(ctx, deleteQuery, int64(17)); err != nil {
		t.Fatal(err)
	}
	replacement, err := New(ctx, db, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = replacement.Close() }()
	if replacement.GetWorkerID() != 17 {
		t.Fatalf("worker ID after capacity gap = %d, want 17", replacement.GetWorkerID())
	}
}

func testExistingTableMigration(t *testing.T, ctx context.Context, db *sqlx.DB, driver string) {
	t.Helper()
	tableName := fmt.Sprintf("snowflake_migration_%s_%d", driver, time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+tableName)
	})
	createWorkerTableWithoutConstraints(t, ctx, db, tableName)
	insertQuery := db.Rebind(fmt.Sprintf(
		"INSERT INTO %s (worker_id, last_timestamp, ip_address, updated_at, lease_token) VALUES (?, ?, ?, ?, ?)",
		tableName,
	))
	if _, err := db.ExecContext(ctx, insertQuery, int64(0), time.Now().UnixMilli(), "test", coordinatorUpdatedAt, "existing"); err != nil {
		t.Fatal(err)
	}

	config := Config{TableName: tableName, Epoch: time.Now().Add(-time.Hour).UnixMilli()}
	generator, err := New(ctx, db, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = generator.Close() }()
	if generator.GetWorkerID() != 1 {
		t.Fatalf("worker ID after schema migration = %d, want 1", generator.GetWorkerID())
	}
	assertCoordinatorRow(t, ctx, db, tableName, config.Epoch)
	if _, err := db.ExecContext(ctx, insertQuery, MaxWorkerID+1, time.Now().UnixMilli(), "test", coordinatorUpdatedAt, "invalid"); err == nil {
		t.Fatal("worker ID range constraint did not reject 1024")
	}
}

func testInvalidWorkerTableRejected(t *testing.T, ctx context.Context, db *sqlx.DB, driver string) {
	t.Helper()
	tableName := fmt.Sprintf("snowflake_invalid_%s_%d", driver, time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+tableName)
	})
	createWorkerTableWithoutConstraints(t, ctx, db, tableName)
	insertQuery := db.Rebind(fmt.Sprintf(
		"INSERT INTO %s (worker_id, last_timestamp, ip_address, updated_at, lease_token) VALUES (?, ?, ?, ?, ?)",
		tableName,
	))
	for range 2 {
		if _, err := db.ExecContext(ctx, insertQuery, int64(0), time.Now().UnixMilli(), "test", coordinatorUpdatedAt, "duplicate"); err != nil {
			t.Fatal(err)
		}
	}
	config := Config{TableName: tableName, Epoch: time.Now().Add(-time.Hour).UnixMilli()}
	if _, err := New(ctx, db, config); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("duplicate worker table error = %v, want ErrInvalidConfig", err)
	}
}

func createWorkerTableWithoutConstraints(t *testing.T, ctx context.Context, db *sqlx.DB, tableName string) {
	t.Helper()
	query := fmt.Sprintf(`CREATE TABLE %s (
		worker_id INTEGER NOT NULL,
		last_timestamp BIGINT NOT NULL,
		ip_address VARCHAR(45) NOT NULL,
		updated_at BIGINT NOT NULL,
		lease_token VARCHAR(64) NOT NULL
	)`, tableName)
	if _, err := db.ExecContext(ctx, query); err != nil {
		t.Fatal(err)
	}
}
