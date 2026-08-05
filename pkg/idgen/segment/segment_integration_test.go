package segment

import (
	"context"
	"fmt"
	"os"
	"sort"
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

	tableName := fmt.Sprintf("segment_it_%s_%d", driver, time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+tableName)
	})
	config := Config{TableName: tableName}

	resourceName := fmt.Sprintf("segment-integration-%s-%d", driver, time.Now().UnixNano())
	if err := resource.Set(resourceName, db); err != nil {
		t.Fatal(err)
	}

	first, err := New(ctx, db, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	second, err := NewFromResource(ctx, resourceName, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })

	const (
		bizTag          = "integration-orders"
		step            = int32(11)
		idsPerGenerator = 600
	)
	if err := first.Init(ctx, bizTag, 1000, step); err != nil {
		t.Fatal(err)
	}
	if err := second.Init(ctx, bizTag, 0, step); err != nil {
		t.Fatal(err)
	}

	// 两个独立生成器对同一标签并发跨越多个号段。
	ids := make(chan int64, idsPerGenerator*2)
	errorsChannel := make(chan error, idsPerGenerator*2)
	var wg sync.WaitGroup
	for _, generator := range []*SegmentIDGen{first, second} {
		for range idsPerGenerator {
			wg.Add(1)
			go func() {
				defer wg.Done()
				id, getErr := generator.GetID(ctx, bizTag)
				if getErr != nil {
					errorsChannel <- getErr
					return
				}
				ids <- id
			}()
		}
	}
	wg.Wait()
	close(ids)
	close(errorsChannel)
	for getErr := range errorsChannel {
		t.Error(getErr)
	}

	values := make([]int64, 0, idsPerGenerator*2)
	unique := make(map[int64]struct{}, idsPerGenerator*2)
	for id := range ids {
		values = append(values, id)
		unique[id] = struct{}{}
	}
	if len(values) != idsPerGenerator*2 || len(unique) != len(values) {
		t.Fatalf("generated IDs = %d, unique IDs = %d", len(values), len(unique))
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	if values[0] <= 1000 {
		t.Fatalf("minimum ID = %d, want > 1000", values[0])
	}

	var databaseMaxID int64
	maxQuery := db.Rebind(fmt.Sprintf("SELECT max_id FROM %s WHERE biz_tag = ?", tableName))
	if err := db.QueryRowxContext(ctx, maxQuery, bizTag).Scan(&databaseMaxID); err != nil {
		t.Fatal(err)
	}
	if databaseMaxID < values[len(values)-1] {
		t.Fatalf("database max_id = %d, generated max = %d", databaseMaxID, values[len(values)-1])
	}

	// 重复初始化只更新后续步长，不得使用传入的 startID 重置进度。
	if err := first.Init(ctx, bizTag, 0, 7); err != nil {
		t.Fatal(err)
	}
	third, err := New(ctx, db, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = third.Close() })
	if err := third.Init(ctx, bizTag, 0, 7); err != nil {
		t.Fatal(err)
	}
	restartedID, err := third.GetID(ctx, bizTag)
	if err != nil {
		t.Fatal(err)
	}
	if restartedID <= databaseMaxID {
		t.Fatalf("ID after restart = %d, want > previous database max %d", restartedID, databaseMaxID)
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := third.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("database was closed by SegmentIDGen.Close: %v", err)
	}
}
