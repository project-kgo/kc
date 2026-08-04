package infra

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/jmoiron/sqlx"
	. "github.com/project-kgo/kc/pkg/mq"
	"github.com/project-kgo/kc/pkg/resource"
	"github.com/redis/go-redis/v9"
)

func TestInitRegistersDataAndMQWithoutNetworkChecks(t *testing.T) {
	config := Config{
		Data: map[string]DataConfig{
			"infra-test-mysql": {
				Type: DataTypeMySQL, DSN: "user:pass@tcp(127.0.0.1:3306)/db", SkipCheck: true,
				SQL: &SQLConfig{MaxOpenConns: 7},
			},
			"infra-test-postgres": {
				Type: DataTypePostgreSQL, DSN: "postgres://user:pass@127.0.0.1:5432/db?sslmode=disable", SkipCheck: true,
			},
			"infra-test-redis": {
				Type: DataTypeRedis, DSN: "redis://127.0.0.1:6379/0", SkipCheck: true,
			},
			"infra-test-redis-cluster": {
				Type: DataTypeRedisCluster, DSN: "redis://127.0.0.1:6379?addr=127.0.0.1:6380", SkipCheck: true,
			},
			"infra-test-elasticsearch": {
				Type: DataTypeElasticsearch, DSN: "http://127.0.0.1:9200", SkipCheck: true,
			},
		},
		MQ: map[string]MQConfig{
			"infra-test-redis-stream-base": {
				Type: MQTypeRedisStream, DSN: "redis://127.0.0.1:6379/0", SkipCheck: true,
				RedisStream: &RedisStreamConfig{},
			},
		},
	}
	if err := Init(context.Background(), config); err != nil {
		t.Fatal(err)
	}

	mysqlDB := mustResource[*sqlx.DB](t, "infra-test-mysql")
	if mysqlDB.Stats().MaxOpenConnections != 7 {
		t.Fatalf("MaxOpenConnections = %d, want 7", mysqlDB.Stats().MaxOpenConnections)
	}
	postgresDB := mustResource[*sqlx.DB](t, "infra-test-postgres")
	redisClient := mustResource[*redis.Client](t, "infra-test-redis")
	redisCluster := mustResource[*redis.ClusterClient](t, "infra-test-redis-cluster")
	elasticsearchClient := mustResource[*elasticsearch.TypedClient](t, "infra-test-elasticsearch")
	mqClient := mustResource[MQ](t, "infra-test-redis-stream-base")

	_ = mysqlDB.Close()
	_ = postgresDB.Close()
	_ = redisClient.Close()
	_ = redisCluster.Close()
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	_ = elasticsearchClient.Close(closeCtx)
	cancel()
	_ = mqClient.Close()
}

func TestInitAllowsSameNameAcrossDataAndMQ(t *testing.T) {
	const name = "infra-test-shared-name"
	config := Config{
		Data: map[string]DataConfig{
			name: {Type: DataTypeRedis, DSN: "redis://127.0.0.1:6379/0", SkipCheck: true},
		},
		MQ: map[string]MQConfig{
			name: {
				Type: MQTypeRedisStream, DSN: "redis://127.0.0.1:6379/0", SkipCheck: true,
				RedisStream: &RedisStreamConfig{},
			},
		},
	}
	if err := Init(context.Background(), config); err != nil {
		t.Fatal(err)
	}

	redisClient := mustResource[*redis.Client](t, name)
	mqClient := mustResource[MQ](t, name)
	_ = redisClient.Close()
	_ = mqClient.Close()
}

func TestInitRegistersRedisStreamMQWithoutNetworkCheck(t *testing.T) {
	const (
		standaloneName = "infra-test-redis-stream"
		clusterName    = "infra-test-redis-stream-cluster"
	)
	config := Config{MQ: map[string]MQConfig{
		standaloneName: {
			Type: MQTypeRedisStream, DSN: "redis://127.0.0.1:6379/0", SkipCheck: true,
			RedisStream: &RedisStreamConfig{Concurrency: 4},
		},
		clusterName: {
			Type: MQTypeRedisStreamCluster, SkipCheck: true,
			Redis:       &RedisConfig{Addrs: []string{"127.0.0.1:6379", "127.0.0.1:6380"}},
			RedisStream: &RedisStreamConfig{},
		},
	}}
	if err := Init(context.Background(), config); err != nil {
		t.Fatal(err)
	}

	standalone := mustResource[MQ](t, standaloneName)
	cluster := mustResource[MQ](t, clusterName)
	if _, ok := standalone.(*redisStreamMQ); !ok {
		t.Fatalf("standalone MQ type = %T", standalone)
	}
	if _, ok := cluster.(*redisStreamMQ); !ok {
		t.Fatalf("cluster MQ type = %T", cluster)
	}
	if err := standalone.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestInitRejectsExistingNameWithinDataModule(t *testing.T) {
	const name = "infra-test-existing-data"
	redisConfig := Config{Data: map[string]DataConfig{
		name: {Type: DataTypeRedis, DSN: "redis://127.0.0.1:6379/0", SkipCheck: true},
	}}
	if err := Init(context.Background(), redisConfig); err != nil {
		t.Fatal(err)
	}
	mysqlConfig := Config{Data: map[string]DataConfig{
		name: {Type: DataTypeMySQL, DSN: "user:pass@tcp(127.0.0.1:3306)/db", SkipCheck: true},
	}}
	if err := Init(context.Background(), mysqlConfig); !errors.Is(err, ErrAlreadyInitialized) {
		t.Fatalf("existing data error = %v, want ErrAlreadyInitialized", err)
	}
	if _, ok := resource.Get[*sqlx.DB](name); ok {
		t.Fatal("conflicting data resource must not be published")
	}
	client := mustResource[*redis.Client](t, name)
	_ = client.Close()
}

func TestInitChecksConnectivityAndDoesNotPublishPartialConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	const dataName = "infra-test-rollback-mysql"
	const mqName = "infra-test-rollback-redis-stream"
	config := Config{
		Data: map[string]DataConfig{
			dataName: {
				Type: DataTypeMySQL, DSN: "user:pass@tcp(127.0.0.1:3306)/db", SkipCheck: true,
			},
		},
		MQ: map[string]MQConfig{
			mqName: {
				Type: MQTypeRedisStream, DSN: "redis://127.0.0.1:6379/0", RedisStream: &RedisStreamConfig{},
			},
		},
	}
	err := Init(ctx, config)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Init error = %v, want context.Canceled", err)
	}
	if _, ok := resource.Get[*sqlx.DB](dataName); ok {
		t.Fatal("staged data client must not be published")
	}
	if _, ok := resource.Get[MQ](mqName); ok {
		t.Fatal("failed MQ client must not be published")
	}
}

func TestInitRejectsNilContext(t *testing.T) {
	if err := Init(nil, Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Init(nil) error = %v, want ErrInvalidConfig", err)
	}
}

func TestConcurrentInitAllowsOnlyOneInitializer(t *testing.T) {
	const (
		name    = "infra-test-concurrent-init"
		workers = 16
	)
	config := Config{Data: map[string]DataConfig{
		name: {Type: DataTypeRedis, DSN: "redis://127.0.0.1:6379/0", SkipCheck: true},
	}}

	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- Init(context.Background(), config)
		}()
	}
	wg.Wait()
	close(errs)

	var successes int
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrAlreadyInitialized):
		default:
			t.Errorf("unexpected Init error: %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful initializers = %d, want 1", successes)
	}
	client := mustResource[*redis.Client](t, name)
	_ = client.Close()
}

func TestRollbackClosesInReverseOrderAndJoinsErrors(t *testing.T) {
	cause := errors.New("initialization failed")
	closeErr := errors.New("close failed")
	var order []int
	clients := []preparedClient{
		{name: "first", module: "data", typ: string(DataTypeRedis), close: func(context.Context) error {
			order = append(order, 1)
			return closeErr
		}},
		{name: "second", module: "mq", typ: string(MQTypeRedisStream), close: func(context.Context) error {
			order = append(order, 2)
			return nil
		}},
	}

	err := rollback(clients, cause)
	if !errors.Is(err, cause) || !errors.Is(err, closeErr) {
		t.Fatalf("rollback error = %v, want joined cause and close error", err)
	}
	if len(order) != 2 || order[0] != 2 || order[1] != 1 {
		t.Fatalf("close order = %v, want [2 1]", order)
	}
}

func mustResource[T any](t *testing.T, name string) T {
	t.Helper()
	value, ok := resource.Get[T](name)
	if !ok {
		t.Fatalf("resource %q is missing", name)
	}
	return value
}
