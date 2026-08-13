package infra

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/jmoiron/sqlx"
	coremq "github.com/project-kgo/kc/pkg/mq"
	"github.com/project-kgo/kc/pkg/resource"
	"github.com/redis/go-redis/v9"
)

const defaultCheckTimeout = 5 * time.Second

var initMu sync.Mutex

type preparedClient struct {
	name   string
	module string
	typ    string
	commit func() error
	close  func(context.Context) error
}

// Init 校验并初始化 Data 与 MQ 模块中的全部客户端，成功后统一注册到 resource。
func Init(ctx context.Context, config Config) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidConfig)
	}

	// 初始化过程串行化，避免两个 Init 同时通过重复资源检查。
	initMu.Lock()
	defer initMu.Unlock()

	if err := validateConfig(config); err != nil {
		return err
	}
	if err := validateResourcesAvailable(config); err != nil {
		return err
	}

	prepared := make([]preparedClient, 0, len(config.Data)+len(config.MQ))
	for _, name := range sortedMapKeys(config.Data) {
		dataConfig := config.Data[name]
		client, err := prepareData(ctx, name, dataConfig)
		if err != nil {
			return rollback(prepared, fmt.Errorf("infra: initialize data %q (%s): %w", name, dataConfig.Type, err))
		}
		prepared = append(prepared, client)
	}
	for _, name := range sortedMapKeys(config.MQ) {
		mqConfig := config.MQ[name]
		client, err := prepareMQ(ctx, name, mqConfig)
		if err != nil {
			return rollback(prepared, fmt.Errorf("infra: initialize mq %q (%s): %w", name, mqConfig.Type, err))
		}
		prepared = append(prepared, client)
	}

	// 名称已预先校验，resource.Set 在提交阶段不会失败。
	for _, client := range prepared {
		if err := client.commit(); err != nil {
			return rollback(prepared, fmt.Errorf("infra: register %s %q (%s): %w", client.module, client.name, client.typ, err))
		}
	}
	return nil
}

func validateResourcesAvailable(config Config) error {
	for _, name := range sortedMapKeys(config.Data) {
		if dataResourceExists(name) {
			return fmt.Errorf("%w: data %q", ErrAlreadyInitialized, name)
		}
	}
	for _, name := range sortedMapKeys(config.MQ) {
		if _, ok := resource.Get[coremq.MQ](name); ok {
			return fmt.Errorf("%w: mq %q", ErrAlreadyInitialized, name)
		}
	}
	return nil
}

func dataResourceExists(name string) bool {
	if _, ok := resource.Get[*sqlx.DB](name); ok {
		return true
	}
	if _, ok := resource.Get[*redis.Client](name); ok {
		return true
	}
	if _, ok := resource.Get[*redis.ClusterClient](name); ok {
		return true
	}
	_, ok := resource.Get[*elasticsearch.TypedClient](name)
	return ok
}

func prepareData(ctx context.Context, name string, config DataConfig) (preparedClient, error) {
	switch config.Type {
	case DataTypeMySQL, DataTypePostgreSQL:
		return prepareSQL(ctx, name, config)
	case DataTypeRedis:
		return prepareRedis(ctx, name, config)
	case DataTypeRedisCluster:
		return prepareRedisCluster(ctx, name, config)
	case DataTypeElasticsearch:
		return prepareElasticsearch(ctx, name, config)
	default:
		return preparedClient{}, fmt.Errorf("%w: data %q", ErrUnsupportedType, config.Type)
	}
}

func prepareMQ(ctx context.Context, name string, config MQConfig) (preparedClient, error) {
	switch config.Type {
	case MQTypeKafka, MQTypeKafkaShare:
		return prepareKafka(ctx, name, config)
	case MQTypeRedisStream, MQTypeRedisStreamCluster:
		return prepareRedisStream(ctx, name, config)
	default:
		return preparedClient{}, fmt.Errorf("%w: mq %q", ErrUnsupportedType, config.Type)
	}
}

func checkContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout == 0 {
		timeout = defaultCheckTimeout
	}
	return context.WithTimeout(parent, timeout)
}

func rollback(clients []preparedClient, cause error) error {
	errs := []error{cause}
	ctx, cancel := context.WithTimeout(context.Background(), defaultCheckTimeout)
	defer cancel()

	for i := len(clients) - 1; i >= 0; i-- {
		client := clients[i]
		if client.close == nil {
			continue
		}
		if err := client.close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("infra: close %s %q (%s): %w", client.module, client.name, client.typ, err))
		}
	}
	return errors.Join(errs...)
}
