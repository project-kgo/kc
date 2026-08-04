package infra

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/project-kgo/kc/pkg/resource"
	"github.com/redis/go-redis/v9"
)

func validateRedisConfig(config DataConfig) error {
	redisConfig := config.Redis
	if redisConfig != nil {
		if redisConfig.DB < 0 {
			return fmt.Errorf("%w: redis db is negative", ErrInvalidConfig)
		}
		if config.Type == DataTypeRedisCluster && redisConfig.DB != 0 {
			return fmt.Errorf("%w: redis cluster does not support db", ErrInvalidConfig)
		}
		if redisConfig.Protocol != 0 && redisConfig.Protocol != 2 && redisConfig.Protocol != 3 {
			return fmt.Errorf("%w: redis protocol must be 2 or 3", ErrInvalidConfig)
		}
		if redisConfig.PoolSize < 0 || redisConfig.MinIdleConns < 0 || redisConfig.DialTimeout < 0 || redisConfig.ReadTimeout < 0 || redisConfig.WriteTimeout < 0 {
			return fmt.Errorf("%w: redis pool or timeout option is negative", ErrInvalidConfig)
		}
	}

	if config.DSN != "" {
		var err error
		if config.Type == DataTypeRedis {
			_, err = redis.ParseURL(config.DSN)
		} else {
			_, err = redis.ParseClusterURL(config.DSN)
			if err == nil && redisClusterDSNUsesDB(config.DSN) {
				return fmt.Errorf("%w: redis cluster does not support db", ErrInvalidConfig)
			}
		}
		if err != nil {
			return fmt.Errorf("%w: invalid redis dsn", ErrInvalidConfig)
		}
		return nil
	}
	if redisConfig == nil {
		return fmt.Errorf("%w: redis config is missing", ErrInvalidConfig)
	}
	if config.Type == DataTypeRedis && len(redisConfig.Addrs) != 1 {
		return fmt.Errorf("%w: redis requires exactly one address", ErrInvalidConfig)
	}
	if config.Type == DataTypeRedisCluster && len(redisConfig.Addrs) == 0 {
		return fmt.Errorf("%w: redis cluster address is empty", ErrInvalidConfig)
	}
	for _, address := range redisConfig.Addrs {
		if strings.TrimSpace(address) == "" {
			return fmt.Errorf("%w: redis address is empty", ErrInvalidConfig)
		}
	}
	return nil
}

func redisClusterDSNUsesDB(dsn string) bool {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return false
	}
	db := strings.Trim(parsed.Path, "/")
	return db != "" && db != "0"
}

func prepareRedis(ctx context.Context, name string, config DataConfig) (preparedClient, error) {
	options, err := redisOptions(config)
	if err != nil {
		return preparedClient{}, err
	}
	client := redis.NewClient(options)
	if !config.SkipCheck {
		checkCtx, cancel := checkContext(ctx, config.CheckTimeout)
		err = client.Ping(checkCtx).Err()
		cancel()
		if err != nil {
			return preparedClient{}, errors.Join(fmt.Errorf("redis connection check: %w", err), client.Close())
		}
	}

	return preparedClient{
		name:   name,
		module: "data",
		typ:    string(config.Type),
		commit: func() error {
			return resource.Set(name, client)
		},
		close: func(context.Context) error {
			return client.Close()
		},
	}, nil
}

func prepareRedisCluster(ctx context.Context, name string, config DataConfig) (preparedClient, error) {
	options, err := redisClusterOptions(config)
	if err != nil {
		return preparedClient{}, err
	}
	client := redis.NewClusterClient(options)
	if !config.SkipCheck {
		checkCtx, cancel := checkContext(ctx, config.CheckTimeout)
		err = client.Ping(checkCtx).Err()
		cancel()
		if err != nil {
			return preparedClient{}, errors.Join(fmt.Errorf("redis cluster connection check: %w", err), client.Close())
		}
	}

	return preparedClient{
		name:   name,
		module: "data",
		typ:    string(config.Type),
		commit: func() error {
			return resource.Set(name, client)
		},
		close: func(context.Context) error {
			return client.Close()
		},
	}, nil
}

func redisOptions(config DataConfig) (*redis.Options, error) {
	var options *redis.Options
	var err error
	if config.DSN != "" {
		options, err = redis.ParseURL(config.DSN)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid redis dsn", ErrInvalidConfig)
		}
	} else {
		redisConfig := config.Redis
		options = &redis.Options{
			Addr:       redisConfig.Addrs[0],
			Username:   redisConfig.Username,
			Password:   redisConfig.Password,
			DB:         redisConfig.DB,
			ClientName: redisConfig.ClientName,
			Protocol:   redisConfig.Protocol,
		}
		if redisConfig.TLS {
			options.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
	}
	applyRedisTuning(options, config.Redis)
	return options, nil
}

func redisClusterOptions(config DataConfig) (*redis.ClusterOptions, error) {
	var options *redis.ClusterOptions
	var err error
	if config.DSN != "" {
		options, err = redis.ParseClusterURL(config.DSN)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid redis cluster dsn", ErrInvalidConfig)
		}
	} else {
		redisConfig := config.Redis
		options = &redis.ClusterOptions{
			Addrs:      append([]string(nil), redisConfig.Addrs...),
			Username:   redisConfig.Username,
			Password:   redisConfig.Password,
			ClientName: redisConfig.ClientName,
			Protocol:   redisConfig.Protocol,
		}
		if redisConfig.TLS {
			options.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
	}
	applyRedisClusterTuning(options, config.Redis)
	return options, nil
}

// DSN 已配置的调优参数优先，嵌套配置只填充 DSN 中未提供的值。
func applyRedisTuning(options *redis.Options, config *RedisConfig) {
	if config == nil {
		return
	}
	if options.ClientName == "" {
		options.ClientName = config.ClientName
	}
	if options.Protocol == 0 {
		options.Protocol = config.Protocol
	}
	if options.PoolSize == 0 {
		options.PoolSize = config.PoolSize
	}
	if options.MinIdleConns == 0 {
		options.MinIdleConns = config.MinIdleConns
	}
	if options.DialTimeout == 0 {
		options.DialTimeout = config.DialTimeout
	}
	if options.ReadTimeout == 0 {
		options.ReadTimeout = config.ReadTimeout
	}
	if options.WriteTimeout == 0 {
		options.WriteTimeout = config.WriteTimeout
	}
}

func applyRedisClusterTuning(options *redis.ClusterOptions, config *RedisConfig) {
	if config == nil {
		return
	}
	if options.ClientName == "" {
		options.ClientName = config.ClientName
	}
	if options.Protocol == 0 {
		options.Protocol = config.Protocol
	}
	if options.PoolSize == 0 {
		options.PoolSize = config.PoolSize
	}
	if options.MinIdleConns == 0 {
		options.MinIdleConns = config.MinIdleConns
	}
	if options.DialTimeout == 0 {
		options.DialTimeout = config.DialTimeout
	}
	if options.ReadTimeout == 0 {
		options.ReadTimeout = config.ReadTimeout
	}
	if options.WriteTimeout == 0 {
		options.WriteTimeout = config.WriteTimeout
	}
	options.ReadOnly = options.ReadOnly || config.ReadOnly
	options.RouteByLatency = options.RouteByLatency || config.RouteByLatency
	options.RouteRandomly = options.RouteRandomly || config.RouteRandomly
}
