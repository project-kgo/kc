package infra

import (
	"testing"
	"time"
)

func TestRedisDSNConnectionFieldsTakePriority(t *testing.T) {
	config := DataConfig{
		Type: DataTypeRedis,
		DSN:  "redis://dsn-user:dsn-pass@127.0.0.1:6380/3?pool_size=8",
		Redis: &RedisConfig{
			Addrs:       []string{"ignored:6379"},
			Username:    "ignored-user",
			Password:    "ignored-pass",
			DB:          9,
			ClientName:  "worker",
			PoolSize:    16,
			DialTimeout: 2 * time.Second,
		},
	}

	options, err := redisOptions(config)
	if err != nil {
		t.Fatal(err)
	}
	if options.Addr != "127.0.0.1:6380" || options.Username != "dsn-user" || options.Password != "dsn-pass" || options.DB != 3 {
		t.Fatalf("DSN connection fields not preserved: %+v", options)
	}
	if options.PoolSize != 8 {
		t.Fatalf("DSN pool size = %d, want 8", options.PoolSize)
	}
	if options.ClientName != "worker" || options.DialTimeout != 2*time.Second {
		t.Fatalf("nested tuning was not applied: %+v", options)
	}
}

func TestRedisClusterDSNParsesAdditionalAddresses(t *testing.T) {
	config := DataConfig{
		Type: DataTypeRedisCluster,
		DSN:  "redis://user:pass@127.0.0.1:6379?addr=127.0.0.1:6380&route_by_latency=true",
		Redis: &RedisConfig{
			PoolSize: 12,
		},
	}

	options, err := redisClusterOptions(config)
	if err != nil {
		t.Fatal(err)
	}
	if len(options.Addrs) != 2 || options.Addrs[0] != "127.0.0.1:6379" || options.Addrs[1] != "127.0.0.1:6380" {
		t.Fatalf("cluster addresses = %v", options.Addrs)
	}
	if !options.RouteByLatency || options.PoolSize != 12 {
		t.Fatalf("cluster tuning not applied: %+v", options)
	}
}
