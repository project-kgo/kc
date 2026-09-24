// Package cache 提供带固定 TTL 的泛型本地缓存，以及可选的 Redis 跨实例失效通知。
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maypok86/otter/v2"
	"github.com/redis/go-redis/v9"
)

const cacheMessageVersion = 1

var (
	// ErrInvalidConfig 表示缓存配置缺失或不合法。
	ErrInvalidConfig = errors.New("cache: invalid config")
	// ErrInvalidTTL 表示 TTL 不是正数。
	ErrInvalidTTL = errors.New("cache: invalid ttl")
	// ErrUnsupportedKeyType 表示键类型无法自动编解码，且没有提供自定义 Codec。
	ErrUnsupportedKeyType = errors.New("cache: unsupported key type")
	// ErrInvalidArgument 表示调用参数不合法。
	ErrInvalidArgument = errors.New("cache: invalid argument")
	// ErrClosed 表示缓存已经关闭。
	ErrClosed = errors.New("cache: closed")
)

// KeyCodec 将泛型键转换为 Redis 消息中的字符串，并负责反向转换。
type KeyCodec[K comparable] interface {
	Encode(K) (string, error)
	Decode(string) (K, error)
}

// Config 配置一个本地缓存实例。
type Config[K comparable] struct {
	DefaultTTL time.Duration
	Capacity   uint64

	Redis         redis.UniversalClient
	PubSubChannel string
	Namespace     string
	KeyCodec      KeyCodec[K]
	Logger        *slog.Logger
}

// Cache 是线程安全的泛型本地缓存。
type Cache[K comparable, V any] struct {
	local *otter.Cache[K, V]
	codec keyCodec[K]

	redis     redis.UniversalClient
	pubsub    *redis.PubSub
	channel   string
	namespace string
	logger    *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed atomic.Bool
	once   sync.Once
}

type keyCodec[K comparable] interface {
	Encode(K) (string, error)
	Decode(string) (K, error)
}

type invalidationMessage struct {
	Version   int    `json:"version"`
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
}

// New 创建缓存。Redis 未配置时只启用本地缓存；配置 Redis 后会同步完成订阅握手。
func New[K comparable, V any](ctx context.Context, config Config[K]) (*Cache[K, V], error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidConfig)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: context is already done: %w", ErrInvalidConfig, err)
	}
	if config.DefaultTTL <= 0 {
		return nil, fmt.Errorf("%w: default ttl must be positive", ErrInvalidTTL)
	}
	if config.Redis != nil {
		if strings.TrimSpace(config.PubSubChannel) == "" {
			return nil, fmt.Errorf("%w: pub/sub channel is empty", ErrInvalidConfig)
		}
		if strings.TrimSpace(config.Namespace) == "" {
			return nil, fmt.Errorf("%w: namespace is empty", ErrInvalidConfig)
		}
	}

	codec, err := resolveCodec(config.KeyCodec)
	if err != nil {
		if config.Redis != nil {
			return nil, err
		}
		// 没有 Redis 时，任意 comparable 键都可以直接保存在本地。
		codec = nil
	}

	options := &otter.Options[K, V]{
		ExpiryCalculator: otter.ExpiryWriting[K, V](config.DefaultTTL),
	}
	if config.Capacity > 0 {
		if config.Capacity > uint64(int(^uint(0)>>1)) {
			return nil, fmt.Errorf("%w: capacity is too large", ErrInvalidConfig)
		}
		options.MaximumSize = int(config.Capacity)
	}
	local, err := otter.New(options)
	if err != nil {
		return nil, fmt.Errorf("create local cache: %w", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	c := &Cache[K, V]{
		local:     local,
		codec:     codec,
		redis:     config.Redis,
		channel:   config.PubSubChannel,
		namespace: config.Namespace,
		logger:    config.Logger,
		ctx:       runCtx,
		cancel:    cancel,
	}
	if c.logger == nil {
		c.logger = slog.Default()
	}

	if c.redis == nil {
		return c, nil
	}

	c.pubsub = c.redis.Subscribe(ctx, c.channel)
	if _, err := c.pubsub.Receive(ctx); err != nil {
		cancel()
		_ = c.pubsub.Close()
		local.StopAllGoroutines()
		return nil, fmt.Errorf("subscribe cache invalidation channel: %w", err)
	}
	c.wg.Add(1)
	go c.runSubscriber()
	return c, nil
}

// Get 获取未过期的缓存值。读取不会刷新 TTL。
func (c *Cache[K, V]) Get(key K) (V, bool) {
	if c == nil || c.closed.Load() {
		var zero V
		return zero, false
	}
	return c.local.GetIfPresent(key)
}

// Set 使用实例默认 TTL 写入缓存。
func (c *Cache[K, V]) Set(key K, value V) error {
	if c == nil {
		return ErrClosed
	}
	if c.closed.Load() {
		return ErrClosed
	}
	if err := c.validateKey(key); err != nil {
		return err
	}
	c.local.Set(key, value)
	return nil
}

// SetWithTTL 使用指定 TTL 写入缓存。
func (c *Cache[K, V]) SetWithTTL(key K, value V, ttl time.Duration) error {
	if c == nil {
		return ErrClosed
	}
	if ttl <= 0 {
		return fmt.Errorf("%w: ttl must be positive", ErrInvalidTTL)
	}
	if c.closed.Load() {
		return ErrClosed
	}
	if err := c.validateKey(key); err != nil {
		return err
	}
	c.local.Set(key, value)
	c.local.SetExpiresAfter(key, ttl)
	return nil
}

// Delete 删除当前实例中的键，并向其他实例发布失效消息。
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	if c == nil || c.closed.Load() {
		return ErrClosed
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidArgument)
	}
	c.local.Invalidate(key)
	if c.redis == nil {
		return nil
	}

	encoded, err := c.codec.Encode(key)
	if err != nil {
		return fmt.Errorf("encode cache key for invalidation: %w", err)
	}
	payload, err := json.Marshal(invalidationMessage{
		Version:   cacheMessageVersion,
		Namespace: c.namespace,
		Key:       encoded,
	})
	if err != nil {
		return fmt.Errorf("marshal cache invalidation: %w", err)
	}
	if err := c.redis.Publish(ctx, c.channel, payload).Err(); err != nil {
		return fmt.Errorf("publish cache invalidation: %w", err)
	}
	return nil
}

// Len 返回当前缓存中条目的近似数量。
func (c *Cache[K, V]) Len() int {
	if c == nil || c.closed.Load() {
		return 0
	}
	c.local.CleanUp()
	return c.local.EstimatedSize()
}

// Close 停止缓存后台任务和 Redis 订阅，但不会关闭调用方注入的 Redis 客户端。
func (c *Cache[K, V]) Close() error {
	if c == nil {
		return nil
	}
	var closeErr error
	c.once.Do(func() {
		c.closed.Store(true)
		c.cancel()
		if c.pubsub != nil {
			closeErr = c.pubsub.Close()
		}
		c.wg.Wait()
		c.local.StopAllGoroutines()
	})
	return closeErr
}

func (c *Cache[K, V]) validateKey(key K) error {
	if c.redis == nil {
		return nil
	}
	_, err := c.codec.Encode(key)
	if err != nil {
		return fmt.Errorf("encode cache key: %w", err)
	}
	return nil
}

func (c *Cache[K, V]) runSubscriber() {
	defer c.wg.Done()
	for {
		message, err := c.pubsub.ReceiveMessage(c.ctx)
		if err == nil {
			c.handleInvalidation(message.Payload)
			continue
		}
		if c.ctx.Err() != nil {
			return
		}
		c.logger.WarnContext(context.Background(), "receive cache invalidation failed", "err", err)
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (c *Cache[K, V]) handleInvalidation(payload string) {
	var message invalidationMessage
	if err := json.Unmarshal([]byte(payload), &message); err != nil {
		c.logger.Warn("decode cache invalidation failed", "err", err)
		return
	}
	if message.Version != cacheMessageVersion || message.Namespace != c.namespace {
		return
	}
	key, err := c.codec.Decode(message.Key)
	if err != nil {
		c.logger.Warn("decode cache key for invalidation failed", "err", err)
		return
	}
	c.local.Invalidate(key)
}

func resolveCodec[K comparable](custom KeyCodec[K]) (keyCodec[K], error) {
	if custom != nil {
		return custom, nil
	}
	typ := reflect.TypeFor[K]()
	switch typ.Kind() {
	case reflect.String, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64:
		return primitiveCodec[K]{typ: typ}, nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedKeyType, typ)
	}
}

type primitiveCodec[K comparable] struct{ typ reflect.Type }

func (c primitiveCodec[K]) Encode(key K) (string, error) {
	value := reflect.ValueOf(key)
	switch c.typ.Kind() {
	case reflect.String:
		return value.String(), nil
	case reflect.Bool:
		return strconv.FormatBool(value.Bool()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(value.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(value.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		floatValue := value.Float()
		if math.IsNaN(floatValue) {
			return "", fmt.Errorf("%w: NaN key is not supported", ErrInvalidArgument)
		}
		return strconv.FormatFloat(floatValue, 'g', -1, value.Type().Bits()), nil
	default:
		return "", fmt.Errorf("%w: %s", ErrUnsupportedKeyType, c.typ)
	}
}

func (c primitiveCodec[K]) Decode(encoded string) (K, error) {
	value := reflect.New(c.typ).Elem()
	var err error
	switch c.typ.Kind() {
	case reflect.String:
		value.SetString(encoded)
	case reflect.Bool:
		var parsed bool
		parsed, err = strconv.ParseBool(encoded)
		value.SetBool(parsed)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var parsed int64
		parsed, err = strconv.ParseInt(encoded, 10, c.typ.Bits())
		value.SetInt(parsed)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		var parsed uint64
		parsed, err = strconv.ParseUint(encoded, 10, c.typ.Bits())
		value.SetUint(parsed)
	case reflect.Float32, reflect.Float64:
		var parsed float64
		parsed, err = strconv.ParseFloat(encoded, c.typ.Bits())
		value.SetFloat(parsed)
	default:
		return *new(K), fmt.Errorf("%w: %s", ErrUnsupportedKeyType, c.typ)
	}
	if err != nil {
		return *new(K), err
	}
	return value.Interface().(K), nil
}
