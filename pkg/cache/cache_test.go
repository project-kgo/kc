package cache

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type testKey struct {
	Tenant int
	Name   string
}

type testKeyCodec struct{}

func (testKeyCodec) Encode(key testKey) (string, error) {
	data, err := json.Marshal(key)
	return string(data), err
}

func (testKeyCodec) Decode(value string) (testKey, error) {
	var key testKey
	err := json.Unmarshal([]byte(value), &key)
	return key, err
}

func TestCachePreservesGenericKeys(t *testing.T) {
	c, err := New[testKey, string](context.Background(), Config[testKey]{
		DefaultTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	key := testKey{Tenant: 7, Name: "profile"}
	if err := c.Set(key, "value"); err != nil {
		t.Fatal(err)
	}
	if got, ok := c.Get(key); !ok || got != "value" {
		t.Fatalf("Get() = (%q, %v), want (value, true)", got, ok)
	}
}

func TestCacheTTLDoesNotRefreshOnGet(t *testing.T) {
	c, err := New[int, string](context.Background(), Config[int]{
		DefaultTTL: 70 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Set(1, "value"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(35 * time.Millisecond)
	if _, ok := c.Get(1); !ok {
		t.Fatal("value expired too early")
	}
	time.Sleep(50 * time.Millisecond)
	if _, ok := c.Get(1); ok {
		t.Fatal("Get unexpectedly refreshed the TTL")
	}
}

func TestCacheSetWithTTLOverridesDefault(t *testing.T) {
	c, err := New[string, string](context.Background(), Config[string]{
		DefaultTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.SetWithTTL("short", "value", 25*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(45 * time.Millisecond)
	if _, ok := c.Get("short"); ok {
		t.Fatal("entry with overridden TTL is still present")
	}
}

func TestCacheCapacity(t *testing.T) {
	c, err := New[int, int](context.Background(), Config[int]{
		DefaultTTL: time.Minute,
		Capacity:   2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for i := 0; i < 8; i++ {
		if err := c.Set(i, i); err != nil {
			t.Fatal(err)
		}
	}
	if got := c.Len(); got > 2 {
		t.Fatalf("Len() = %d, want at most 2", got)
	}
}

func TestPrimitiveCodecSupportsNamedTypes(t *testing.T) {
	type userID int64
	codec, err := resolveCodec[userID](nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.Encode(userID(42))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != userID(42) {
		t.Fatalf("decoded key = %v, want 42", decoded)
	}
}

func TestUnsupportedKeyTypeRequiresCodecWhenRedisIsConfigured(t *testing.T) {
	if _, err := resolveCodec[testKey](nil); !errors.Is(err, ErrUnsupportedKeyType) {
		t.Fatalf("resolveCodec() error = %v, want ErrUnsupportedKeyType", err)
	}
	codec, err := resolveCodec[testKey](testKeyCodec{})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.Encode(testKey{Tenant: 1, Name: "x"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.Decode(encoded)
	if err != nil || decoded != (testKey{Tenant: 1, Name: "x"}) {
		t.Fatalf("codec round trip = (%#v, %v)", decoded, err)
	}
}

func TestCacheCloseRejectsWrites(t *testing.T) {
	c, err := New[int, string](context.Background(), Config[int]{
		DefaultTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Set(1, "value"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Set() error = %v, want ErrClosed", err)
	}
	if err := c.Delete(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Delete() error = %v, want ErrClosed", err)
	}
}

func TestHandleInvalidationDecodesKeyAndFiltersNamespace(t *testing.T) {
	c, err := New[int, string](context.Background(), Config[int]{
		DefaultTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.codec, err = resolveCodec[int](nil)
	if err != nil {
		t.Fatal(err)
	}
	c.namespace = "orders"

	if err := c.Set(10, "value"); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(invalidationMessage{Version: cacheMessageVersion, Namespace: "other", Key: "10"})
	if err != nil {
		t.Fatal(err)
	}
	c.handleInvalidation(string(payload))
	if _, ok := c.Get(10); !ok {
		t.Fatal("different namespace removed the cache entry")
	}

	payload, err = json.Marshal(invalidationMessage{Version: cacheMessageVersion, Namespace: "orders", Key: "10"})
	if err != nil {
		t.Fatal(err)
	}
	c.handleInvalidation(string(payload))
	if _, ok := c.Get(10); ok {
		t.Fatal("matching namespace did not remove the cache entry")
	}

	if err := c.Set(10, "value"); err != nil {
		t.Fatal(err)
	}
	for _, message := range []invalidationMessage{
		{Version: cacheMessageVersion + 1, Namespace: "orders", Key: "10"},
		{Version: cacheMessageVersion, Namespace: "orders", Key: "not-an-int"},
	} {
		payload, err := json.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		c.handleInvalidation(string(payload))
	}
	if _, ok := c.Get(10); !ok {
		t.Fatal("invalidation with invalid version or key removed the cache entry")
	}
}

func TestHandleInvalidationUsesCustomCodec(t *testing.T) {
	c, err := New[testKey, string](context.Background(), Config[testKey]{
		DefaultTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.codec = testKeyCodec{}
	c.namespace = "profiles"

	key := testKey{Tenant: 9, Name: "avatar"}
	if err := c.Set(key, "value"); err != nil {
		t.Fatal(err)
	}
	encoded, err := c.codec.Encode(key)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(invalidationMessage{
		Version:   cacheMessageVersion,
		Namespace: "profiles",
		Key:       encoded,
	})
	if err != nil {
		t.Fatal(err)
	}
	c.handleInvalidation(string(payload))
	if _, ok := c.Get(key); ok {
		t.Fatal("custom codec key was not invalidated")
	}
}
