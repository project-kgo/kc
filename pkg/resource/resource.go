// Package resource 提供按名称和类型存取共享资源的能力。
package resource

import (
	"errors"
	"reflect"
	"sync"
)

// ErrEmptyName 表示资源名称为空。
var ErrEmptyName = errors.New("resource: name is empty")

type resourceKey struct {
	name string
	typ  reflect.Type
}

var resources = struct {
	sync.RWMutex
	values map[resourceKey]any
}{
	values: make(map[resourceKey]any),
}

// Set 按名称和声明类型保存资源；同一名称和类型的旧值会被覆盖。
func Set[T any](name string, value T) error {
	if name == "" {
		return ErrEmptyName
	}

	key := resourceKey{name: name, typ: reflect.TypeFor[T]()}
	resources.Lock()
	resources.values[key] = value
	resources.Unlock()
	return nil
}

// Get 按名称和声明类型获取资源。资源不存在时返回 T 的零值和 false。
func Get[T any](name string) (T, bool) {
	var zero T
	if name == "" {
		return zero, false
	}

	key := resourceKey{name: name, typ: reflect.TypeFor[T]()}
	resources.RLock()
	value, ok := resources.values[key]
	resources.RUnlock()
	if !ok {
		return zero, false
	}
	// 显式注册 nil 接口时，转换为 any 后没有动态类型。
	if value == nil {
		return zero, true
	}
	return value.(T), true
}
