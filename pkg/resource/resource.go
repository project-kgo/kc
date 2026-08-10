// Package resource 提供按名称和类型存取共享资源的能力。
package resource

import (
	"errors"
	"io"
	"reflect"
	"sync"
)

var (
	// ErrEmptyName 表示资源名称为空。
	ErrEmptyName = errors.New("resource: name is empty")
	// ErrNilFactory 表示资源创建函数为空。
	ErrNilFactory = errors.New("resource: factory is nil")
)

type resourceKey struct {
	name string
	typ  reflect.Type
}

type resourceCall struct {
	done       chan struct{}
	value      any
	err        error
	panicValue any
}

var resources = struct {
	sync.RWMutex
	values   map[resourceKey]any
	creating map[resourceKey]*resourceCall
}{
	values:   make(map[resourceKey]any),
	creating: make(map[resourceKey]*resourceCall),
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
	return cast[T](value), true
}

// Shutdown 关闭当前已注册且实现 io.Closer 的全部资源，并清空这些资源的注册记录。
// 关闭期间新注册或仍在创建的资源不属于本次关闭范围。
func Shutdown() error {
	resources.Lock()
	values := resources.values
	resources.values = make(map[resourceKey]any)
	resources.Unlock()

	var errs []error
	for _, value := range values {
		closer, ok := value.(io.Closer)
		if !ok || isNil(closer) {
			continue
		}
		if err := closer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// GetOrCreate 按名称和声明类型获取资源；资源不存在时调用 factory 创建并保存。
// 同一名称和类型的并发调用只会执行一次 factory。创建失败不会缓存，后续调用可再次尝试。
func GetOrCreate[T any](name string, factory func() (T, error)) (T, error) {
	var zero T
	if name == "" {
		return zero, ErrEmptyName
	}
	if factory == nil {
		return zero, ErrNilFactory
	}

	key := resourceKey{name: name, typ: reflect.TypeFor[T]()}
	resources.RLock()
	value, ok := resources.values[key]
	resources.RUnlock()
	if ok {
		return cast[T](value), nil
	}

	resources.Lock()
	if value, ok := resources.values[key]; ok {
		resources.Unlock()
		return cast[T](value), nil
	}
	if call, ok := resources.creating[key]; ok {
		resources.Unlock()
		<-call.done
		if call.panicValue != nil {
			panic(call.panicValue)
		}
		if call.err != nil {
			return zero, call.err
		}
		return cast[T](call.value), nil
	}

	call := &resourceCall{done: make(chan struct{})}
	resources.creating[key] = call
	resources.Unlock()

	return create(key, call, factory)
}

func create[T any](key resourceKey, call *resourceCall, factory func() (T, error)) (value T, err error) {
	completed := false
	defer func() {
		if completed {
			return
		}

		// factory 发生 panic 时也要唤醒等待方，并允许后续调用重新创建。
		panicValue := recover()
		resources.Lock()
		call.panicValue = panicValue
		delete(resources.creating, key)
		close(call.done)
		resources.Unlock()
		panic(panicValue)
	}()

	value, err = factory()
	resources.Lock()
	call.value = value
	call.err = err
	if err == nil {
		resources.values[key] = value
	}
	delete(resources.creating, key)
	close(call.done)
	resources.Unlock()
	completed = true
	return value, err
}

func cast[T any](value any) T {
	// 显式注册 nil 接口时，转换为 any 后没有动态类型。
	if value == nil {
		var zero T
		return zero
	}
	return value.(T)
}

func isNil(value any) bool {
	if value == nil {
		return true
	}

	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
