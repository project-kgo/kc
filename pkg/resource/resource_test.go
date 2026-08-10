package resource

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
)

type testConfig struct {
	address string
}

type testService interface {
	Name() string
}

type testClient struct {
	name string
}

type testCloser struct {
	closeErr error
	onClose  func()
	calls    int
}

func (c *testClient) Name() string {
	return c.name
}

func (c *testCloser) Close() error {
	c.calls++
	if c.onClose != nil {
		c.onClose()
	}
	return c.closeErr
}

func TestSetAndGet(t *testing.T) {
	config := &testConfig{address: "127.0.0.1:6379"}
	if err := Set("resource-test-config", config); err != nil {
		t.Fatal(err)
	}

	got, ok := Get[*testConfig]("resource-test-config")
	if !ok {
		t.Fatal("expected resource to exist")
	}
	if got != config {
		t.Fatal("Get must return the stored object")
	}
}

func TestSetAndGetExplicitInterfaceType(t *testing.T) {
	client := &testClient{name: "main"}
	if err := Set[testService]("resource-test-interface", client); err != nil {
		t.Fatal(err)
	}

	got, ok := Get[testService]("resource-test-interface")
	if !ok || got != client {
		t.Fatal("Get did not return the explicitly typed interface resource")
	}
	if _, ok := Get[*testClient]("resource-test-interface"); ok {
		t.Fatal("declared interface type must be isolated from concrete type")
	}
}

func TestGetMissingOrMismatchedResource(t *testing.T) {
	if got, ok := Get[int]("resource-test-missing"); ok || got != 0 {
		t.Fatalf("Get missing resource = (%d, %v), want (0, false)", got, ok)
	}

	if err := Set("resource-test-mismatch", 42); err != nil {
		t.Fatal(err)
	}
	if got, ok := Get[string]("resource-test-mismatch"); ok || got != "" {
		t.Fatalf("Get mismatched resource = (%q, %v), want (\"\", false)", got, ok)
	}
	if got, ok := Get[int](""); ok || got != 0 {
		t.Fatalf("Get empty name = (%d, %v), want (0, false)", got, ok)
	}
}

func TestSetRejectsEmptyName(t *testing.T) {
	if err := Set("", 1); !errors.Is(err, ErrEmptyName) {
		t.Fatalf("Set empty name error = %v, want ErrEmptyName", err)
	}
}

func TestSetOverwritesSameTypeAndIsolatesDifferentTypes(t *testing.T) {
	const name = "resource-test-overwrite"
	if err := Set(name, 1); err != nil {
		t.Fatal(err)
	}
	if err := Set(name, 2); err != nil {
		t.Fatal(err)
	}
	if err := Set(name, "two"); err != nil {
		t.Fatal(err)
	}

	if got, ok := Get[int](name); !ok || got != 2 {
		t.Fatalf("Get[int] = (%d, %v), want (2, true)", got, ok)
	}
	if got, ok := Get[string](name); !ok || got != "two" {
		t.Fatalf("Get[string] = (%q, %v), want (\"two\", true)", got, ok)
	}
}

func TestSetAndGetNilResources(t *testing.T) {
	var client *testClient
	if err := Set("resource-test-typed-nil", client); err != nil {
		t.Fatal(err)
	}
	if got, ok := Get[*testClient]("resource-test-typed-nil"); !ok || got != nil {
		t.Fatalf("Get typed nil = (%v, %v), want (nil, true)", got, ok)
	}

	var service testService
	if err := Set[testService]("resource-test-nil-interface", service); err != nil {
		t.Fatal(err)
	}
	if got, ok := Get[testService]("resource-test-nil-interface"); !ok || got != nil {
		t.Fatalf("Get nil interface = (%v, %v), want (nil, true)", got, ok)
	}
}

func TestGetOrCreateReusesExistingResource(t *testing.T) {
	const name = "resource-test-get-or-create-existing"
	want := &testClient{name: "existing"}
	if err := Set(name, want); err != nil {
		t.Fatal(err)
	}

	called := false
	got, err := GetOrCreate(name, func() (*testClient, error) {
		called = true
		return &testClient{name: "new"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("已有资源时不应调用 factory")
	}
	if got != want {
		t.Fatal("GetOrCreate 必须返回已有资源")
	}
}

func TestGetOrCreateCreatesResource(t *testing.T) {
	const name = "resource-test-get-or-create-new"
	want := &testClient{name: "new"}

	got, err := GetOrCreate(name, func() (*testClient, error) {
		return want, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatal("GetOrCreate 必须返回新创建的资源")
	}
	if stored, ok := Get[*testClient](name); !ok || stored != want {
		t.Fatal("GetOrCreate 未保存新创建的资源")
	}
}

func TestGetOrCreateRetriesAfterFactoryError(t *testing.T) {
	const name = "resource-test-get-or-create-retry"
	wantErr := errors.New("create failed")

	if _, err := GetOrCreate(name, func() (*testClient, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("GetOrCreate error = %v, want %v", err, wantErr)
	}
	if _, ok := Get[*testClient](name); ok {
		t.Fatal("创建失败的资源不应被缓存")
	}

	want := &testClient{name: "retry"}
	got, err := GetOrCreate(name, func() (*testClient, error) {
		return want, nil
	})
	if err != nil || got != want {
		t.Fatalf("retry GetOrCreate = (%v, %v), want (%v, nil)", got, err, want)
	}
}

func TestConcurrentGetOrCreateCallsFactoryOnce(t *testing.T) {
	const (
		name    = "resource-test-get-or-create-concurrent"
		workers = 32
	)
	want := &testClient{name: "shared"}
	start := make(chan struct{})
	results := make(chan *testClient, workers)
	errs := make(chan error, workers)
	var calls int
	var callsMu sync.Mutex
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			got, err := GetOrCreate(name, func() (*testClient, error) {
				callsMu.Lock()
				calls++
				callsMu.Unlock()
				return want, nil
			})
			results <- got
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for got := range results {
		if got != want {
			t.Fatal("并发调用必须返回同一个资源")
		}
	}
	callsMu.Lock()
	defer callsMu.Unlock()
	if calls != 1 {
		t.Fatalf("factory calls = %d, want 1", calls)
	}
}

func TestGetOrCreateValidatesArguments(t *testing.T) {
	factory := func() (int, error) { return 1, nil }
	if _, err := GetOrCreate("", factory); !errors.Is(err, ErrEmptyName) {
		t.Fatalf("GetOrCreate empty name error = %v, want ErrEmptyName", err)
	}
	if _, err := GetOrCreate[int]("resource-test-nil-factory", nil); !errors.Is(err, ErrNilFactory) {
		t.Fatalf("GetOrCreate nil factory error = %v, want ErrNilFactory", err)
	}
}

func TestConcurrentSetAndGet(t *testing.T) {
	const (
		name       = "resource-test-concurrent"
		workers    = 32
		iterations = 500
	)
	if err := Set(name, 0); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range iterations {
				if err := Set(name, worker*iterations+i); err != nil {
					errCh <- err
					return
				}
				if _, ok := Get[int](name); !ok {
					errCh <- fmt.Errorf("worker %d: resource unexpectedly missing", worker)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func TestShutdownClosesResourcesAndClearsRegistry(t *testing.T) {
	prepareShutdownTest(t)

	closer := &testCloser{}
	if err := Set("resource-test-shutdown-closer", closer); err != nil {
		t.Fatal(err)
	}
	if err := Set("resource-test-shutdown-value", 42); err != nil {
		t.Fatal(err)
	}

	if err := Shutdown(); err != nil {
		t.Fatalf("Shutdown error = %v, want nil", err)
	}
	if closer.calls != 1 {
		t.Fatalf("Close calls = %d, want 1", closer.calls)
	}
	if _, ok := Get[*testCloser]("resource-test-shutdown-closer"); ok {
		t.Fatal("Shutdown 后 Closer 资源仍然存在")
	}
	if _, ok := Get[int]("resource-test-shutdown-value"); ok {
		t.Fatal("Shutdown 后普通资源仍然存在")
	}

	if err := Shutdown(); err != nil {
		t.Fatalf("second Shutdown error = %v, want nil", err)
	}
	if closer.calls != 1 {
		t.Fatalf("second Shutdown Close calls = %d, want 1", closer.calls)
	}
}

func TestShutdownAggregatesCloseErrorsAndContinues(t *testing.T) {
	prepareShutdownTest(t)

	firstErr := errors.New("first close failed")
	secondErr := errors.New("second close failed")
	first := &testCloser{closeErr: firstErr}
	second := &testCloser{closeErr: secondErr}
	if err := Set("resource-test-shutdown-error-first", first); err != nil {
		t.Fatal(err)
	}
	if err := Set("resource-test-shutdown-error-second", second); err != nil {
		t.Fatal(err)
	}

	err := Shutdown()
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("Shutdown error = %v, want both close errors", err)
	}
	if first.calls != 1 || second.calls != 1 {
		t.Fatalf("Close calls = (%d, %d), want (1, 1)", first.calls, second.calls)
	}
	if _, ok := Get[*testCloser]("resource-test-shutdown-error-first"); ok {
		t.Fatal("关闭失败的资源仍然存在")
	}
	if _, ok := Get[*testCloser]("resource-test-shutdown-error-second"); ok {
		t.Fatal("关闭失败的资源仍然存在")
	}
}

func TestShutdownSkipsNilClosers(t *testing.T) {
	prepareShutdownTest(t)

	var typedNil *testCloser
	if err := Set("resource-test-shutdown-typed-nil", typedNil); err != nil {
		t.Fatal(err)
	}
	var nilCloser io.Closer
	if err := Set[io.Closer]("resource-test-shutdown-nil-interface", nilCloser); err != nil {
		t.Fatal(err)
	}

	if err := Shutdown(); err != nil {
		t.Fatalf("Shutdown error = %v, want nil", err)
	}
	if _, ok := Get[*testCloser]("resource-test-shutdown-typed-nil"); ok {
		t.Fatal("Shutdown 后 typed nil 资源仍然存在")
	}
	if _, ok := Get[io.Closer]("resource-test-shutdown-nil-interface"); ok {
		t.Fatal("Shutdown 后 nil 接口资源仍然存在")
	}
}

func TestShutdownClosesEachRegistration(t *testing.T) {
	prepareShutdownTest(t)

	closer := &testCloser{}
	if err := Set("resource-test-shutdown-duplicate-first", closer); err != nil {
		t.Fatal(err)
	}
	if err := Set("resource-test-shutdown-duplicate-second", closer); err != nil {
		t.Fatal(err)
	}

	if err := Shutdown(); err != nil {
		t.Fatalf("Shutdown error = %v, want nil", err)
	}
	if closer.calls != 2 {
		t.Fatalf("Close calls = %d, want 2", closer.calls)
	}
}

func TestShutdownAllowsCloseToRegisterReplacement(t *testing.T) {
	prepareShutdownTest(t)

	const name = "resource-test-shutdown-reentrant"
	replacement := &testCloser{}
	var registerErr error
	original := &testCloser{onClose: func() {
		registerErr = Set(name, replacement)
	}}
	if err := Set(name, original); err != nil {
		t.Fatal(err)
	}

	if err := Shutdown(); err != nil {
		t.Fatalf("Shutdown error = %v, want nil", err)
	}
	if registerErr != nil {
		t.Fatalf("Close 中重新注册资源失败: %v", registerErr)
	}
	if got, ok := Get[*testCloser](name); !ok || got != replacement {
		t.Fatalf("Get replacement = (%v, %v), want (%v, true)", got, ok, replacement)
	}
}

func TestShutdownDoesNotWaitForCreatingResource(t *testing.T) {
	prepareShutdownTest(t)

	const name = "resource-test-shutdown-creating"
	closer := &testCloser{}
	factoryStarted := make(chan struct{})
	releaseFactory := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := GetOrCreate(name, func() (*testCloser, error) {
			close(factoryStarted)
			<-releaseFactory
			return closer, nil
		})
		result <- err
	}()

	<-factoryStarted
	if err := Shutdown(); err != nil {
		t.Fatalf("Shutdown error = %v, want nil", err)
	}
	close(releaseFactory)
	if err := <-result; err != nil {
		t.Fatalf("GetOrCreate error = %v, want nil", err)
	}

	if closer.calls != 0 {
		t.Fatalf("Close calls = %d, want 0", closer.calls)
	}
	if got, ok := Get[*testCloser](name); !ok || got != closer {
		t.Fatalf("Get created resource = (%v, %v), want (%v, true)", got, ok, closer)
	}
}

func prepareShutdownTest(t *testing.T) {
	t.Helper()
	if err := Shutdown(); err != nil {
		t.Fatalf("prepare Shutdown error = %v", err)
	}
	t.Cleanup(func() {
		_ = Shutdown()
	})
}
