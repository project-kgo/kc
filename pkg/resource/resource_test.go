package resource

import (
	"errors"
	"fmt"
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

func (c *testClient) Name() string {
	return c.name
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
