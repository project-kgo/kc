package etcdclient

import (
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestNamedClientIsReusedAndCanBeRecreated(t *testing.T) {
	defer CloseAllClients()
	cfg := testConfig()
	first, err := GetOrCreateClient("main", cfg)
	if err != nil {
		t.Fatal(err)
	}
	second, err := GetOrCreateClient("main", clientv3.Config{Endpoints: []string{"http://ignored:2379"}})
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("same name must reuse client")
	}
	if got, ok := GetClient("main"); !ok || got != first {
		t.Fatal("GetClient did not return named client")
	}
	if err := CloseClient("main"); err != nil {
		t.Fatal(err)
	}
	third, err := GetOrCreateClient("main", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if third == first {
		t.Fatal("closed name must create a new client")
	}
}

func TestConcurrentGetOrCreateReusesOneClient(t *testing.T) {
	defer CloseAllClients()
	const workers = 32
	clients := make(chan *clientv3.Client, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := GetOrCreateClient("shared", testConfig())
			clients <- client
			errs <- err
		}()
	}
	wg.Wait()
	close(clients)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var expected *clientv3.Client
	for client := range clients {
		if expected == nil {
			expected = client
		}
		if client != expected {
			t.Fatal("concurrent creation returned different clients")
		}
	}
}

func TestNamedClientRejectsEmptyName(t *testing.T) {
	if _, err := GetOrCreateClient("", clientv3.Config{}); err == nil {
		t.Fatal("expected empty name error")
	}
}

func testConfig() clientv3.Config {
	return clientv3.Config{Endpoints: []string{"http://127.0.0.1:2379"}, DialTimeout: 50 * time.Millisecond}
}
