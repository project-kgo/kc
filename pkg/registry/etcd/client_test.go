package etcd

import (
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestNamedClientIsReusedAndCanBeRecreated(t *testing.T) {
	defer CloseAllClients()
	cfg := clientv3.Config{Endpoints: []string{"http://127.0.0.1:2379"}, DialTimeout: 50 * time.Millisecond}
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

func TestNamedClientRejectsEmptyName(t *testing.T) {
	if _, err := GetOrCreateClient("", clientv3.Config{}); err == nil {
		t.Fatal("expected empty name error")
	}
}
