package etcd

import (
	"errors"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var namedClients = struct {
	sync.RWMutex
	clients map[string]*clientv3.Client
}{clients: make(map[string]*clientv3.Client)}

// GetOrCreateClient returns the named client, creating it from config on first use.
func GetOrCreateClient(name string, config clientv3.Config) (*clientv3.Client, error) {
	if name == "" {
		return nil, errors.New("registry/etcd: client name is empty")
	}
	namedClients.Lock()
	defer namedClients.Unlock()
	if client := namedClients.clients[name]; client != nil {
		return client, nil
	}
	client, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}
	namedClients.clients[name] = client
	return client, nil
}

// GetClient returns a previously created named client.
func GetClient(name string) (*clientv3.Client, bool) {
	namedClients.RLock()
	defer namedClients.RUnlock()
	client, ok := namedClients.clients[name]
	return client, ok
}

// CloseClient removes and closes a named client. Missing names are ignored.
func CloseClient(name string) error {
	namedClients.Lock()
	client, ok := namedClients.clients[name]
	if ok {
		delete(namedClients.clients, name)
	}
	namedClients.Unlock()
	if !ok {
		return nil
	}
	return client.Close()
}

// CloseAllClients atomically removes and then closes every named client.
func CloseAllClients() error {
	namedClients.Lock()
	clients := namedClients.clients
	namedClients.clients = make(map[string]*clientv3.Client)
	namedClients.Unlock()
	var errs []error
	for _, client := range clients {
		if err := client.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
