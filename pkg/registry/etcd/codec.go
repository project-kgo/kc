package etcd

import (
	"encoding/json"
	"fmt"

	"github.com/project-kgo/kc/pkg/registry"
)

// storedInstance is the versioned JSON representation persisted in etcd.
type storedInstance struct {
	Version  int               `json:"version"`
	Service  string            `json:"service"`
	ID       string            `json:"id"`
	Endpoint string            `json:"endpoint"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// encodeInstance serializes an instance using the current storage schema.
func encodeInstance(instance registry.Instance) ([]byte, error) {
	return json.Marshal(storedInstance{Version: 1, Service: instance.Service, ID: instance.ID, Endpoint: instance.Endpoint, Metadata: instance.Metadata})
}

// decodeInstance validates schema version and instance fields while decoding.
func decodeInstance(key string, value []byte) (registry.Instance, error) {
	var stored storedInstance
	if err := json.Unmarshal(value, &stored); err != nil {
		return registry.Instance{}, fmt.Errorf("registry/etcd: decode %q: %w", key, err)
	}
	instance := registry.Instance{Service: stored.Service, ID: stored.ID, Endpoint: stored.Endpoint, Metadata: stored.Metadata}
	if stored.Version != 1 {
		return registry.Instance{}, fmt.Errorf("registry/etcd: decode %q: unsupported schema version %d", key, stored.Version)
	}
	if err := instance.Validate(); err != nil {
		return registry.Instance{}, fmt.Errorf("registry/etcd: decode %q: %w", key, err)
	}
	return instance, nil
}
