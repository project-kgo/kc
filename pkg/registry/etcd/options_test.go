package etcd

import (
	"testing"
	"time"
)

func TestOptionsDefaultAndValidateOperationTimeout(t *testing.T) {
	options, err := (Options{}).withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if options.OperationTimeout != 5*time.Second {
		t.Fatalf("unexpected operation timeout: %v", options.OperationTimeout)
	}
	if _, err := (Options{OperationTimeout: -time.Second}).withDefaults(); err == nil {
		t.Fatal("negative operation timeout must be rejected")
	}
}
