package registry

import (
	"errors"
	"testing"
	"time"
)

func TestInstanceValidate(t *testing.T) {
	valid := Instance{Service: "users.v1", ID: "node-1", Endpoint: "http://127.0.0.1:8080", Metadata: map[string]string{"zone": "a"}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid instance rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Instance)
	}{
		{"empty service", func(i *Instance) { i.Service = "" }},
		{"service path", func(i *Instance) { i.Service = "a/b" }},
		{"empty id", func(i *Instance) { i.ID = "" }},
		{"id path", func(i *Instance) { i.ID = "../x" }},
		{"https", func(i *Instance) { i.Endpoint = "https://127.0.0.1:8080" }},
		{"missing port", func(i *Instance) { i.Endpoint = "http://127.0.0.1" }},
		{"path", func(i *Instance) { i.Endpoint = "http://127.0.0.1:8080/rpc" }},
		{"metadata empty key", func(i *Instance) { i.Metadata = map[string]string{"": "x"} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := valid.Clone()
			tt.mutate(&got)
			if err := got.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestInstanceCloneDeepCopiesMetadata(t *testing.T) {
	original := Instance{Metadata: map[string]string{"zone": "a"}}
	clone := original.Clone()
	clone.Metadata["zone"] = "b"
	if original.Metadata["zone"] != "a" {
		t.Fatal("clone mutated original metadata")
	}
}

func TestResultValidate(t *testing.T) {
	if err := (Result{Outcome: OutcomeSuccess, Latency: time.Millisecond}).Validate(); err != nil {
		t.Fatalf("valid result rejected: %v", err)
	}
	if !errors.Is((Result{Outcome: Outcome(99), Latency: time.Millisecond}).Validate(), ErrInvalidResult) {
		t.Fatal("invalid outcome must return ErrInvalidResult")
	}
	if !errors.Is((Result{Outcome: OutcomeSuccess}).Validate(), ErrInvalidResult) {
		t.Fatal("non-positive latency must return ErrInvalidResult")
	}
}
