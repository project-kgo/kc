// Package registry defines backend-neutral service registration and resolution.
package registry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

var (
	ErrInvalidInstance     = errors.New("registry: invalid instance")
	ErrInvalidResult       = errors.New("registry: invalid result")
	ErrNoAvailableInstance = errors.New("registry: no available instance")
	ErrClosed              = errors.New("registry: closed")
	ErrAlreadyReported     = errors.New("registry: resolution already reported")
	ErrReportExpired       = errors.New("registry: resolution report expired")
)

// Instance describes one h2c service instance.
type Instance struct {
	Service  string            `json:"service"`
	ID       string            `json:"id"`
	Endpoint string            `json:"endpoint"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Validate checks that the instance can be safely stored and resolved.
func (i Instance) Validate() error {
	if invalidSegment(i.Service) {
		return fmt.Errorf("%w: invalid service %q", ErrInvalidInstance, i.Service)
	}
	if invalidSegment(i.ID) {
		return fmt.Errorf("%w: invalid id %q", ErrInvalidInstance, i.ID)
	}
	u, err := url.Parse(i.Endpoint)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Host == "" || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("%w: endpoint must be http://host:port", ErrInvalidInstance)
	}
	if _, port, err := net.SplitHostPort(u.Host); err != nil || port == "" {
		return fmt.Errorf("%w: endpoint must include a valid port", ErrInvalidInstance)
	}
	for key := range i.Metadata {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("%w: metadata key is empty", ErrInvalidInstance)
		}
	}
	return nil
}

func invalidSegment(value string) bool {
	value = strings.TrimSpace(value)
	return value == "" || value == "." || value == ".." || strings.ContainsAny(value, `/\\`)
}

// Clone returns a copy whose metadata can be mutated independently.
func (i Instance) Clone() Instance {
	clone := i
	if i.Metadata != nil {
		clone.Metadata = make(map[string]string, len(i.Metadata))
		for key, value := range i.Metadata {
			clone.Metadata[key] = value
		}
	}
	return clone
}

// Outcome describes the transport-level result of one resolved request.
type Outcome uint8

const (
	// OutcomeSuccess means the transport received a valid response.
	OutcomeSuccess Outcome = iota
	// OutcomeFailure means the request failed at the connection, timeout, or protocol layer.
	OutcomeFailure
)

// Result feeds latency and transport health back to the resolver.
type Result struct {
	Outcome Outcome
	Latency time.Duration
}

func (r Result) Validate() error {
	if (r.Outcome != OutcomeSuccess && r.Outcome != OutcomeFailure) || r.Latency <= 0 {
		return ErrInvalidResult
	}
	return nil
}

// Registration represents a leased service registration.
type Registration interface {
	Done() <-chan struct{}
	Err() error
	Close(context.Context) error
}

// Resolution binds an instance choice to exactly one result report.
type Resolution interface {
	Instance() Instance
	Report(Result) error
}

// Registrar registers leased service instances.
type Registrar interface {
	Register(context.Context, Instance, time.Duration) (Registration, error)
}

// Resolver selects instances while maintaining discovery and health state internally.
type Resolver interface {
	Resolve(context.Context, string) (Resolution, error)
	Close() error
}

// Registry combines registration and resolution.
type Registry interface {
	Registrar
	Resolver
}
