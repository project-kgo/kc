package etcd

import (
	"errors"
	"time"
)

const defaultPrefix = "/kc/registry/v1"

// Options configures etcd storage and P2C health behavior. Zero values use defaults.
type Options struct {
	Prefix           string
	FailureThreshold int
	EjectionDuration time.Duration
	ReportTimeout    time.Duration
	InitialLatency   time.Duration
	EWMAAlpha        float64
	// CleanupInterval controls the shared report-expiry and service-eviction sweep.
	CleanupInterval time.Duration
	// ServiceIdleTTL removes cached services and their watches after inactivity.
	ServiceIdleTTL time.Duration
	// RefreshInterval rate-limits full reloads while every instance is ejected.
	RefreshInterval time.Duration
	// OperationTimeout bounds internal lease cleanup and Registry shutdown calls.
	OperationTimeout time.Duration
}

// withDefaults applies production-safe defaults and validates every option.
func (o Options) withDefaults() (Options, error) {
	if o.Prefix == "" {
		o.Prefix = defaultPrefix
	}
	if o.FailureThreshold == 0 {
		o.FailureThreshold = 3
	}
	if o.EjectionDuration == 0 {
		o.EjectionDuration = 30 * time.Second
	}
	if o.ReportTimeout == 0 {
		o.ReportTimeout = 10 * time.Second
	}
	if o.InitialLatency == 0 {
		o.InitialLatency = 10 * time.Millisecond
	}
	if o.EWMAAlpha == 0 {
		o.EWMAAlpha = .2
	}
	if o.CleanupInterval == 0 {
		o.CleanupInterval = time.Second
	}
	if o.ServiceIdleTTL == 0 {
		o.ServiceIdleTTL = 10 * time.Minute
	}
	if o.RefreshInterval == 0 {
		o.RefreshInterval = time.Second
	}
	if o.OperationTimeout == 0 {
		o.OperationTimeout = 5 * time.Second
	}
	if o.Prefix[0] != '/' || o.FailureThreshold < 1 || o.EjectionDuration < 0 || o.ReportTimeout <= 0 || o.InitialLatency <= 0 || o.EWMAAlpha <= 0 || o.EWMAAlpha > 1 || o.CleanupInterval <= 0 || o.ServiceIdleTTL <= 0 || o.RefreshInterval <= 0 || o.OperationTimeout <= 0 {
		return Options{}, errors.New("registry/etcd: invalid options")
	}
	for len(o.Prefix) > 1 && o.Prefix[len(o.Prefix)-1] == '/' {
		o.Prefix = o.Prefix[:len(o.Prefix)-1]
	}
	return o, nil
}
