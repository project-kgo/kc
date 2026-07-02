// Package etcd implements registry.Registry using etcd leases and watches.
package etcd

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/project-kgo/kc/pkg/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Registry implements registry.Registry with etcd leases and watches.
type Registry struct {
	client         *clientv3.Client
	options        Options
	ctx            context.Context
	cancel         context.CancelFunc
	closeOnce      sync.Once
	mu             sync.RWMutex
	closed         bool
	services       map[string]*serviceState
	registrations  map[*registration]struct{}
	wg             sync.WaitGroup
	fullReloads    atomic.Uint64
	watchEvents    atomic.Uint64
	decodeErrors   atomic.Uint64
	forcedRefresh  atomic.Uint64
	expiredReports atomic.Uint64
	watchRestarts  atomic.Uint64
	refreshErrors  atomic.Uint64
	serviceEvicts  atomic.Uint64
}

// New creates a Registry using an externally owned etcd client.
func New(client *clientv3.Client, options Options) (*Registry, error) {
	if client == nil {
		return nil, errors.New("registry/etcd: nil client")
	}
	resolved, err := options.withDefaults()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Registry{
		client: client, options: resolved, ctx: ctx, cancel: cancel,
		services: make(map[string]*serviceState), registrations: make(map[*registration]struct{}),
	}
	r.wg.Add(1)
	go r.janitor()
	return r, nil
}

// key builds the etcd key for one service instance.
func (r *Registry) key(instance registry.Instance) string {
	return path.Join(r.options.Prefix, instance.Service, instance.ID)
}

// servicePrefix returns the etcd prefix watched for a service.
func (r *Registry) servicePrefix(service string) string {
	return path.Join(r.options.Prefix, service) + "/"
}

// revoke revokes a lease with the configured internal operation timeout.
func (r *Registry) revoke(leaseID clientv3.LeaseID) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.options.OperationTimeout)
	defer cancel()
	_, err := r.client.Revoke(ctx, leaseID)
	return err
}

// Register grants a lease, stores the instance, and starts keepalive monitoring.
func (r *Registry) Register(ctx context.Context, instance registry.Instance, ttl time.Duration) (registry.Registration, error) {
	if err := instance.Validate(); err != nil {
		return nil, err
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("registry/etcd: ttl must be positive")
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, registry.ErrClosed
	}
	r.mu.Unlock()
	seconds := int64((ttl-1)/time.Second + 1)
	lease, err := r.client.Grant(ctx, seconds)
	if err != nil {
		return nil, err
	}
	revoke := func() { _ = r.revoke(lease.ID) }
	keepaliveCtx, keepaliveCancel := context.WithCancel(r.ctx)
	keepalive, err := r.client.KeepAlive(keepaliveCtx, lease.ID)
	if err != nil {
		keepaliveCancel()
		revoke()
		return nil, err
	}
	value, err := encodeInstance(instance)
	if err != nil {
		keepaliveCancel()
		revoke()
		return nil, err
	}
	if _, err = r.client.Put(ctx, r.key(instance), string(value), clientv3.WithLease(lease.ID)); err != nil {
		keepaliveCancel()
		revoke()
		return nil, err
	}
	reg := &registration{owner: r, leaseID: lease.ID, cancel: keepaliveCancel, done: make(chan struct{})}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), r.options.OperationTimeout)
		defer cancel()
		_ = reg.Close(ctx)
		return nil, registry.ErrClosed
	}
	r.registrations[reg] = struct{}{}
	r.mu.Unlock()
	go reg.monitor(keepalive)
	return reg, nil
}

// Resolve lazily starts discovery and selects an instance using health-aware P2C.
func (r *Registry) Resolve(ctx context.Context, service string) (registry.Resolution, error) {
	if strings.TrimSpace(service) == "" || service == "." || service == ".." || path.Base(service) != service || strings.Contains(service, `\`) {
		return nil, fmt.Errorf("registry/etcd: invalid service %q", service)
	}
	state, err := r.ensureService(service)
	if err != nil {
		return nil, err
	}
	defer state.active.Add(-1)
	select {
	case <-state.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.ctx.Done():
		return nil, registry.ErrClosed
	}
	state.mu.Lock()
	loadErr, count := state.loadErr, len(state.instances)
	state.mu.Unlock()
	if count == 0 {
		if loadErr != nil {
			return nil, loadErr
		}
		return nil, registry.ErrNoAvailableInstance
	}
	if state.allEjected(time.Now()) {
		if err := r.forceRefresh(ctx, state); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			// The last snapshot remains usable in fail-open mode.
		}
	}
	selected, _ := state.pick(time.Now(), nil, r.options.FailureThreshold, r.options.EjectionDuration)
	if selected == nil {
		return nil, registry.ErrNoAvailableInstance
	}
	return newResolution(state, selected, r.options), nil
}

// ensureService acquires an active reference and lazily creates the service watch.
func (r *Registry) ensureService(service string) (*serviceState, error) {
	r.mu.RLock()
	closed := r.closed
	state := r.services[service]
	r.mu.RUnlock()
	if closed {
		return nil, registry.ErrClosed
	}
	if state != nil {
		state.active.Add(1)
		return state, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, registry.ErrClosed
	}
	if state := r.services[service]; state != nil {
		state.active.Add(1)
		return state, nil
	}
	state = newServiceState(service)
	state.ctx, state.cancel = context.WithCancel(r.ctx)
	state.active.Store(1)
	r.services[service] = state
	r.wg.Add(1)
	go r.watch(state)
	return state, nil
}

// load performs a full linearizable snapshot load and applies its revision atomically.
func (r *Registry) load(ctx context.Context, state *serviceState) (int64, error) {
	r.fullReloads.Add(1)
	response, err := r.client.Get(ctx, r.servicePrefix(state.name), clientv3.WithPrefix())
	if err != nil {
		return 0, err
	}
	instances := make([]registry.Instance, 0, len(response.Kvs))
	for _, kv := range response.Kvs {
		instance, err := decodeInstance(string(kv.Key), kv.Value)
		if err != nil {
			r.decodeErrors.Add(1)
			continue
		}
		if instance.Service != state.name {
			r.decodeErrors.Add(1)
			continue
		}
		instances = append(instances, instance)
	}
	state.replaceAt(instances, r.options.InitialLatency, response.Header.Revision)
	state.mu.Lock()
	state.loadErr = nil
	state.mu.Unlock()
	state.readyOnce.Do(func() { close(state.ready) })
	return response.Header.Revision, nil
}

// watch maintains the service cache with revision-ordered incremental events.
func (r *Registry) watch(state *serviceState) {
	defer r.wg.Done()
	backoff := 100 * time.Millisecond
	for {
		revision, err := r.load(state.ctx, state)
		if err != nil {
			state.mu.Lock()
			state.loadErr = err
			state.mu.Unlock()
			state.readyOnce.Do(func() { close(state.ready) })
			if !r.waitBackoff(state.ctx, backoff) {
				return
			}
			backoff = nextWatchBackoff(backoff, false)
			continue
		}
		healthy := false
		watch := r.client.Watch(state.ctx, r.servicePrefix(state.name), clientv3.WithPrefix(), clientv3.WithRev(revision+1))
		for response := range watch {
			if response.Canceled {
				break
			}
			healthy = true
			revision := response.Header.Revision
			for _, event := range response.Events {
				r.watchEvents.Add(1)
				if event.Type == clientv3.EventTypeDelete {
					id := strings.TrimPrefix(string(event.Kv.Key), r.servicePrefix(state.name))
					if id != "" && !strings.Contains(id, "/") {
						state.removeAt(id, revision)
					}
					continue
				}
				instance, err := decodeInstance(string(event.Kv.Key), event.Kv.Value)
				if err != nil || instance.Service != state.name {
					r.decodeErrors.Add(1)
					continue
				}
				state.putAt(instance, r.options.InitialLatency, revision)
			}
			state.advanceRevision(revision)
			state.mu.Lock()
			state.loadErr = nil
			state.mu.Unlock()
		}
		if state.ctx.Err() != nil {
			return
		}
		r.watchRestarts.Add(1)
		delay := backoff
		if healthy {
			delay = 100 * time.Millisecond
		}
		if !r.waitBackoff(state.ctx, delay) {
			return
		}
		backoff = nextWatchBackoff(backoff, healthy)
	}
}

// nextWatchBackoff resets after healthy traffic or exponentially backs off failures.
func nextWatchBackoff(current time.Duration, healthy bool) time.Duration {
	if healthy {
		return 100 * time.Millisecond
	}
	return min(current*2, 5*time.Second)
}

// waitBackoff waits with jitter and returns false when the watch context is canceled.
func (r *Registry) waitBackoff(ctx context.Context, delay time.Duration) bool {
	jitter := time.Duration(uint64(time.Now().UnixNano()) % uint64(delay/2+1))
	timer := time.NewTimer(delay + jitter)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// forceRefresh coalesces and rate-limits full loads when every endpoint is ejected.
func (r *Registry) forceRefresh(ctx context.Context, state *serviceState) error {
	now := time.Now()
	state.mu.Lock()
	if state.refreshing {
		done := state.refreshDone
		state.mu.Unlock()
		select {
		case <-done:
			state.mu.Lock()
			err := state.refreshErr
			state.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if !state.lastRefresh.IsZero() && now.Sub(state.lastRefresh) < r.options.RefreshInterval {
		err := state.refreshErr
		state.mu.Unlock()
		return err
	}
	state.refreshing = true
	state.lastRefresh = now
	state.refreshDone = make(chan struct{})
	done := state.refreshDone
	state.mu.Unlock()
	r.forcedRefresh.Add(1)
	_, err := r.load(ctx, state)
	if err != nil {
		r.refreshErrors.Add(1)
	}
	state.mu.Lock()
	state.refreshing = false
	state.refreshErr = err
	close(done)
	state.mu.Unlock()
	return err
}

// Stats is a point-in-time operational snapshot. Counters are monotonic for
// the lifetime of a Registry; gauges reflect current in-memory state.
type Stats struct {
	Services           int
	Registrations      int
	PendingResolutions int
	FullReloads        uint64
	WatchEvents        uint64
	DecodeErrors       uint64
	ForcedRefreshes    uint64
	ExpiredReports     uint64
	WatchRestarts      uint64
	RefreshErrors      uint64
	ServiceEvictions   uint64
}

// Stats returns lock-safe counters and gauges suitable for metrics polling.
func (r *Registry) Stats() Stats {
	result := Stats{
		FullReloads: r.fullReloads.Load(), WatchEvents: r.watchEvents.Load(),
		DecodeErrors: r.decodeErrors.Load(), ForcedRefreshes: r.forcedRefresh.Load(),
		ExpiredReports: r.expiredReports.Load(), WatchRestarts: r.watchRestarts.Load(),
		RefreshErrors: r.refreshErrors.Load(), ServiceEvictions: r.serviceEvicts.Load(),
	}
	r.mu.RLock()
	result.Services = len(r.services)
	result.Registrations = len(r.registrations)
	for _, state := range r.services {
		state.pendingMu.Lock()
		for pending := state.pending; pending != nil; pending = pending.next {
			result.PendingResolutions++
		}
		state.pendingMu.Unlock()
	}
	r.mu.RUnlock()
	return result
}

// janitor expires missing reports and evicts idle service watches using one ticker.
func (r *Registry) janitor() {
	defer r.wg.Done()
	ticker := time.NewTicker(r.options.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			r.mu.Lock()
			for name, state := range r.services {
				r.expiredReports.Add(uint64(state.expire(now)))
				idle := state.canEvict(now, r.options.ServiceIdleTTL)
				if idle && r.services[name] == state {
					state.cancel()
					delete(r.services, name)
					r.serviceEvicts.Add(1)
				}
			}
			r.mu.Unlock()
		case <-r.ctx.Done():
			return
		}
	}
}

// Close revokes active registrations and stops every watcher and background worker.
func (r *Registry) Close() error {
	var errs []error
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		registrations := make([]*registration, 0, len(r.registrations))
		for item := range r.registrations {
			registrations = append(registrations, item)
		}
		r.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), r.options.OperationTimeout)
		defer cancel()
		for _, item := range registrations {
			if err := item.Close(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		r.cancel()
		r.wg.Wait()
	})
	return errors.Join(errs...)
}

// registration owns one lease keepalive lifecycle.
type registration struct {
	owner   *Registry
	leaseID clientv3.LeaseID
	cancel  context.CancelFunc
	done    chan struct{}
	once    sync.Once
	mu      sync.RWMutex
	err     error
}

// Done is closed when the registration reaches a terminal state.
func (r *registration) Done() <-chan struct{} { return r.done }

// Err returns the terminal keepalive error, if any.
func (r *registration) Err() error { r.mu.RLock(); defer r.mu.RUnlock(); return r.err }

// monitor consumes keepalive responses and records unexpected lease termination.
func (r *registration) monitor(channel <-chan *clientv3.LeaseKeepAliveResponse) {
	for response := range channel {
		if response == nil {
			r.finish(errors.New("registry/etcd: lease keepalive ended"), false, context.Background())
			return
		}
	}
	r.finish(errors.New("registry/etcd: lease keepalive ended"), false, context.Background())
}

// Close explicitly revokes the lease and leaves Err nil.
func (r *registration) Close(ctx context.Context) error { return r.finish(nil, true, ctx) }

// finish performs the registration's idempotent terminal transition.
func (r *registration) finish(cause error, revoke bool, ctx context.Context) error {
	var result error
	r.once.Do(func() {
		r.cancel()
		if revoke {
			_, result = r.owner.client.Revoke(ctx, r.leaseID)
		}
		r.mu.Lock()
		r.err = cause
		r.mu.Unlock()
		r.owner.mu.Lock()
		delete(r.owner.registrations, r)
		r.owner.mu.Unlock()
		close(r.done)
	})
	return result
}

var _ registry.Registry = (*Registry)(nil)
