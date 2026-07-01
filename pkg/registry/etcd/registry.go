// Package etcd implements registry.Registry using etcd leases and watches.
package etcd

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/project-kgo/kc/pkg/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Registry implements registry.Registry with etcd leases and watches.
type Registry struct {
	client        *clientv3.Client
	options       Options
	ctx           context.Context
	cancel        context.CancelFunc
	closeOnce     sync.Once
	mu            sync.Mutex
	closed        bool
	services      map[string]*serviceState
	registrations map[*registration]struct{}
	wg            sync.WaitGroup
	randMu        sync.Mutex
	rand          *rand.Rand
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
	return &Registry{
		client: client, options: resolved, ctx: ctx, cancel: cancel,
		services: make(map[string]*serviceState), registrations: make(map[*registration]struct{}),
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

func (r *Registry) key(instance registry.Instance) string {
	return path.Join(r.options.Prefix, instance.Service, instance.ID)
}

func (r *Registry) servicePrefix(service string) string {
	return path.Join(r.options.Prefix, service) + "/"
}

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
	revoke := func() { _, _ = r.client.Revoke(context.Background(), lease.ID) }
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
		_ = reg.Close(context.Background())
		return nil, registry.ErrClosed
	}
	r.registrations[reg] = struct{}{}
	r.mu.Unlock()
	go reg.monitor(keepalive)
	return reg, nil
}

func (r *Registry) Resolve(ctx context.Context, service string) (registry.Resolution, error) {
	if strings.TrimSpace(service) == "" || service == "." || service == ".." || path.Base(service) != service || strings.Contains(service, `\`) {
		return nil, fmt.Errorf("registry/etcd: invalid service %q", service)
	}
	state, err := r.ensureService(service)
	if err != nil {
		return nil, err
	}
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
	r.randMu.Lock()
	selected, _ := state.pick(time.Now(), r.rand.Intn, r.options.FailureThreshold, r.options.EjectionDuration)
	r.randMu.Unlock()
	if selected == nil {
		return nil, registry.ErrNoAvailableInstance
	}
	return newResolution(state, selected, r.options), nil
}

func (r *Registry) ensureService(service string) (*serviceState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, registry.ErrClosed
	}
	if state := r.services[service]; state != nil {
		return state, nil
	}
	state := newServiceState(service)
	r.services[service] = state
	r.wg.Add(1)
	go r.watch(state)
	return state, nil
}

func (r *Registry) load(ctx context.Context, state *serviceState) (int64, error) {
	response, err := r.client.Get(ctx, r.servicePrefix(state.name), clientv3.WithPrefix())
	if err != nil {
		return 0, err
	}
	instances := make([]registry.Instance, 0, len(response.Kvs))
	for _, kv := range response.Kvs {
		instance, err := decodeInstance(string(kv.Key), kv.Value)
		if err != nil {
			return 0, err
		}
		if instance.Service != state.name {
			return 0, fmt.Errorf("registry/etcd: key contains service %q, value contains %q", state.name, instance.Service)
		}
		instances = append(instances, instance)
	}
	sort.Slice(instances, func(i, j int) bool { return instances[i].ID < instances[j].ID })
	state.replace(instances, r.options.InitialLatency)
	state.mu.Lock()
	state.loadErr = nil
	state.mu.Unlock()
	state.readyOnce.Do(func() { close(state.ready) })
	return response.Header.Revision, nil
}

func (r *Registry) watch(state *serviceState) {
	defer r.wg.Done()
	backoff := 100 * time.Millisecond
	for {
		revision, err := r.load(r.ctx, state)
		if err != nil {
			state.mu.Lock()
			state.loadErr = err
			state.mu.Unlock()
			state.readyOnce.Do(func() { close(state.ready) })
			if !r.waitBackoff(backoff) {
				return
			}
			backoff = min(backoff*2, 5*time.Second)
			continue
		}
		backoff = 100 * time.Millisecond
		watch := r.client.Watch(r.ctx, r.servicePrefix(state.name), clientv3.WithPrefix(), clientv3.WithRev(revision+1))
		for response := range watch {
			if response.Canceled {
				break
			}
			if _, err := r.load(r.ctx, state); err != nil {
				state.mu.Lock()
				state.loadErr = err
				state.mu.Unlock()
				break
			}
		}
		if r.ctx.Err() != nil {
			return
		}
		if !r.waitBackoff(backoff) {
			return
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

func (r *Registry) waitBackoff(delay time.Duration) bool {
	r.randMu.Lock()
	jitter := time.Duration(r.rand.Int63n(int64(delay/2 + 1)))
	r.randMu.Unlock()
	timer := time.NewTimer(delay + jitter)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-r.ctx.Done():
		return false
	}
}

func (r *Registry) forceRefresh(ctx context.Context, state *serviceState) error {
	state.mu.Lock()
	if state.refreshing {
		done := state.refreshDone
		state.mu.Unlock()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	state.refreshing = true
	state.refreshDone = make(chan struct{})
	done := state.refreshDone
	state.mu.Unlock()
	_, err := r.load(ctx, state)
	state.mu.Lock()
	state.refreshing = false
	close(done)
	state.mu.Unlock()
	return err
}

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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

type registration struct {
	owner   *Registry
	leaseID clientv3.LeaseID
	cancel  context.CancelFunc
	done    chan struct{}
	once    sync.Once
	mu      sync.RWMutex
	err     error
}

func (r *registration) Done() <-chan struct{} { return r.done }
func (r *registration) Err() error            { r.mu.RLock(); defer r.mu.RUnlock(); return r.err }

func (r *registration) monitor(channel <-chan *clientv3.LeaseKeepAliveResponse) {
	for response := range channel {
		if response == nil {
			r.finish(errors.New("registry/etcd: lease keepalive ended"), false, context.Background())
			return
		}
	}
	r.finish(errors.New("registry/etcd: lease keepalive ended"), false, context.Background())
}

func (r *registration) Close(ctx context.Context) error { return r.finish(nil, true, ctx) }

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
