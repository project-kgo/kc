package etcd

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/project-kgo/kc/pkg/registry"
)

// instanceState stores one immutable descriptor and atomically updated health data.
type instanceState struct {
	instance     atomic.Pointer[registry.Instance]
	index        int
	ewma         atomic.Int64
	inflight     atomic.Int64
	failures     atomic.Int32
	ejectedUntil atomic.Int64
	halfOpen     atomic.Bool
}

// newInstanceState initializes health data for a newly discovered endpoint.
func newInstanceState(instance registry.Instance, initialLatency time.Duration) *instanceState {
	item := &instanceState{}
	item.storeInstance(instance)
	item.ewma.Store(int64(initialLatency))
	return item
}

// storeInstance atomically replaces the descriptor with a defensive copy.
func (s *instanceState) storeInstance(instance registry.Instance) {
	clone := instance.Clone()
	s.instance.Store(&clone)
}

// loadInstance returns the current immutable descriptor without cloning metadata.
func (s *instanceState) loadInstance() registry.Instance {
	return *s.instance.Load()
}

// serviceState owns one service's topology, health data, and pending reports.
type serviceState struct {
	name        string
	mu          sync.RWMutex // protects topology, load, and refresh state
	instances   map[string]*instanceState
	items       []*instanceState
	revision    int64
	ready       chan struct{}
	readyOnce   sync.Once
	loadErr     error
	refreshing  bool
	refreshDone chan struct{}
	refreshErr  error
	lastRefresh time.Time
	lastUsed    atomic.Int64
	active      atomic.Int64
	rng         atomic.Uint64
	ctx         context.Context
	cancel      context.CancelFunc
}

// newServiceState creates an empty service cache with a per-service random seed.
func newServiceState(name string) *serviceState {
	seed := uint64(time.Now().UnixNano()) ^ uint64(len(name))*0x9e3779b97f4a7c15
	if seed == 0 {
		seed = 1
	}
	state := &serviceState{name: name, instances: make(map[string]*instanceState), ready: make(chan struct{})}
	state.lastUsed.Store(time.Now().UnixNano())
	state.rng.Store(seed)
	return state
}

// replace unconditionally replaces topology; it is used by isolated unit paths.
func (s *serviceState) replace(instances []registry.Instance, initialLatency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replaceLocked(instances, initialLatency)
}

// replaceAt applies a full snapshot only when its etcd revision is not stale.
func (s *serviceState) replaceAt(instances []registry.Instance, initialLatency time.Duration, revision int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if revision < s.revision {
		return
	}
	s.replaceLocked(instances, initialLatency)
	s.revision = revision
}

// replaceLocked rebuilds the indexed topology while preserving compatible health state.
func (s *serviceState) replaceLocked(instances []registry.Instance, initialLatency time.Duration) {
	nextMap := make(map[string]*instanceState, len(instances))
	nextItems := make([]*instanceState, 0, len(instances))
	for _, instance := range instances {
		item := s.instances[instance.ID]
		if item == nil || item.loadInstance().Endpoint != instance.Endpoint {
			item = newInstanceState(instance, initialLatency)
		} else {
			item.storeInstance(instance)
		}
		item.index = len(nextItems)
		nextMap[instance.ID] = item
		nextItems = append(nextItems, item)
	}
	s.instances, s.items = nextMap, nextItems
}

// put unconditionally inserts or updates one instance.
func (s *serviceState) put(instance registry.Instance, initialLatency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.putLocked(instance, initialLatency)
}

// putAt applies a watch PUT only when its revision is current.
func (s *serviceState) putAt(instance registry.Instance, initialLatency time.Duration, revision int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if revision < s.revision {
		return
	}
	s.putLocked(instance, initialLatency)
	s.revision = revision
}

// putLocked updates the map and dense P2C candidate slice under the topology lock.
func (s *serviceState) putLocked(instance registry.Instance, initialLatency time.Duration) {
	if previous := s.instances[instance.ID]; previous != nil {
		item := previous
		if previous.loadInstance().Endpoint != instance.Endpoint {
			item = newInstanceState(instance, initialLatency)
		} else {
			item.storeInstance(instance)
		}
		item.index = previous.index
		s.instances[instance.ID] = item
		s.items[item.index] = item
		return
	}
	item := newInstanceState(instance, initialLatency)
	item.index = len(s.items)
	s.instances[instance.ID] = item
	s.items = append(s.items, item)
}

// remove unconditionally removes an instance by ID.
func (s *serviceState) remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeLocked(id)
}

// removeAt applies a watch DELETE only when its revision is current.
func (s *serviceState) removeAt(id string, revision int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if revision < s.revision {
		return
	}
	s.removeLocked(id)
	s.revision = revision
}

// removeLocked removes an instance in O(1) by swapping the dense-slice tail.
func (s *serviceState) removeLocked(id string) {
	item := s.instances[id]
	if item == nil {
		return
	}
	last := len(s.items) - 1
	if item.index != last {
		s.items[item.index] = s.items[last]
		s.items[item.index].index = item.index
	}
	s.items[last] = nil
	s.items = s.items[:last]
	delete(s.instances, id)
}

// advanceRevision records skipped or empty watch responses as consistency barriers.
func (s *serviceState) advanceRevision(revision int64) {
	s.mu.Lock()
	if revision > s.revision {
		s.revision = revision
	}
	s.mu.Unlock()
}

// allEjected reports whether no normal or half-open candidate is currently usable.
func (s *serviceState) allEjected(now time.Time) bool {
	nowNanos := now.UnixNano()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.items) == 0 {
		return false
	}
	for _, item := range s.items {
		until := item.ejectedUntil.Load()
		if until == 0 || (nowNanos >= until && !item.halfOpen.Load()) {
			return false
		}
	}
	return true
}

// canEvict checks inactivity and active Resolve calls.
func (s *serviceState) canEvict(now time.Time, idleTTL time.Duration) bool {
	return s.active.Load() == 0 && now.Sub(time.Unix(0, s.lastUsed.Load())) >= idleTTL
}

// randomN returns a lock-free per-service pseudo-random index.
func (s *serviceState) randomN(n int) int {
	x := s.rng.Add(0x9e3779b97f4a7c15)
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x ^= x >> 31
	return int(x % uint64(n))
}

// pick is allocation-free and concurrent across callers. randN is only used
// by deterministic unit tests; production passes nil.
func (s *serviceState) pick(now time.Time, randN func(int) int, _ int, _ time.Duration) (*instanceState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := len(s.items)
	if count == 0 {
		return nil, false
	}
	nowNanos := now.UnixNano()
	random := s.randomN
	if randN != nil {
		random = randN
	}
	eligible := func(item *instanceState) bool {
		until := item.ejectedUntil.Load()
		return until == 0 || (nowNanos >= until && !item.halfOpen.Load())
	}

	var first, second *instanceState
	for attempts := 0; attempts < 8 && (first == nil || (second == nil && count > 1)); attempts++ {
		item := s.items[random(count)]
		if !eligible(item) {
			continue
		}
		if first == nil {
			first = item
		} else if item != first {
			second = item
		}
	}
	if first == nil || (second == nil && count > 1) {
		for _, item := range s.items {
			if !eligible(item) {
				continue
			}
			if first == nil {
				first = item
			} else if item != first {
				second = item
				break
			}
		}
	}
	allEjected := first == nil
	if allEjected {
		for _, item := range s.items {
			if item.halfOpen.Load() {
				continue
			}
			if first == nil || item.ejectedUntil.Load() < first.ejectedUntil.Load() {
				first = item
			}
		}
		if first == nil {
			return nil, true
		}
	}
	chosen := first
	if second != nil && score(second) < score(first) {
		chosen = second
	}
	until := chosen.ejectedUntil.Load()
	if until != 0 && nowNanos >= until && !chosen.halfOpen.CompareAndSwap(false, true) {
		if second == nil || !reserve(second, nowNanos) {
			return nil, allEjected
		}
		chosen = second
	} else {
		chosen.inflight.Add(1)
	}
	s.lastUsed.Store(nowNanos)
	return chosen, allEjected
}

// reserve atomically claims a candidate and increments its in-flight request count.
func reserve(item *instanceState, nowNanos int64) bool {
	until := item.ejectedUntil.Load()
	if until != 0 && nowNanos >= until && !item.halfOpen.CompareAndSwap(false, true) {
		return false
	}
	item.inflight.Add(1)
	return true
}

// score computes the P2C load score from latency EWMA and current concurrency.
func score(item *instanceState) float64 {
	return float64(item.ewma.Load()) * float64(item.inflight.Load()+1)
}

// resolution binds one selected instance to a single result report.
type resolution struct {
	state            *serviceState
	selected         *instanceState
	instance         registry.Instance
	ejectionDuration time.Duration
	failureThreshold int32
	alpha            float64
	status           atomic.Uint32
}

// newResolution binds the selected instance to caller-owned result reporting.
func newResolution(state *serviceState, selected *instanceState, options Options) *resolution {
	return &resolution{
		state: state, selected: selected, instance: selected.loadInstance(),
		ejectionDuration: options.EjectionDuration, failureThreshold: int32(options.FailureThreshold),
		alpha: options.EWMAAlpha,
	}
}

// Instance returns a defensive copy of the selected instance.
func (r *resolution) Instance() registry.Instance { return r.instance.Clone() }

// Endpoint returns the selected h2c endpoint without cloning metadata.
func (r *resolution) Endpoint() string { return r.instance.Endpoint }

// Report applies latency and health feedback exactly once.
func (r *resolution) Report(result registry.Result) error {
	if err := result.Validate(); err != nil {
		return err
	}
	if !r.status.CompareAndSwap(0, 1) {
		return registry.ErrAlreadyReported
	}
	r.complete(&result, time.Now())
	return nil
}

// complete updates in-flight, EWMA, failure, ejection, and half-open state.
func (r *resolution) complete(result *registry.Result, now time.Time) {
	selected := r.selected
	for {
		current := selected.inflight.Load()
		if current <= 0 || selected.inflight.CompareAndSwap(current, current-1) {
			break
		}
	}
	r.state.mu.RLock()
	current := r.state.instances[r.instance.ID]
	valid := current == selected && current.loadInstance().Endpoint == r.instance.Endpoint
	r.state.mu.RUnlock()
	if !valid {
		return
	}
	if result == nil {
		if selected.halfOpen.CompareAndSwap(true, false) {
			selected.ejectedUntil.Store(now.Add(r.ejectionDuration).UnixNano())
		}
		return
	}
	updateEWMA(selected, result.Latency, r.alpha)
	if result.Outcome == registry.OutcomeSuccess {
		selected.failures.Store(0)
		selected.ejectedUntil.Store(0)
		selected.halfOpen.Store(false)
		return
	}
	failures := selected.failures.Add(1)
	if selected.halfOpen.Load() || failures >= r.failureThreshold {
		selected.failures.Store(0)
		selected.halfOpen.Store(false)
		selected.ejectedUntil.Store(now.Add(r.ejectionDuration).UnixNano())
	}
}

// updateEWMA atomically incorporates a latency sample.
func updateEWMA(item *instanceState, latency time.Duration, alpha float64) {
	for {
		old := item.ewma.Load()
		next := int64((1-alpha)*float64(old) + alpha*float64(latency))
		if item.ewma.CompareAndSwap(old, next) {
			return
		}
	}
}
