package etcd

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/project-kgo/kc/pkg/registry"
)

type instanceState struct {
	instance     registry.Instance
	ewma         time.Duration
	inflight     int64
	failures     int
	ejectedUntil time.Time
	halfOpen     bool
}

type serviceState struct {
	name        string
	mu          sync.Mutex
	instances   map[string]*instanceState
	ready       chan struct{}
	readyOnce   sync.Once
	loadErr     error
	refreshing  bool
	refreshDone chan struct{}
}

func newServiceState(name string) *serviceState {
	return &serviceState{name: name, instances: make(map[string]*instanceState), ready: make(chan struct{})}
}

func (s *serviceState) replace(instances []registry.Instance, initialLatency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := make(map[string]*instanceState, len(instances))
	for _, instance := range instances {
		if previous := s.instances[instance.ID]; previous != nil && previous.instance.Endpoint == instance.Endpoint {
			previous.instance = instance.Clone()
			next[instance.ID] = previous
		} else {
			next[instance.ID] = &instanceState{instance: instance.Clone(), ewma: initialLatency}
		}
	}
	s.instances = next
}

func (s *serviceState) allEjected(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.instances) == 0 {
		return false
	}
	for _, item := range s.instances {
		if item.ejectedUntil.IsZero() || (!now.Before(item.ejectedUntil) && !item.halfOpen) {
			return false
		}
	}
	return true
}

func (s *serviceState) pick(now time.Time, randN func(int) int, _ int, _ time.Duration) (*instanceState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.instances) == 0 {
		return nil, false
	}
	items := make([]*instanceState, 0, len(s.instances))
	all := make([]*instanceState, 0, len(s.instances))
	for _, item := range s.instances {
		all = append(all, item)
		if item.ejectedUntil.IsZero() || (!now.Before(item.ejectedUntil) && !item.halfOpen) {
			items = append(items, item)
		}
	}
	allEjected := len(items) == 0
	if allEjected {
		failOpen := all[:0]
		for _, item := range all {
			if !item.halfOpen {
				failOpen = append(failOpen, item)
			}
		}
		if len(failOpen) == 0 {
			return nil, true
		}
		sort.Slice(failOpen, func(i, j int) bool { return failOpen[i].ejectedUntil.Before(failOpen[j].ejectedUntil) })
		items = failOpen[:1]
	}
	chosen := items[0]
	if len(items) > 1 {
		first := randN(len(items))
		second := randN(len(items) - 1)
		if second >= first {
			second++
		}
		a, b := items[first], items[second]
		if score(b) < score(a) {
			chosen = b
		} else {
			chosen = a
		}
	}
	if !chosen.ejectedUntil.IsZero() && !now.Before(chosen.ejectedUntil) {
		chosen.halfOpen = true
	}
	chosen.inflight++
	return chosen, allEjected
}

func score(item *instanceState) float64 {
	return float64(item.ewma) * float64(item.inflight+1)
}

type resolution struct {
	state    *serviceState
	selected *instanceState
	instance registry.Instance
	options  Options
	mu       sync.Mutex
	status   uint8
	timer    *time.Timer
}

func newResolution(state *serviceState, selected *instanceState, options Options) *resolution {
	r := &resolution{state: state, selected: selected, instance: selected.instance.Clone(), options: options}
	r.timer = time.AfterFunc(options.ReportTimeout, r.expire)
	return r
}

func (r *resolution) Instance() registry.Instance { return r.instance.Clone() }

func (r *resolution) Report(result registry.Result) error {
	if err := result.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	if r.status == 1 {
		r.mu.Unlock()
		return registry.ErrAlreadyReported
	}
	if r.status == 2 {
		r.mu.Unlock()
		return registry.ErrReportExpired
	}
	r.status = 1
	r.timer.Stop()
	r.complete(&result)
	r.mu.Unlock()
	return nil
}

func (r *resolution) expire() {
	r.mu.Lock()
	if r.status != 0 {
		r.mu.Unlock()
		return
	}
	r.status = 2
	r.complete(nil)
	r.mu.Unlock()
}

func (r *resolution) complete(result *registry.Result) {
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	selected := r.selected
	if selected.inflight > 0 {
		selected.inflight--
	}
	current := r.state.instances[r.instance.ID]
	if current != selected || current.instance.Endpoint != r.instance.Endpoint {
		return
	}
	if result == nil {
		if selected.halfOpen {
			selected.halfOpen = false
			selected.ejectedUntil = time.Now().Add(r.options.EjectionDuration)
		}
		return
	}
	selected.ewma = time.Duration((1-r.options.EWMAAlpha)*float64(selected.ewma) + r.options.EWMAAlpha*float64(result.Latency))
	if result.Outcome == registry.OutcomeSuccess {
		selected.failures = 0
		selected.ejectedUntil = time.Time{}
		selected.halfOpen = false
		return
	}
	selected.failures++
	if selected.halfOpen || selected.failures >= r.options.FailureThreshold {
		selected.failures = 0
		selected.halfOpen = false
		selected.ejectedUntil = time.Now().Add(r.options.EjectionDuration)
	}
}

var errNoInstances = errors.New("no instances")
