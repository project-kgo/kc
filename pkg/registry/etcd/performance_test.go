package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/project-kgo/kc/pkg/registry"
)

func BenchmarkResolutionLifecycle(b *testing.B) {
	options, err := (Options{ReportTimeout: time.Hour}).withDefaults()
	if err != nil {
		b.Fatal(err)
	}
	state := newServiceState("bench")
	instances := make([]registry.Instance, 32)
	for i := range instances {
		instances[i] = registry.Instance{Service: "bench", ID: string(rune('a' + i)), Endpoint: "http://127.0.0.1:8080"}
	}
	state.replace(instances, options.InitialLatency)
	next := 0
	randN := func(n int) int {
		next++
		return next % n
	}
	result := registry.Result{Outcome: registry.OutcomeSuccess, Latency: time.Millisecond}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		selected, _ := state.pick(time.Now(), randN, options.FailureThreshold, options.EjectionDuration)
		resolution := newResolution(state, selected, options)
		if err := resolution.Report(result); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRegistryResolveParallel(b *testing.B) {
	options, err := (Options{ReportTimeout: time.Hour}).withDefaults()
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := newServiceState("bench")
	state.ctx, state.cancel = context.WithCancel(ctx)
	state.replace([]registry.Instance{
		{Service: "bench", ID: "a", Endpoint: "http://127.0.0.1:8080"},
		{Service: "bench", ID: "b", Endpoint: "http://127.0.0.1:8081"},
	}, options.InitialLatency)
	state.readyOnce.Do(func() { close(state.ready) })
	r := &Registry{options: options, ctx: ctx, cancel: cancel, services: map[string]*serviceState{"bench": state}}
	result := registry.Result{Outcome: registry.OutcomeSuccess, Latency: time.Millisecond}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resolution, err := r.Resolve(context.Background(), "bench")
			if err != nil {
				b.Fatal(err)
			}
			if err := resolution.Report(result); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestResolutionLifecycleAllocationBudget(t *testing.T) {
	options, err := (Options{ReportTimeout: time.Hour}).withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	state := newServiceState("alloc")
	state.replace([]registry.Instance{
		{Service: "alloc", ID: "a", Endpoint: "http://127.0.0.1:8080"},
		{Service: "alloc", ID: "b", Endpoint: "http://127.0.0.1:8081"},
	}, options.InitialLatency)
	result := registry.Result{Outcome: registry.OutcomeSuccess, Latency: time.Millisecond}
	allocations := testing.AllocsPerRun(1000, func() {
		selected, _ := state.pick(time.Now(), func(int) int { return 0 }, options.FailureThreshold, options.EjectionDuration)
		resolution := newResolution(state, selected, options)
		if err := resolution.Report(result); err != nil {
			t.Fatal(err)
		}
	})
	if allocations > 1 {
		t.Fatalf("resolution lifecycle allocated %.1f objects; budget is 1", allocations)
	}
}

func TestEndpointFastPathDoesNotCloneMetadata(t *testing.T) {
	options, err := (Options{ReportTimeout: time.Hour}).withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	state := newServiceState("alloc")
	state.replace([]registry.Instance{{
		Service: "alloc", ID: "a", Endpoint: "http://127.0.0.1:8080",
		Metadata: map[string]string{"zone": "a", "version": "one"},
	}}, options.InitialLatency)
	result := registry.Result{Outcome: registry.OutcomeSuccess, Latency: time.Millisecond}
	allocations := testing.AllocsPerRun(1000, func() {
		selected, _ := state.pick(time.Now(), nil, options.FailureThreshold, options.EjectionDuration)
		resolution := newResolution(state, selected, options)
		if resolution.Endpoint() == "" {
			t.Fatal("empty endpoint")
		}
		if err := resolution.Report(result); err != nil {
			t.Fatal(err)
		}
	})
	if allocations > 1 {
		t.Fatalf("endpoint fast path allocated %.1f objects; budget is 1", allocations)
	}
}
