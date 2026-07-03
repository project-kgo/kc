package etcd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/project-kgo/kc/pkg/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestRegisterResolveAndReport(t *testing.T) {
	client := startEtcd(t)
	r, err := New(client, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	instance := registry.Instance{Service: "users", ID: "one", Endpoint: "http://127.0.0.1:8080", Metadata: map[string]string{"zone": "a"}}
	registration, err := r.Register(ctx, instance, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	resolution, err := r.Resolve(ctx, "users")
	if err != nil {
		t.Fatal(err)
	}
	if got := resolution.Instance(); got.Endpoint != instance.Endpoint || got.Metadata["zone"] != "a" {
		t.Fatalf("unexpected instance: %+v", got)
	}
	if err := resolution.Report(registry.Result{Outcome: registry.OutcomeSuccess, Latency: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if err := resolution.Report(registry.Result{Outcome: registry.OutcomeSuccess, Latency: time.Millisecond}); !errors.Is(err, registry.ErrAlreadyReported) {
		t.Fatalf("expected ErrAlreadyReported, got %v", err)
	}
	if err := registration.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := registration.Err(); err != nil {
		t.Fatalf("active close reported failure: %v", err)
	}
	select {
	case <-registration.Done():
	case <-ctx.Done():
		t.Fatal("registration did not close")
	}
}

func TestResolveMissingService(t *testing.T) {
	client := startEtcd(t)
	r, err := New(client, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := r.Resolve(ctx, "missing"); !errors.Is(err, registry.ErrNoAvailableInstance) {
		t.Fatalf("expected ErrNoAvailableInstance, got %v", err)
	}
}

func TestWatchTracksAddedAndRemovedInstances(t *testing.T) {
	client := startEtcd(t)
	r, err := New(client, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	one, err := r.Register(ctx, registry.Instance{Service: "users", ID: "one", Endpoint: "http://127.0.0.1:8081"}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if resolution, err := r.Resolve(ctx, "users"); err != nil {
		t.Fatal(err)
	} else if err := resolution.Report(registry.Result{Outcome: registry.OutcomeSuccess, Latency: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	initialReloads := r.Stats().FullReloads
	two, err := r.Register(ctx, registry.Instance{Service: "users", ID: "two", Endpoint: "http://127.0.0.1:8082"}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	waitForCount(t, ctx, r.services["users"], 2)
	if got := r.Stats().FullReloads; got != initialReloads {
		t.Fatalf("watch PUT caused full reload: before=%d after=%d", initialReloads, got)
	}
	if err := one.Close(ctx); err != nil {
		t.Fatal(err)
	}
	waitForCount(t, ctx, r.services["users"], 1)
	if got := r.Stats().FullReloads; got != initialReloads {
		t.Fatalf("watch DELETE caused full reload: before=%d after=%d", initialReloads, got)
	}
	resolution, err := r.Resolve(ctx, "users")
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Instance().ID != "two" {
		t.Fatalf("expected remaining instance two, got %s", resolution.Instance().ID)
	}
	_ = resolution.Report(registry.Result{Outcome: registry.OutcomeSuccess, Latency: time.Millisecond})
	_ = two.Close(ctx)
}

func TestIdleServiceStateIsEvicted(t *testing.T) {
	client := startEtcd(t)
	r, err := New(client, Options{ServiceIdleTTL: 20 * time.Millisecond, ServiceSweepInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = r.Resolve(ctx, "unused")
	waitFor(t, ctx, func() bool { return r.Stats().Services == 0 })
}

func TestMalformedWatchValueIsIsolated(t *testing.T) {
	client := startEtcd(t)
	r, err := New(client, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	registration, err := r.Register(ctx, registry.Instance{Service: "users", ID: "good", Endpoint: "http://127.0.0.1:8080"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer registration.Close(ctx)
	resolution, err := r.Resolve(ctx, "users")
	if err != nil {
		t.Fatal(err)
	}
	_ = resolution.Report(registry.Result{Outcome: registry.OutcomeSuccess, Latency: time.Millisecond})
	reloads := r.Stats().FullReloads
	if _, err := client.Put(ctx, r.servicePrefix("users")+"poison", "not-json"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, func() bool { return r.Stats().DecodeErrors > 0 })
	if got := r.Stats().FullReloads; got != reloads {
		t.Fatalf("malformed watch value caused full reload: before=%d after=%d", reloads, got)
	}
	resolution, err = r.Resolve(ctx, "users")
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Instance().ID != "good" {
		t.Fatal("malformed value replaced valid snapshot")
	}
	_ = resolution.Report(registry.Result{Outcome: registry.OutcomeSuccess, Latency: time.Millisecond})
}

func TestAllEjectedRefreshIsRateLimited(t *testing.T) {
	client := startEtcd(t)
	r, err := New(client, Options{FailureThreshold: 1, EjectionDuration: time.Hour, RefreshInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	registration, err := r.Register(ctx, registry.Instance{Service: "users", ID: "one", Endpoint: "http://127.0.0.1:8080"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer registration.Close(ctx)
	resolution, err := r.Resolve(ctx, "users")
	if err != nil {
		t.Fatal(err)
	}
	if err := resolution.Report(registry.Result{Outcome: registry.OutcomeFailure, Latency: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	before := r.Stats().ForcedRefreshes
	for range 20 {
		resolution, err = r.Resolve(ctx, "users")
		if err != nil {
			t.Fatal(err)
		}
		if err := resolution.Report(registry.Result{Outcome: registry.OutcomeFailure, Latency: time.Millisecond}); err != nil {
			t.Fatal(err)
		}
	}
	if delta := r.Stats().ForcedRefreshes - before; delta != 1 {
		t.Fatalf("expected one forced refresh inside cooldown, got %d", delta)
	}
}

func TestResolutionFailureEjects(t *testing.T) {
	options, err := (Options{FailureThreshold: 1, EjectionDuration: time.Second}).withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	state := newServiceState("users")
	state.replace([]registry.Instance{{Service: "users", ID: "one", Endpoint: "http://127.0.0.1:1"}}, options.InitialLatency)
	selected, _ := state.pick(time.Now(), func(int) int { return 0 }, options.FailureThreshold, options.EjectionDuration)
	resolution := newResolution(state, selected, options)
	if err := resolution.Report(registry.Result{Outcome: registry.OutcomeFailure, Latency: 5 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if selected.ejectedUntil.Load() == 0 {
		t.Fatal("failed instance was not ejected")
	}
}

func TestP2CPrefersLowerScore(t *testing.T) {
	state := newServiceState("users")
	state.replace([]registry.Instance{
		{Service: "users", ID: "fast", Endpoint: "http://127.0.0.1:1"},
		{Service: "users", ID: "slow", Endpoint: "http://127.0.0.1:2"},
	}, 10*time.Millisecond)
	state.instances["fast"].ewma.Store(int64(time.Millisecond))
	state.instances["slow"].ewma.Store(int64(100 * time.Millisecond))
	chosen, _ := state.pick(time.Now(), func(int) int { return 0 }, 3, 30*time.Second)
	if chosen.loadInstance().ID != "fast" {
		t.Fatalf("expected fast instance, got %s", chosen.loadInstance().ID)
	}
}

func TestPickDoesNotReuseHalfOpenProbe(t *testing.T) {
	state := newServiceState("users")
	state.replace([]registry.Instance{{Service: "users", ID: "one", Endpoint: "http://127.0.0.1:1"}}, 10*time.Millisecond)
	state.instances["one"].ejectedUntil.Store(time.Now().Add(-time.Second).UnixNano())
	state.instances["one"].halfOpen.Store(true)
	chosen, _ := state.pick(time.Now(), func(int) int { return 0 }, 3, time.Second)
	if chosen != nil {
		t.Fatal("an in-flight half-open probe must not be selected again")
	}
}

func TestMetadataUpdatePreservesInflightAccounting(t *testing.T) {
	options, err := (Options{}).withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	state := newServiceState("users")
	instance := registry.Instance{Service: "users", ID: "one", Endpoint: "http://127.0.0.1:8080", Metadata: map[string]string{"version": "one"}}
	state.replace([]registry.Instance{instance}, options.InitialLatency)
	selected, _ := state.pick(time.Now(), nil, options.FailureThreshold, options.EjectionDuration)
	resolution := newResolution(state, selected, options)
	instance.Metadata = map[string]string{"version": "two"}
	state.put(instance, options.InitialLatency)
	if err := resolution.Report(registry.Result{Outcome: registry.OutcomeSuccess, Latency: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	current := state.instances["one"]
	if got := current.inflight.Load(); got != 0 {
		t.Fatalf("metadata update leaked inflight count: %d", got)
	}
}

func TestServiceWithActiveResolveCannotBeEvicted(t *testing.T) {
	state := newServiceState("users")
	state.lastUsed.Store(time.Now().Add(-time.Hour).UnixNano())
	state.active.Store(1)
	if state.canEvict(time.Now(), time.Minute) {
		t.Fatal("service with an active Resolve was considered evictable")
	}
	state.active.Store(0)
	if !state.canEvict(time.Now(), time.Minute) {
		t.Fatal("idle service without pending work should be evictable")
	}
}

func TestStaleSnapshotCannotOverwriteNewerWatchRevision(t *testing.T) {
	state := newServiceState("users")
	newer := registry.Instance{Service: "users", ID: "one", Endpoint: "http://127.0.0.1:8082"}
	older := registry.Instance{Service: "users", ID: "one", Endpoint: "http://127.0.0.1:8081"}
	state.putAt(newer, time.Millisecond, 20)
	state.replaceAt([]registry.Instance{older}, time.Millisecond, 19)
	got := state.instances["one"].loadInstance()
	if got.Endpoint != newer.Endpoint {
		t.Fatalf("stale snapshot overwrote revision 20: %s", got.Endpoint)
	}
}

func TestSkippedWatchEventStillAdvancesRevision(t *testing.T) {
	state := newServiceState("users")
	current := registry.Instance{Service: "users", ID: "one", Endpoint: "http://127.0.0.1:8082"}
	stale := registry.Instance{Service: "users", ID: "one", Endpoint: "http://127.0.0.1:8081"}
	state.replaceAt([]registry.Instance{current}, time.Millisecond, 10)
	state.advanceRevision(20) // revision 20 contained an invalid value and was skipped
	state.replaceAt([]registry.Instance{stale}, time.Millisecond, 19)
	if got := state.instances["one"].loadInstance().Endpoint; got != current.Endpoint {
		t.Fatalf("snapshot older than skipped watch revision was applied: %s", got)
	}
}

func TestWatchBackoffOnlyResetsAfterHealthyResponse(t *testing.T) {
	if got := nextWatchBackoff(400*time.Millisecond, false); got != 800*time.Millisecond {
		t.Fatalf("unhealthy watch did not back off: %v", got)
	}
	if got := nextWatchBackoff(4*time.Second, false); got != 5*time.Second {
		t.Fatalf("watch backoff exceeded cap: %v", got)
	}
	if got := nextWatchBackoff(4*time.Second, true); got != 100*time.Millisecond {
		t.Fatalf("healthy watch did not reset backoff: %v", got)
	}
}

func TestConcurrentResolveReportAndMetadataUpdates(t *testing.T) {
	options, err := (Options{}).withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	state := newServiceState("users")
	instances := make([]registry.Instance, 8)
	for i := range instances {
		instances[i] = registry.Instance{Service: "users", ID: fmt.Sprintf("node-%d", i), Endpoint: fmt.Sprintf("http://127.0.0.1:%d", 8000+i)}
	}
	state.replace(instances, options.InitialLatency)

	const workers, iterations = 32, 500
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				selected, _ := state.pick(time.Now(), nil, options.FailureThreshold, options.EjectionDuration)
				if selected == nil {
					errs <- errors.New("no selected instance")
					return
				}
				resolution := newResolution(state, selected, options)
				if err := resolution.Report(registry.Result{Outcome: registry.OutcomeSuccess, Latency: time.Millisecond}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	for i := range iterations {
		instance := instances[i%len(instances)]
		instance.Metadata = map[string]string{"revision": fmt.Sprint(i)}
		state.put(instance, options.InitialLatency)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	state.mu.RLock()
	for _, item := range state.items {
		if got := item.inflight.Load(); got != 0 {
			t.Fatalf("instance %s retained inflight=%d", item.loadInstance().ID, got)
		}
	}
	state.mu.RUnlock()
}

func startEtcd(t *testing.T) *clientv3.Client {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.Logger = "zap"
	cfg.LogOutputs = []string{"/dev/null"}
	cfg.ListenClientUrls = []url.URL{freeURL(t)}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls
	cfg.ListenPeerUrls = []url.URL{freeURL(t)}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	server, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	select {
	case <-server.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		server.Server.Stop()
		t.Fatal("embedded etcd did not start")
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{cfg.AdvertiseClientUrls[0].String()}, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func freeURL(t *testing.T) url.URL {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return url.URL{Scheme: "http", Host: address}
}

func waitForCount(t *testing.T, ctx context.Context, state *serviceState, count int) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		state.mu.Lock()
		got := len(state.instances)
		state.mu.Unlock()
		if got == count {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("wanted %d instances, got %d", count, got)
		}
	}
}

func waitFor(t *testing.T, ctx context.Context, condition func() bool) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("condition was not met before timeout")
		}
	}
}
