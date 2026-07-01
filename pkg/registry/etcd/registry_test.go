package etcd

import (
	"context"
	"errors"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/project-kgo/kc/pkg/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestRegisterResolveAndReport(t *testing.T) {
	client := startEtcd(t)
	r, err := New(client, Options{ReportTimeout: time.Second})
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
	two, err := r.Register(ctx, registry.Instance{Service: "users", ID: "two", Endpoint: "http://127.0.0.1:8082"}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	waitForCount(t, ctx, r.services["users"], 2)
	if err := one.Close(ctx); err != nil {
		t.Fatal(err)
	}
	waitForCount(t, ctx, r.services["users"], 1)
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

func TestResolutionFailureEjectsAndTimeoutExpires(t *testing.T) {
	options, err := (Options{FailureThreshold: 1, EjectionDuration: time.Second, ReportTimeout: 20 * time.Millisecond}).withDefaults()
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
	if selected.ejectedUntil.IsZero() {
		t.Fatal("failed instance was not ejected")
	}

	selected.ejectedUntil = time.Time{}
	selected2, _ := state.pick(time.Now(), func(int) int { return 0 }, options.FailureThreshold, options.EjectionDuration)
	expired := newResolution(state, selected2, options)
	time.Sleep(50 * time.Millisecond)
	if err := expired.Report(registry.Result{Outcome: registry.OutcomeSuccess, Latency: time.Millisecond}); !errors.Is(err, registry.ErrReportExpired) {
		t.Fatalf("expected ErrReportExpired, got %v", err)
	}
	if selected2.inflight != 0 {
		t.Fatalf("expired resolution leaked inflight: %d", selected2.inflight)
	}
}

func TestP2CPrefersLowerScore(t *testing.T) {
	state := newServiceState("users")
	state.replace([]registry.Instance{
		{Service: "users", ID: "fast", Endpoint: "http://127.0.0.1:1"},
		{Service: "users", ID: "slow", Endpoint: "http://127.0.0.1:2"},
	}, 10*time.Millisecond)
	state.instances["fast"].ewma = time.Millisecond
	state.instances["slow"].ewma = 100 * time.Millisecond
	chosen, _ := state.pick(time.Now(), func(int) int { return 0 }, 3, 30*time.Second)
	if chosen.instance.ID != "fast" {
		t.Fatalf("expected fast instance, got %s", chosen.instance.ID)
	}
}

func TestPickDoesNotReuseHalfOpenProbe(t *testing.T) {
	state := newServiceState("users")
	state.replace([]registry.Instance{{Service: "users", ID: "one", Endpoint: "http://127.0.0.1:1"}}, 10*time.Millisecond)
	state.instances["one"].ejectedUntil = time.Now().Add(-time.Second)
	state.instances["one"].halfOpen = true
	chosen, _ := state.pick(time.Now(), func(int) int { return 0 }, 3, time.Second)
	if chosen != nil {
		t.Fatal("an in-flight half-open probe must not be selected again")
	}
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
