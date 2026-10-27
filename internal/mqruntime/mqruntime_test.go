package mqruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	coremq "github.com/project-kgo/kc/pkg/mq"
)

// startFake 启动一个模拟订阅：拉取上下文结束后等待 work 返回再 Finish。
func startFake(t *testing.T, lifecycle *Lifecycle, parent context.Context, work func(context.Context)) *Subscription {
	t.Helper()
	subscription := lifecycle.NewSubscription(parent)
	if !lifecycle.Track(subscription) {
		t.Fatal("Track failed")
	}
	go func() {
		defer subscription.Finish()
		<-subscription.PollContext().Done()
		work(subscription.WorkContext())
	}()
	return subscription
}

func TestValidateSubscribe(t *testing.T) {
	handler := func(context.Context, *coremq.Message) error { return nil }
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	cases := []struct {
		name    string
		ctx     context.Context
		topic   string
		group   string
		handler coremq.Handler
		want    error
	}{
		{"nil ctx", nil, "t", "g", handler, coremq.ErrInvalidArgument},
		{"empty topic", context.Background(), " ", "g", handler, coremq.ErrInvalidArgument},
		{"empty group", context.Background(), "t", "", handler, coremq.ErrInvalidArgument},
		{"nil handler", context.Background(), "t", "g", nil, coremq.ErrInvalidArgument},
		{"canceled", canceled, "t", "g", handler, context.Canceled},
		{"ok", context.Background(), "t", "g", handler, nil},
	}
	for _, c := range cases {
		if err := ValidateSubscribe(c.ctx, c.topic, c.group, c.handler); !errors.Is(err, c.want) || (c.want == nil && err != nil) {
			t.Errorf("%s: err = %v, want %v", c.name, err, c.want)
		}
	}
}

func TestSubscriptionStopWaitsWithoutCancelingWork(t *testing.T) {
	lifecycle := NewLifecycle()
	release := make(chan struct{})
	workErr := make(chan error, 1)
	subscription := startFake(t, lifecycle, context.Background(), func(ctx context.Context) {
		<-release
		workErr <- ctx.Err()
	})

	stopped := make(chan error, 1)
	go func() { stopped <- subscription.Stop(context.Background()) }()
	select {
	case err := <-stopped:
		t.Fatalf("Stop returned early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatalf("Stop = %v", err)
	}
	if err := <-workErr; err != nil {
		t.Fatalf("work context canceled: %v", err)
	}
	if lifecycle.Active() != 0 {
		t.Fatal("subscription still tracked after Finish")
	}
}

func TestSubscriptionStopAbortsAfterDeadline(t *testing.T) {
	lifecycle := NewLifecycle()
	cause := make(chan error, 1)
	subscription := startFake(t, lifecycle, context.Background(), func(ctx context.Context) {
		<-ctx.Done()
		cause <- context.Cause(ctx)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := subscription.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v", err)
	}
	if err := <-cause; !errors.Is(err, coremq.ErrClosed) {
		t.Fatalf("cause = %v", err)
	}
	if err := subscription.Stop(nil); !errors.Is(err, coremq.ErrInvalidArgument) { //nolint:staticcheck // 校验 nil ctx
		t.Fatalf("Stop(nil) = %v", err)
	}
}

func TestSubscriptionParentCancelIsHardStop(t *testing.T) {
	lifecycle := NewLifecycle()
	parent, cancel := context.WithCancel(context.Background())
	workErr := make(chan error, 1)
	subscription := startFake(t, lifecycle, parent, func(ctx context.Context) { workErr <- ctx.Err() })
	cancel()
	if err := <-workErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("work err = %v", err)
	}
	<-subscription.Done()
}

func TestLifecycleCloseStopsSubscriptionsAndReleasesOnce(t *testing.T) {
	lifecycle := NewLifecycle()
	cause := make(chan error, 1)
	startFake(t, lifecycle, context.Background(), func(ctx context.Context) {
		<-ctx.Done()
		cause <- context.Cause(ctx)
	})
	if !lifecycle.Begin() {
		t.Fatal("Begin failed")
	}
	lifecycle.End()

	releaseErr := errors.New("release failed")
	releases := 0
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := lifecycle.Close(ctx, func() error { releases++; return releaseErr }); !errors.Is(err, releaseErr) {
		t.Fatalf("Close = %v", err)
	}
	if err := <-cause; !errors.Is(err, coremq.ErrClosed) {
		t.Fatalf("cause = %v", err)
	}
	if err := lifecycle.Close(context.Background(), func() error { releases++; return nil }); !errors.Is(err, releaseErr) {
		t.Fatalf("second Close = %v", err)
	}
	if releases != 1 {
		t.Fatalf("releases = %d", releases)
	}
	if lifecycle.Begin() || !lifecycle.Closed() || lifecycle.Track(lifecycle.NewSubscription(context.Background())) {
		t.Fatal("closed lifecycle accepted new work")
	}
}
