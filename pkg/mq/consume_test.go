package mq

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"
	"testing"
)

type fakeSubscription struct {
	topic      string
	subscriber *fakeSubscriber
}

// Stop 会被 Subscriptions.Stop 并发调用。
func (s *fakeSubscription) Stop(context.Context) error {
	s.subscriber.mu.Lock()
	defer s.subscriber.mu.Unlock()
	s.subscriber.stopped = append(s.subscriber.stopped, s.topic)
	return s.subscriber.stopErr
}

func (s *fakeSubscription) Done() <-chan struct{} { return nil }

type fakeSubscriber struct {
	mu        sync.Mutex
	failTopic string
	stopErr   error
	stopped   []string
}

func (s *fakeSubscriber) Subscribe(context.Context, string, string, Handler, ...SubscribeOption) error {
	return nil
}

func (s *fakeSubscriber) Consume(_ context.Context, topic, _ string, _ Handler, _ ...SubscribeOption) (Subscription, error) {
	if topic == s.failTopic {
		return nil, ErrClosed
	}
	return &fakeSubscription{topic: topic, subscriber: s}, nil
}

func TestConsumeAllRollsBackOnFailure(t *testing.T) {
	stopErr := errors.New("stop failed")
	subscriber := &fakeSubscriber{failTopic: "c", stopErr: stopErr}
	subscriptions, err := ConsumeAll(context.Background(), subscriber,
		Consumption{Topic: "a", Group: "g"}, Consumption{Topic: "b", Group: "g"}, Consumption{Topic: "c", Group: "g"})
	if subscriptions != nil || !errors.Is(err, ErrClosed) || !errors.Is(err, stopErr) {
		t.Fatalf("ConsumeAll = %v, %v", subscriptions, err)
	}
	if !reflect.DeepEqual(sortedCopy(subscriber.stopped), []string{"a", "b"}) {
		t.Fatalf("stopped = %v", subscriber.stopped)
	}
}

func TestConsumeAllStopsEverySubscription(t *testing.T) {
	subscriber := &fakeSubscriber{}
	subscriptions, err := ConsumeAll(context.Background(), subscriber,
		Consumption{Topic: "a", Group: "g"}, Consumption{Topic: "b", Group: "g"})
	if err != nil || len(subscriptions) != 2 {
		t.Fatalf("ConsumeAll = %v, %v", subscriptions, err)
	}
	if err := subscriptions.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sortedCopy(subscriber.stopped), []string{"a", "b"}) {
		t.Fatalf("stopped = %v", subscriber.stopped)
	}
	if _, err := ConsumeAll(context.Background(), nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil subscriber err = %v", err)
	}
}

func sortedCopy(values []string) []string {
	out := slices.Clone(values)
	slices.Sort(out)
	return out
}
