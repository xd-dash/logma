package pubsubruntime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/internal/pubsubmodel"
)

type concurrentRuntimeStore struct {
	mu          sync.RWMutex
	channels    map[string]pubsubmodel.Channel
	subscribers map[string]pubsubmodel.Subscriber
	callbacks   map[string]pubsubmodel.Callback
}

func (s *concurrentRuntimeStore) GetChannel(_ context.Context, name string) (pubsubmodel.Channel, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, ok := s.channels[name]
	if !ok {
		return pubsubmodel.Channel{}, pubsubmodel.ErrNotFound
	}
	return channel, nil
}

func (s *concurrentRuntimeStore) GetSubscriber(_ context.Context, id string) (pubsubmodel.Subscriber, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	subscriber, ok := s.subscribers[id]
	if !ok {
		return pubsubmodel.Subscriber{}, pubsubmodel.ErrNotFound
	}
	return subscriber, nil
}

func (s *concurrentRuntimeStore) GetCallback(_ context.Context, id string) (pubsubmodel.Callback, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	callback, ok := s.callbacks[id]
	if !ok {
		return pubsubmodel.Callback{}, pubsubmodel.ErrNotFound
	}
	return callback, nil
}

func (s *concurrentRuntimeStore) moveSubscriber(id, channel string) {
	s.mu.Lock()
	subscriber := s.subscribers[id]
	subscriber.Channel = channel
	s.subscribers[id] = subscriber
	s.mu.Unlock()
}

func TestConcurrentReconciliationsShareInProgressTargetListener(t *testing.T) {
	store := &concurrentRuntimeStore{
		channels: map[string]pubsubmodel.Channel{
			"old-a": {Name: "old-a"},
			"old-b": {Name: "old-b"},
			"new":   {Name: "new"},
		},
		subscribers: map[string]pubsubmodel.Subscriber{
			"sub-a": {ID: "sub-a", Channel: "old-a", CallbackIDs: []string{"hook-a"}},
			"sub-b": {ID: "sub-b", Channel: "old-b", CallbackIDs: []string{"hook-b"}},
		},
		callbacks: map[string]pubsubmodel.Callback{
			"hook-a": {ID: "hook-a", Type: pubsubmodel.CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://a.example/hook"}},
			"hook-b": {ID: "hook-b", Type: pubsubmodel.CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://b.example/hook"}},
		},
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	var subMu sync.Mutex
	var newSubscriber *fakeSubscriber
	runtime := newWithDependencies(client, store, func(_ context.Context, _ *redis.Client, channel string, _ func(string)) Subscriber {
		if channel != "new" {
			return newFakeSubscriber()
		}
		subMu.Lock()
		defer subMu.Unlock()
		if newSubscriber == nil {
			newSubscriber = &fakeSubscriber{ready: make(chan struct{}), stopped: make(chan struct{})}
		}
		return newSubscriber
	}, func(context.Context, string, string) error { return nil })
	controller, err := NewSubscriptionController(runtime)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		controller.Close()
		runtime.Close()
	})

	if err := controller.ActivateSubscription(context.Background(), "sub-a"); err != nil {
		t.Fatalf("initial sub-a activation: %v", err)
	}
	if err := controller.ActivateSubscription(context.Background(), "sub-b"); err != nil {
		t.Fatalf("initial sub-b activation: %v", err)
	}
	store.moveSubscriber("sub-a", "new")
	store.moveSubscriber("sub-b", "new")

	ctxA, cancelA := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancelA()
	resultA := make(chan error, 1)
	resultB := make(chan error, 1)
	go func() { resultA <- controller.ActivateSubscription(ctxA, "sub-a") }()
	go func() { resultB <- controller.ActivateSubscription(context.Background(), "sub-b") }()

	deadline := time.Now().Add(time.Second)
	for {
		runtime.mu.Lock()
		activation := runtime.active["new"]
		leases := 0
		if activation != nil {
			leases = activation.leases
		}
		runtime.mu.Unlock()
		if leases == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("target listener never reached two concurrent leases; got %d", leases)
		}
		time.Sleep(time.Millisecond)
	}

	if err := <-resultA; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("sub-a reconcile = %v, want deadline exceeded", err)
	}
	if !runtime.Active("new") {
		t.Fatal("one timed-out reconciliation canceled listener still leased by sub-b")
	}

	subMu.Lock()
	ready := newSubscriber.ready
	subMu.Unlock()
	close(ready)
	if err := <-resultB; err != nil {
		t.Fatalf("sub-b reconcile after peer timeout: %v", err)
	}
	if !runtime.Active("new") {
		t.Fatal("successful sub-b reconciliation did not retain target listener")
	}
	if !runtime.Active("old-a") {
		t.Fatal("failed sub-a reconciliation lost its previous known-good listener")
	}
}
