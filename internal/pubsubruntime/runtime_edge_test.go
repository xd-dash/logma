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

type generationSubscriber struct {
	mu      sync.Mutex
	ready   chan struct{}
	stopped chan struct{}
}

func newGenerationSubscriber(ready bool) *generationSubscriber {
	ch := make(chan struct{})
	if ready {
		close(ch)
	}
	return &generationSubscriber{ready: ch, stopped: make(chan struct{})}
}

func (s *generationSubscriber) Ready() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ready
}
func (s *generationSubscriber) Stopped() <-chan struct{} { return s.stopped }
func (s *generationSubscriber) LastError() error         { return nil }
func (s *generationSubscriber) markNotReady() {
	s.mu.Lock()
	s.ready = make(chan struct{})
	s.mu.Unlock()
}
func (s *generationSubscriber) markReady() {
	s.mu.Lock()
	select {
	case <-s.ready:
	default:
		close(s.ready)
	}
	s.mu.Unlock()
}

func TestFailedReconcilePreservesKnownGoodHandler(t *testing.T) {
	store := &mutableRuntimeStore{
		channels:   map[string]pubsubmodel.Channel{"events": {Name: "events"}},
		subscriber: pubsubmodel.Subscriber{ID: "sub-a", Channel: "events", CallbackIDs: []string{"hook"}},
		callback:   pubsubmodel.Callback{ID: "hook", Type: pubsubmodel.CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://old.example/hook"}},
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	sub := newGenerationSubscriber(true)
	var dispatch func(string)
	deliveries := make(chan string, 4)
	runtime := newWithDependencies(client, store, func(_ context.Context, _ *redis.Client, _ string, onMessage func(string)) Subscriber {
		dispatch = onMessage
		return sub
	}, func(_ context.Context, url, payload string) error {
		deliveries <- url + "|" + payload
		return nil
	})
	controller, err := NewSubscriptionController(runtime)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		controller.Close()
		runtime.Close()
	})

	if err := controller.ActivateSubscription(context.Background(), "sub-a"); err != nil {
		t.Fatalf("initial activation: %v", err)
	}
	store.callback = pubsubmodel.Callback{ID: "hook", Type: pubsubmodel.CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://new.example/hook"}}
	sub.markNotReady()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := controller.ActivateSubscription(ctx, "sub-a"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reconcile during reconnect = %v, want deadline exceeded", err)
	}

	sub.markReady()
	dispatch("after-failed-reconcile")
	select {
	case got := <-deliveries:
		if got != "https://old.example/hook|after-failed-reconcile" {
			t.Fatalf("failed reconcile changed active handler: %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("known-good handler disappeared after failed reconciliation")
	}
}

func TestDetachedSubscriberRejectsStaleDispatchSnapshot(t *testing.T) {
	store := fakeStore{
		channels:    map[string]pubsubmodel.Channel{"events": {Name: "events"}},
		subscribers: map[string]pubsubmodel.Subscriber{"sub-a": {ID: "sub-a", Channel: "events", CallbackIDs: []string{"hook"}}},
		callbacks:   map[string]pubsubmodel.Callback{"hook": {ID: "hook", Type: pubsubmodel.CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://example.invalid/hook"}}},
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	runtime := newWithDependencies(client, store, func(context.Context, *redis.Client, string, func(string)) Subscriber {
		return newFakeSubscriber()
	}, func(context.Context, string, string) error {
		t.Fatal("stale detached handler reached webhook sender")
		return nil
	})
	t.Cleanup(runtime.Close)

	channel, err := runtime.Activate(context.Background(), "events", nil)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := runtime.AttachSubscriber(context.Background(), "sub-a")
	if err != nil {
		t.Fatal(err)
	}

	handle.activation.mu.RLock()
	stale := handle.activation.handlers["sub-a"].fn
	handle.activation.mu.RUnlock()
	if !handle.Close() {
		t.Fatal("Subscriber handle did not detach")
	}

	stale("late-snapshot")
	time.Sleep(20 * time.Millisecond)
	channel.Close()
}
