package pubsubruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/internal/pubsubmodel"
)

func TestFailedReconcileToNewChannelPreservesKnownGoodHandler(t *testing.T) {
	store := &mutableRuntimeStore{
		channels: map[string]pubsubmodel.Channel{
			"events":     {Name: "events"},
			"events-new": {Name: "events-new"},
		},
		subscriber: pubsubmodel.Subscriber{ID: "sub-a", Channel: "events", CallbackIDs: []string{"hook"}},
		callback:   pubsubmodel.Callback{ID: "hook", Type: pubsubmodel.CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://old.example/hook"}},
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	dispatches := make(map[string]func(string))
	deliveries := make(chan string, 4)
	runtime := newWithDependencies(client, store, func(_ context.Context, _ *redis.Client, channel string, onMessage func(string)) Subscriber {
		dispatches[channel] = onMessage
		if channel == "events-new" {
			return &fakeSubscriber{ready: make(chan struct{}), stopped: make(chan struct{})}
		}
		return newFakeSubscriber()
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

	store.subscriber = pubsubmodel.Subscriber{ID: "sub-a", Channel: "events-new", CallbackIDs: []string{"hook"}}
	store.callback = pubsubmodel.Callback{ID: "hook", Type: pubsubmodel.CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://new.example/hook"}}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := controller.ActivateSubscription(ctx, "sub-a"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reconcile to unready Channel = %v, want deadline exceeded", err)
	}
	if runtime.Active("events-new") {
		t.Fatal("failed reconciliation left a new empty listener active")
	}

	dispatches["events"]("after-failed-reconcile")
	select {
	case got := <-deliveries:
		if got != "https://old.example/hook|after-failed-reconcile" {
			t.Fatalf("failed reconcile changed active handler: %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("known-good handler disappeared after failed reconciliation")
	}
}

func TestFailedAttachAfterNewListenerReadyCleansListenerAndPreservesOldHandler(t *testing.T) {
	store := &mutableRuntimeStore{
		channels: map[string]pubsubmodel.Channel{
			"events":     {Name: "events"},
			"events-new": {Name: "events-new"},
		},
		subscriber: pubsubmodel.Subscriber{ID: "sub-a", Channel: "events", CallbackIDs: []string{"hook"}},
		callback:   pubsubmodel.Callback{ID: "hook", Type: pubsubmodel.CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://old.example/hook"}},
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	dispatches := make(map[string]func(string))
	deliveries := make(chan string, 4)
	runtime := newWithDependencies(client, store, func(_ context.Context, _ *redis.Client, channel string, onMessage func(string)) Subscriber {
		dispatches[channel] = onMessage
		return newFakeSubscriber()
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

	// The replacement listener reaches initial readiness, but the callback
	// declaration becomes unsupported before AttachSubscriber re-materializes it.
	store.subscriber = pubsubmodel.Subscriber{ID: "sub-a", Channel: "events-new", CallbackIDs: []string{"hook"}}
	store.callback = pubsubmodel.Callback{ID: "hook", Type: pubsubmodel.CallbackLua, Lua: &pubsubmodel.LuaCallback{Name: "unsupported-at-runtime"}}

	if err := controller.ActivateSubscription(context.Background(), "sub-a"); err == nil {
		t.Fatal("reconcile unexpectedly accepted unsupported replacement callback")
	}
	if runtime.Active("events-new") {
		t.Fatal("attach failure left operation-created replacement listener active")
	}

	dispatches["events"]("after-attach-failure")
	select {
	case got := <-deliveries:
		if got != "https://old.example/hook|after-attach-failure" {
			t.Fatalf("attach failure changed old active handler: %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("old handler disappeared after replacement attach failure")
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
