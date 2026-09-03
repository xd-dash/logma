package pubsubruntime

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/internal/pubsubmodel"
)

func TestRuntimeIdempotentActivateCanInstallChannelHandler(t *testing.T) {
	store := fakeStore{channels: map[string]pubsubmodel.Channel{"events": {Name: "events"}}}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	var dispatch func(string)
	runtime := newWithDependencies(client, store, func(_ context.Context, _ *redis.Client, _ string, onMessage func(string)) Subscriber {
		dispatch = onMessage
		return newFakeSubscriber()
	}, func(context.Context, string, string) error { return nil })
	t.Cleanup(runtime.Close)

	first, err := runtime.Activate(context.Background(), "events", nil)
	if err != nil {
		t.Fatal(err)
	}
	var got string
	if _, err := runtime.Activate(context.Background(), "events", func(payload string) { got = payload }); err != nil {
		t.Fatal(err)
	}
	dispatch("installed-later")
	if got != "installed-later" {
		t.Fatalf("channel handler got %q", got)
	}
	first.Close()
}

func TestStaleChannelHandleCannotDeactivateNewActivation(t *testing.T) {
	store := fakeStore{channels: map[string]pubsubmodel.Channel{"events": {Name: "events"}}}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	runtime := newWithDependencies(client, store, func(context.Context, *redis.Client, string, func(string)) Subscriber {
		return newFakeSubscriber()
	}, func(context.Context, string, string) error { return nil })
	t.Cleanup(runtime.Close)

	first, err := runtime.Activate(context.Background(), "events", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Close() {
		t.Fatal("first Close did not deactivate first generation")
	}
	second, err := runtime.Activate(context.Background(), "events", nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.Close() {
		t.Fatal("stale Channel handle deactivated a newer generation")
	}
	if !runtime.Active("events") {
		t.Fatal("new Channel generation is no longer active")
	}
	second.Close()
}

func TestListenerLeaseDoesNotMigrateAcrossGenerationReplacement(t *testing.T) {
	store := fakeStore{
		channels:    map[string]pubsubmodel.Channel{"events": {Name: "events"}},
		subscribers: map[string]pubsubmodel.Subscriber{"sub-a": {ID: "sub-a", Channel: "events", CallbackIDs: []string{"hook"}}},
		callbacks:   map[string]pubsubmodel.Callback{"hook": {ID: "hook", Type: pubsubmodel.CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://example.invalid/hook"}}},
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	var activations int
	runtime := newWithDependencies(client, store, func(context.Context, *redis.Client, string, func(string)) Subscriber {
		activations++
		return newFakeSubscriber()
	}, func(context.Context, string, string) error { return nil })
	t.Cleanup(runtime.Close)

	lease, err := runtime.acquireListener(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	generationA := lease.activation
	if !runtime.Deactivate("events") {
		t.Fatal("force Deactivate did not retire leased generation A")
	}

	generationB, err := runtime.Activate(context.Background(), "events", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer generationB.Close()
	if generationB.activation == generationA {
		t.Fatal("replacement activation reused retired generation A")
	}
	if activations != 2 {
		t.Fatalf("listener activations=%d want 2", activations)
	}

	if err := lease.WaitReady(context.Background()); err == nil {
		t.Fatal("generation-A lease silently waited on replacement generation B")
	}
	if _, err := lease.AttachSubscriber(context.Background(), "sub-a"); err == nil {
		t.Fatal("generation-A lease silently attached Subscriber to replacement generation B")
	}
	if lease.Close() {
		t.Fatal("stale generation-A lease released state from replacement generation B")
	}
	if !runtime.Active("events") {
		t.Fatal("stale generation-A lease disturbed replacement generation B")
	}
}

func TestCanceledCurrentGenerationRejectsAttachmentBeforeStoppedCleanup(t *testing.T) {
	store := fakeStore{
		channels:    map[string]pubsubmodel.Channel{"events": {Name: "events"}},
		subscribers: map[string]pubsubmodel.Subscriber{"sub-a": {ID: "sub-a", Channel: "events", CallbackIDs: []string{"hook"}}},
		callbacks:   map[string]pubsubmodel.Callback{"hook": {ID: "hook", Type: pubsubmodel.CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://example.invalid/hook"}}},
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	stopped := make(chan struct{})
	runtime := newWithDependencies(client, store, func(context.Context, *redis.Client, string, func(string)) Subscriber {
		ready := make(chan struct{})
		close(ready)
		return &fakeSubscriber{ready: ready, stopped: stopped}
	}, func(context.Context, string, string) error { return nil })
	t.Cleanup(runtime.Close)

	lease, err := runtime.acquireListener(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	activation := lease.activation

	// Simulate transport/runtime cancellation before the Subscriber's Stopped
	// signal lets removeWhenStopped delete the map entry. The generation is still
	// name-addressable internally, but it is no longer a valid capability target.
	activation.cancel()
	if _, err := lease.AttachSubscriber(context.Background(), "sub-a"); err == nil {
		t.Fatal("attachment succeeded against canceled generation still awaiting stopped cleanup")
	}
	if runtime.Active("events") {
		t.Fatal("canceled generation reported active while awaiting stopped cleanup")
	}
	close(stopped)
	lease.Close()
}

func TestStaleSubscriberHandleCannotDetachReplacement(t *testing.T) {
	store := fakeStore{
		channels:    map[string]pubsubmodel.Channel{"events": {Name: "events"}},
		subscribers: map[string]pubsubmodel.Subscriber{"sub-a": {ID: "sub-a", Channel: "events", CallbackIDs: []string{"hook"}}},
		callbacks:   map[string]pubsubmodel.Callback{"hook": {ID: "hook", Type: pubsubmodel.CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://example.invalid/hook"}}},
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	var dispatch func(string)
	delivered := make(chan struct{}, 2)
	runtime := newWithDependencies(client, store, func(_ context.Context, _ *redis.Client, _ string, onMessage func(string)) Subscriber {
		dispatch = onMessage
		return newFakeSubscriber()
	}, func(context.Context, string, string) error {
		delivered <- struct{}{}
		return nil
	})
	t.Cleanup(runtime.Close)

	channel, err := runtime.Activate(context.Background(), "events", nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := runtime.AttachSubscriber(context.Background(), "sub-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := runtime.AttachSubscriber(context.Background(), "sub-a")
	if err != nil {
		t.Fatal(err)
	}
	if first.Close() {
		t.Fatal("stale Subscriber handle detached its replacement")
	}
	dispatch("payload")
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("replacement Subscriber did not receive delivery")
	}
	if !second.Close() {
		t.Fatal("current Subscriber handle did not detach")
	}
	channel.Close()
}

func TestDeactivateCancelsWebhookContext(t *testing.T) {
	store := fakeStore{
		channels:    map[string]pubsubmodel.Channel{"events": {Name: "events"}},
		subscribers: map[string]pubsubmodel.Subscriber{"sub-a": {ID: "sub-a", Channel: "events", CallbackIDs: []string{"hook"}}},
		callbacks:   map[string]pubsubmodel.Callback{"hook": {ID: "hook", Type: pubsubmodel.CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://example.invalid/hook"}}},
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	var dispatch func(string)
	started := make(chan struct{})
	canceled := make(chan struct{})
	runtime := newWithDependencies(client, store, func(_ context.Context, _ *redis.Client, _ string, onMessage func(string)) Subscriber {
		dispatch = onMessage
		return newFakeSubscriber()
	}, func(ctx context.Context, _, _ string) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		return ctx.Err()
	})
	t.Cleanup(runtime.Close)

	channel, err := runtime.Activate(context.Background(), "events", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.AttachSubscriber(context.Background(), "sub-a"); err != nil {
		t.Fatal(err)
	}
	dispatch("payload")
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("webhook delivery did not start")
	}
	channel.Close()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("webhook context was not canceled")
	}
}
