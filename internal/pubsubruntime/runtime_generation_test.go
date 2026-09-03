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
	runtime := newWithDependencies(client, store, func(_ context.Context, _ *redis.Client, _ string, onMessage func(string)) Subscriber { dispatch = onMessage; return newFakeSubscriber() }, func(context.Context, string, string) error { return nil })
	t.Cleanup(runtime.Close)
	first, err := runtime.Activate(context.Background(), "events", nil)
	if err != nil { t.Fatal(err) }
	var got string
	if _, err := runtime.Activate(context.Background(), "events", func(payload string) { got = payload }); err != nil { t.Fatal(err) }
	dispatch("installed-later")
	if got != "installed-later" { t.Fatalf("channel handler got %q", got) }
	first.Close()
}

func TestStaleChannelHandleCannotDeactivateNewActivation(t *testing.T) {
	store := fakeStore{channels: map[string]pubsubmodel.Channel{"events": {Name: "events"}}}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })
	runtime := newWithDependencies(client, store, func(context.Context, *redis.Client, string, func(string)) Subscriber { return newFakeSubscriber() }, func(context.Context, string, string) error { return nil })
	t.Cleanup(runtime.Close)
	first, err := runtime.Activate(context.Background(), "events", nil)
	if err != nil { t.Fatal(err) }
	if !first.Close() { t.Fatal("first Close did not deactivate first generation") }
	second, err := runtime.Activate(context.Background(), "events", nil)
	if err != nil { t.Fatal(err) }
	if first.Close() { t.Fatal("stale Channel handle deactivated a newer generation") }
	if !runtime.Active("events") { t.Fatal("new Channel generation is no longer active") }
	second.Close()
}

func TestStaleSubscriberHandleCannotDetachReplacement(t *testing.T) {
	store := fakeStore{
		channels: map[string]pubsubmodel.Channel{"events": {Name: "events"}},
		subscribers: map[string]pubsubmodel.Subscriber{"sub-a": {ID: "sub-a", Channel: "events", CallbackIDs: []string{"hook"}}},
		callbacks: map[string]pubsubmodel.Callback{"hook": {ID: "hook", Type: CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://example.invalid/hook"}}},
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })
	var dispatch func(string)
	delivered := make(chan struct{}, 2)
	runtime := newWithDependencies(client, store, func(_ context.Context, _ *redis.Client, _ string, onMessage func(string)) Subscriber { dispatch = onMessage; return newFakeSubscriber() }, func(context.Context, string, string) error { delivered <- struct{}{}; return nil })
	t.Cleanup(runtime.Close)
	channel, err := runtime.Activate(context.Background(), "events", nil)
	if err != nil { t.Fatal(err) }
	first, err := runtime.AttachSubscriber(context.Background(), "sub-a")
	if err != nil { t.Fatal(err) }
	second, err := runtime.AttachSubscriber(context.Background(), "sub-a")
	if err != nil { t.Fatal(err) }
	if first.Close() { t.Fatal("stale Subscriber handle detached its replacement") }
	dispatch("payload")
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("replacement Subscriber did not receive delivery")
	}
	if !second.Close() { t.Fatal("current Subscriber handle did not detach") }
	channel.Close()
}

func TestDeactivateCancelsWebhookContext(t *testing.T) {
	store := fakeStore{
		channels: map[string]pubsubmodel.Channel{"events": {Name: "events"}},
		subscribers: map[string]pubsubmodel.Subscriber{"sub-a": {ID: "sub-a", Channel: "events", CallbackIDs: []string{"hook"}}},
		callbacks: map[string]pubsubmodel.Callback{"hook": {ID: "hook", Type: CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://example.invalid/hook"}}},
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })
	var dispatch func(string)
	started := make(chan struct{})
	canceled := make(chan struct{})
	runtime := newWithDependencies(client, store, func(_ context.Context, _ *redis.Client, _ string, onMessage func(string)) Subscriber { dispatch = onMessage; return newFakeSubscriber() }, func(ctx context.Context, _, _ string) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		return ctx.Err()
	})
	t.Cleanup(runtime.Close)
	channel, err := runtime.Activate(context.Background(), "events", nil)
	if err != nil { t.Fatal(err) }
	if _, err := runtime.AttachSubscriber(context.Background(), "sub-a"); err != nil { t.Fatal(err) }
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
