package pubsubruntime

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/internal/pubsubmodel"
)

func TestScopedRuntimeUsesCanonicalTransportAddress(t *testing.T) {
	store := fakeStore{channels: map[string]pubsubmodel.Channel{
		"market:quotes": {Name: "market:quotes"},
	}}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })
	runtime, err := NewScoped(client, store, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)

	var transport string
	runtime.subscribe = func(_ context.Context, _ *redis.Client, channel string, _ func(string)) Subscriber {
		transport = channel
		return newFakeSubscriber()
	}
	if _, err := runtime.Activate(context.Background(), "market:quotes", nil); err != nil {
		t.Fatal(err)
	}
	if want := "tenant-a:logma:transport:channel:market%3Aquotes"; transport != want {
		t.Fatalf("transport channel = %q, want %q", transport, want)
	}
	if !runtime.Active("market:quotes") {
		t.Fatal("logical Channel identity is not active")
	}
}

func TestSubscriptionControllerActivateRefreshesCurrentDefinition(t *testing.T) {
	store := &mutableRuntimeStore{
		channels:   map[string]pubsubmodel.Channel{"events": {Name: "events"}},
		subscriber: pubsubmodel.Subscriber{ID: "sub-a", Channel: "events", CallbackIDs: []string{"hook"}},
		callback:   pubsubmodel.Callback{ID: "hook", Type: pubsubmodel.CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://one.example/hook"}},
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	var dispatch func(string)
	deliveries := make(chan string, 2)
	runtime := newWithDependencies(client, store, func(_ context.Context, _ *redis.Client, _ string, onMessage func(string)) Subscriber {
		dispatch = onMessage
		return newFakeSubscriber()
	}, func(_ context.Context, url, payload string) error {
		deliveries <- url + "|" + payload
		return nil
	})
	controller, err := NewSubscriptionController(runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		controller.Close()
		runtime.Close()
	}()

	if err := controller.ActivateSubscription(context.Background(), "sub-a"); err != nil {
		t.Fatal(err)
	}
	dispatch("first")
	select {
	case got := <-deliveries:
		if want := "https://one.example/hook|first"; got != want {
			t.Fatalf("first delivery = %q, want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("first reconciled delivery did not arrive")
	}

	store.callback = pubsubmodel.Callback{ID: "hook", Type: pubsubmodel.CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://two.example/hook"}}
	if err := controller.ActivateSubscription(context.Background(), "sub-a"); err != nil {
		t.Fatal(err)
	}
	dispatch("second")
	select {
	case got := <-deliveries:
		if want := "https://two.example/hook|second"; got != want {
			t.Fatalf("second delivery = %q, want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("second reconciled delivery did not arrive")
	}
}

type mutableRuntimeStore struct {
	channels   map[string]pubsubmodel.Channel
	subscriber pubsubmodel.Subscriber
	callback   pubsubmodel.Callback
}

func (s *mutableRuntimeStore) GetChannel(_ context.Context, name string) (pubsubmodel.Channel, error) {
	channel, ok := s.channels[name]
	if !ok {
		return pubsubmodel.Channel{}, pubsubmodel.ErrNotFound
	}
	return channel, nil
}

func (s *mutableRuntimeStore) GetSubscriber(_ context.Context, id string) (pubsubmodel.Subscriber, error) {
	if id != s.subscriber.ID {
		return pubsubmodel.Subscriber{}, pubsubmodel.ErrNotFound
	}
	return s.subscriber, nil
}

func (s *mutableRuntimeStore) GetCallback(_ context.Context, id string) (pubsubmodel.Callback, error) {
	if id != s.callback.ID {
		return pubsubmodel.Callback{}, pubsubmodel.ErrNotFound
	}
	return s.callback, nil
}
