package pubsubruntime

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/internal/pubsubmodel"
)

type fakeStore struct {
	channels    map[string]pubsubmodel.Channel
	subscribers map[string]pubsubmodel.Subscriber
	callbacks   map[string]pubsubmodel.Callback
}

func (s fakeStore) GetChannel(_ context.Context, name string) (pubsubmodel.Channel, error) {
	channel, ok := s.channels[name]
	if !ok {
		return pubsubmodel.Channel{}, pubsubmodel.ErrNotFound
	}
	return channel, nil
}

func (s fakeStore) GetSubscriber(_ context.Context, id string) (pubsubmodel.Subscriber, error) {
	subscriber, ok := s.subscribers[id]
	if !ok {
		return pubsubmodel.Subscriber{}, pubsubmodel.ErrNotFound
	}
	return subscriber, nil
}

func (s fakeStore) GetCallback(_ context.Context, id string) (pubsubmodel.Callback, error) {
	callback, ok := s.callbacks[id]
	if !ok {
		return pubsubmodel.Callback{}, pubsubmodel.ErrNotFound
	}
	return callback, nil
}

type fakeSubscriber struct {
	ready   chan struct{}
	stopped chan struct{}
	err     error
}

func newFakeSubscriber() *fakeSubscriber {
	ready := make(chan struct{})
	close(ready)
	return &fakeSubscriber{ready: ready, stopped: make(chan struct{})}
}

func (s *fakeSubscriber) Ready() <-chan struct{}   { return s.ready }
func (s *fakeSubscriber) Stopped() <-chan struct{} { return s.stopped }
func (s *fakeSubscriber) LastError() error         { return s.err }

func TestRuntimeActivatesPersistedChannelWithoutCallback(t *testing.T) {
	store := fakeStore{channels: map[string]pubsubmodel.Channel{
		"events": {Name: "events"},
	}}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	var calls int
	var gotHandler func(string)
	var sub *fakeSubscriber
	runtime := newWithDependencies(client, store, func(_ context.Context, _ *redis.Client, channel string, onMessage func(string)) Subscriber {
		calls++
		if channel != "events" {
			t.Fatalf("subscribe channel = %q", channel)
		}
		gotHandler = onMessage
		sub = newFakeSubscriber()
		return sub
	}, func(context.Context, string, string) error { return nil })
	t.Cleanup(runtime.Close)

	handle, err := runtime.Activate(context.Background(), " events ", nil)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if calls != 1 {
		t.Fatalf("subscribe calls = %d, want 1", calls)
	}
	if !runtime.Active("events") {
		t.Fatal("channel is not active")
	}
	if gotHandler == nil {
		t.Fatal("runtime did not install a Channel dispatcher")
	}
	gotHandler("payload")

	second, err := runtime.Activate(context.Background(), "events", nil)
	if err != nil {
		t.Fatalf("second Activate: %v", err)
	}
	if calls != 1 {
		t.Fatalf("idempotent Activate subscribed %d times, want 1", calls)
	}
	if second.sub != handle.sub {
		t.Fatal("idempotent Activate returned a different subscriber")
	}
	if !handle.Close() {
		t.Fatal("Close did not deactivate channel")
	}
	if runtime.Active("events") {
		t.Fatal("channel remains active after Close")
	}
	if handle.Close() {
		t.Fatal("second Close unexpectedly reported deactivation")
	}
	close(sub.stopped)
}

func TestRuntimeAttachesWebhookSubscriberToActiveChannel(t *testing.T) {
	store := fakeStore{
		channels:    map[string]pubsubmodel.Channel{"events": {Name: "events"}},
		subscribers: map[string]pubsubmodel.Subscriber{"sub-a": {ID: "sub-a", Channel: "events", CallbackIDs: []string{"callback-a"}}},
		callbacks: map[string]pubsubmodel.Callback{
			"callback-a": {
				ID:   "callback-a",
				Type: CallbackWebhook,
				Webhook: &pubsubmodel.WebhookCallback{CallbackURLs: []string{
					"https://one.example/callback",
					"https://two.example/callback",
				}},
			},
		},
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	var dispatch func(string)
	delivered := make(chan string, 4)
	runtime := newWithDependencies(client, store, func(_ context.Context, _ *redis.Client, _ string, onMessage func(string)) Subscriber {
		dispatch = onMessage
		return newFakeSubscriber()
	}, func(_ context.Context, url, payload string) error {
		delivered <- url + "|" + payload
		return nil
	})
	t.Cleanup(runtime.Close)

	if _, err := runtime.Activate(context.Background(), "events", nil); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	handle, err := runtime.AttachSubscriber(context.Background(), "sub-a")
	if err != nil {
		t.Fatalf("AttachSubscriber: %v", err)
	}
	dispatch(`{"probe":"first"}`)
	got := []string{<-delivered, <-delivered}
	want := []string{
		`https://one.example/callback|{"probe":"first"}`,
		`https://two.example/callback|{"probe":"first"}`,
	}
	if !reflect.DeepEqual(got, want) && !reflect.DeepEqual(got, []string{want[1], want[0]}) {
		t.Fatalf("deliveries = %#v, want %#v in either worker order", got, want)
	}

	if !handle.Close() {
		t.Fatal("Subscriber Close did not detach delivery")
	}
	dispatch(`{"probe":"second"}`)
	select {
	case got := <-delivered:
		t.Fatalf("detached Subscriber received delivery: %s", got)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestRuntimeRequiresActiveChannelForSubscriber(t *testing.T) {
	store := fakeStore{
		subscribers: map[string]pubsubmodel.Subscriber{"sub-a": {ID: "sub-a", Channel: "events", CallbackIDs: []string{"callback-a"}}},
		callbacks:   map[string]pubsubmodel.Callback{"callback-a": {ID: "callback-a", Type: CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://one.example/callback"}}},
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	runtime := newWithDependencies(client, store, func(context.Context, *redis.Client, string, func(string)) Subscriber {
		t.Fatal("Redis listener should not start")
		return nil
	}, func(context.Context, string, string) error { return nil })
	t.Cleanup(runtime.Close)

	if _, err := runtime.AttachSubscriber(context.Background(), "sub-a"); err == nil {
		t.Fatal("AttachSubscriber accepted inactive Channel")
	}
}

func TestRuntimeRequiresPersistedChannel(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	runtime := newWithDependencies(client, fakeStore{channels: map[string]pubsubmodel.Channel{}}, func(context.Context, *redis.Client, string, func(string)) Subscriber {
		t.Fatal("subscriber started for missing Channel")
		return nil
	}, func(context.Context, string, string) error { return nil })
	t.Cleanup(runtime.Close)

	_, err := runtime.Activate(context.Background(), "missing", nil)
	if !errors.Is(err, pubsubmodel.ErrNotFound) {
		t.Fatalf("Activate missing error = %v, want ErrNotFound", err)
	}
}

func TestRuntimeRejectsEmptyChannel(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	runtime := newWithDependencies(client, fakeStore{}, func(context.Context, *redis.Client, string, func(string)) Subscriber {
		t.Fatal("subscriber started for empty Channel")
		return nil
	}, func(context.Context, string, string) error { return nil })
	t.Cleanup(runtime.Close)

	if _, err := runtime.Activate(context.Background(), "  ", nil); err == nil {
		t.Fatal("Activate accepted empty channel")
	}
}
