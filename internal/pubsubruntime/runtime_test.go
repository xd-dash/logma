package pubsubruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/internal/pubsubmodel"
)

type fakeStore struct {
	channels map[string]pubsubmodel.Channel
}

func (s fakeStore) GetChannel(_ context.Context, name string) (pubsubmodel.Channel, error) {
	channel, ok := s.channels[name]
	if !ok {
		return pubsubmodel.Channel{}, pubsubmodel.ErrNotFound
	}
	return channel, nil
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
	runtime := newWithSubscriber(client, store, func(_ context.Context, _ *redis.Client, channel string, onMessage func(string)) Subscriber {
		calls++
		if channel != "events" {
			t.Fatalf("subscribe channel = %q", channel)
		}
		gotHandler = onMessage
		sub = newFakeSubscriber()
		return sub
	})

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
		t.Fatal("nil callback was not normalized to a no-op handler")
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

func TestRuntimeRequiresPersistedChannel(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })
	runtime := newWithSubscriber(client, fakeStore{channels: map[string]pubsubmodel.Channel{}}, func(context.Context, *redis.Client, string, func(string)) Subscriber {
		t.Fatal("subscriber started for missing Channel")
		return nil
	})

	_, err := runtime.Activate(context.Background(), "missing", nil)
	if !errors.Is(err, pubsubmodel.ErrNotFound) {
		t.Fatalf("Activate missing error = %v, want ErrNotFound", err)
	}
}

func TestRuntimeRejectsEmptyChannel(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })
	runtime := newWithSubscriber(client, fakeStore{}, func(context.Context, *redis.Client, string, func(string)) Subscriber {
		t.Fatal("subscriber started for empty Channel")
		return nil
	})

	if _, err := runtime.Activate(context.Background(), "  ", nil); err == nil {
		t.Fatal("Activate accepted empty channel")
	}
}
