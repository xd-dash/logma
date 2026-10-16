package pubsubruntime

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/internal/pubsubmodel"
)

type contextBoundFakeSubscriber struct {
	ready   chan struct{}
	stopped chan struct{}
	mu      sync.Mutex
	err     error
}

func newContextBoundFakeSubscriber(ctx context.Context, ready bool) *contextBoundFakeSubscriber {
	s := &contextBoundFakeSubscriber{ready: make(chan struct{}), stopped: make(chan struct{})}
	if ready {
		close(s.ready)
	}
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		s.err = ctx.Err()
		s.mu.Unlock()
		close(s.stopped)
	}()
	return s
}

func (s *contextBoundFakeSubscriber) Ready() <-chan struct{}   { return s.ready }
func (s *contextBoundFakeSubscriber) Stopped() <-chan struct{} { return s.stopped }
func (s *contextBoundFakeSubscriber) LastError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func TestRuntimeOwnsListenerLifetimeAfterCreatorRequestEnds(t *testing.T) {
	store := fakeStore{channels: map[string]pubsubmodel.Channel{"events": {Name: "events"}}}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	listenerContexts := make(chan context.Context, 1)
	runtime := newWithDependencies(client, store, func(ctx context.Context, _ *redis.Client, _ string, _ func(string)) Subscriber {
		listenerContexts <- ctx
		return newContextBoundFakeSubscriber(ctx, true)
	}, func(context.Context, string, string) error { return nil })
	t.Cleanup(runtime.Close)

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	handle, err := runtime.Activate(requestCtx, "events", nil)
	if err != nil {
		t.Fatal(err)
	}
	listenerCtx := <-listenerContexts
	cancelRequest()

	select {
	case <-listenerCtx.Done():
		t.Fatal("listener inherited creator request lifetime")
	case <-time.After(20 * time.Millisecond):
	}

	if !handle.Close() {
		t.Fatal("force-deactivate handle did not close current listener generation")
	}
	select {
	case <-listenerCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("listener context remained live after explicit force-deactivation")
	}
}

func TestListenerLeaseDoesNotMigrateAcrossForceDeactivation(t *testing.T) {
	store := fakeStore{
		channels: map[string]pubsubmodel.Channel{"events": {Name: "events"}},
		subscribers: map[string]pubsubmodel.Subscriber{
			"sub-a": {ID: "sub-a", Channel: "events", CallbackIDs: []string{"hook-a"}},
		},
		callbacks: map[string]pubsubmodel.Callback{
			"hook-a": {ID: "hook-a", Type: pubsubmodel.CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://example.invalid/hook"}},
		},
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	var mu sync.Mutex
	generation := 0
	runtime := newWithDependencies(client, store, func(ctx context.Context, _ *redis.Client, _ string, _ func(string)) Subscriber {
		mu.Lock()
		generation++
		current := generation
		mu.Unlock()
		return newContextBoundFakeSubscriber(ctx, current > 1)
	}, func(context.Context, string, string) error { return nil })
	t.Cleanup(runtime.Close)

	lease, err := runtime.acquireListener(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	if !runtime.Deactivate("events") {
		t.Fatal("failed to force-deactivate first listener generation")
	}
	if _, err := runtime.Activate(context.Background(), "events", nil); err != nil {
		t.Fatalf("create replacement generation: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := lease.WaitReady(ctx); err == nil {
		t.Fatal("stale lease accepted readiness from replacement listener generation")
	}
	if _, err := lease.AttachSubscriber(context.Background(), "sub-a"); err == nil || !strings.Contains(err.Error(), "generation changed") {
		t.Fatalf("stale lease attachment error = %v, want generation-changed failure", err)
	}
}
