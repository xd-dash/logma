package pubsubruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/internal/pubsubmodel"
)

func TestSubscriptionControllerOwnsActivationLifetime(t *testing.T) {
	store := fakeStore{
		channels: map[string]pubsubmodel.Channel{"events": {Name: "events"}},
		subscribers: map[string]pubsubmodel.Subscriber{
			"sub-a": {ID: "sub-a", Channel: "events", CallbackIDs: []string{"callback-a"}},
		},
		callbacks: map[string]pubsubmodel.Callback{
			"callback-a": {ID: "callback-a", Type: pubsubmodel.CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://one.example/callback"}},
		},
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	var activationCtx context.Context
	var dispatch func(string)
	delivered := make(chan string, 2)
	runtime := newWithDependencies(client, store, func(ctx context.Context, _ *redis.Client, _ string, onMessage func(string)) Subscriber {
		activationCtx = ctx
		dispatch = onMessage
		return newFakeSubscriber()
	}, func(_ context.Context, _ string, payload string) error {
		delivered <- payload
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

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	if err := controller.ActivateSubscription(requestCtx, "sub-a"); err != nil {
		t.Fatalf("ActivateSubscription: %v", err)
	}
	cancelRequest()
	select {
	case <-activationCtx.Done():
		t.Fatal("request cancellation stopped controller-owned activation")
	default:
	}

	if err := controller.ActivateSubscription(context.Background(), "sub-a"); err != nil {
		t.Fatalf("reconcile ActivateSubscription: %v", err)
	}
	dispatch("first")
	select {
	case got := <-delivered:
		if got != "first" {
			t.Fatalf("delivery=%q want first", got)
		}
	case <-time.After(time.Second):
		t.Fatal("reconciled Subscription did not receive delivery")
	}
	select {
	case got := <-delivered:
		t.Fatalf("reconciliation installed duplicate handler; extra delivery=%q", got)
	case <-time.After(20 * time.Millisecond):
	}

	if err := controller.ShutdownSubscription(context.Background(), "sub-a"); err != nil {
		t.Fatalf("ShutdownSubscription: %v", err)
	}
	if runtime.Active("events") {
		t.Fatal("last Subscriber shutdown left an empty Redis Channel listener active")
	}
	if err := controller.ShutdownSubscription(context.Background(), "sub-a"); err != nil {
		t.Fatalf("idempotent declared-inactive ShutdownSubscription: %v", err)
	}
	dispatch("second")
	select {
	case got := <-delivered:
		t.Fatalf("shutdown Subscription received delivery=%q", got)
	case <-time.After(20 * time.Millisecond):
	}

	select {
	case <-activationCtx.Done():
	default:
		t.Fatal("last Subscriber shutdown did not cancel controller-owned Channel listener")
	}
}

func TestSubscriptionControllerWaitsForInitialRedisReadiness(t *testing.T) {
	store := fakeStore{
		channels: map[string]pubsubmodel.Channel{"events": {Name: "events"}},
		subscribers: map[string]pubsubmodel.Subscriber{
			"sub-a": {ID: "sub-a", Channel: "events", CallbackIDs: []string{"callback-a"}},
		},
		callbacks: map[string]pubsubmodel.Callback{
			"callback-a": {ID: "callback-a", Type: pubsubmodel.CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://one.example/callback"}},
		},
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	ready := make(chan struct{})
	stopped := make(chan struct{})
	runtime := newWithDependencies(client, store, func(context.Context, *redis.Client, string, func(string)) Subscriber {
		return &fakeSubscriber{ready: ready, stopped: stopped}
	}, func(context.Context, string, string) error { return nil })
	controller, err := NewSubscriptionController(runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		controller.Close()
		runtime.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := controller.ActivateSubscription(ctx, "sub-a"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ActivateSubscription before Redis readiness = %v, want deadline exceeded", err)
	}
	if runtime.Active("events") {
		t.Fatal("failed initial readiness left an empty Redis listener active")
	}
}

func TestSubscriptionControllerInstallsHandlerBeforeReadyAcknowledgement(t *testing.T) {
	store := fakeStore{
		channels: map[string]pubsubmodel.Channel{"events": {Name: "events"}},
		subscribers: map[string]pubsubmodel.Subscriber{
			"sub-a": {ID: "sub-a", Channel: "events", CallbackIDs: []string{"callback-a"}},
		},
		callbacks: map[string]pubsubmodel.Callback{
			"callback-a": {ID: "callback-a", Type: pubsubmodel.CallbackWebhook, Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://one.example/callback"}},
		},
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	ready := make(chan struct{})
	stopped := make(chan struct{})
	dispatchReady := make(chan func(string), 1)
	delivered := make(chan string, 1)
	runtime := newWithDependencies(client, store, func(_ context.Context, _ *redis.Client, _ string, onMessage func(string)) Subscriber {
		dispatchReady <- onMessage
		return &fakeSubscriber{ready: ready, stopped: stopped}
	}, func(_ context.Context, _ string, payload string) error {
		delivered <- payload
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

	result := make(chan error, 1)
	go func() {
		result <- controller.ActivateSubscription(context.Background(), "sub-a")
	}()
	dispatch := <-dispatchReady

	deadline := time.Now().Add(time.Second)
	for {
		runtime.mu.Lock()
		activation := runtime.active["events"]
		runtime.mu.Unlock()
		if activation != nil && activation.handlerCount() == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Subscription handler was not installed while activation waited for readiness")
		}
		time.Sleep(time.Millisecond)
	}

	// Simulate a message becoming deliverable at the same boundary as the Redis
	// SUBSCRIBE acknowledgement. The handler is already present before success.
	dispatch("first-after-ack")
	close(ready)
	if err := <-result; err != nil {
		t.Fatalf("ActivateSubscription: %v", err)
	}
	select {
	case got := <-delivered:
		if got != "first-after-ack" {
			t.Fatalf("delivery=%q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("message at readiness boundary was lost before handler installation")
	}
}

func TestSubscriptionControllerRejectsMissingAndClosedResources(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })
	runtime := newWithDependencies(client, fakeStore{
		channels:    map[string]pubsubmodel.Channel{},
		subscribers: map[string]pubsubmodel.Subscriber{},
		callbacks:   map[string]pubsubmodel.Callback{},
	}, func(context.Context, *redis.Client, string, func(string)) Subscriber {
		t.Fatal("runtime listener started for missing Subscriber")
		return nil
	}, func(context.Context, string, string) error { return nil })
	controller, err := NewSubscriptionController(runtime)
	if err != nil {
		t.Fatal(err)
	}

	if err := controller.ActivateSubscription(context.Background(), "missing"); !errors.Is(err, pubsubmodel.ErrNotFound) {
		t.Fatalf("ActivateSubscription missing error=%v want ErrNotFound", err)
	}
	if err := controller.ShutdownSubscription(context.Background(), "missing"); !errors.Is(err, pubsubmodel.ErrNotFound) {
		t.Fatalf("ShutdownSubscription missing error=%v want ErrNotFound", err)
	}
	controller.Close()
	if err := controller.ActivateSubscription(context.Background(), "missing"); err == nil {
		t.Fatal("closed controller accepted activation")
	}
	if err := controller.ShutdownSubscription(context.Background(), "missing"); err == nil {
		t.Fatal("closed controller accepted shutdown")
	}
	runtime.Close()
}
