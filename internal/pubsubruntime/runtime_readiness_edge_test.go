package pubsubruntime

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/internal/pubsubmodel"
)

func TestWaitReadyRejectsSubscriberAlreadyReadyAndStopped(t *testing.T) {
	store := fakeStore{channels: map[string]pubsubmodel.Channel{"events": {Name: "events"}}}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })

	ready := make(chan struct{})
	stopped := make(chan struct{})
	close(ready)
	close(stopped)
	runtime := newWithDependencies(client, store, func(context.Context, *redis.Client, string, func(string)) Subscriber {
		return &fakeSubscriber{ready: ready, stopped: stopped}
	}, func(context.Context, string, string) error { return nil })
	t.Cleanup(runtime.Close)

	if _, err := runtime.Activate(context.Background(), "events", nil); err != nil {
		t.Fatal(err)
	}
	if err := runtime.WaitReady(context.Background(), "events"); err == nil {
		t.Fatal("WaitReady accepted listener whose Ready and Stopped signals were both closed")
	}
}
