package pubsubruntime

import (
	"context"
	"testing"
	"time"
)

func TestDeliveryDispatcherDropsWhenBoundedQueueIsFull(t *testing.T) {
	started := make(chan struct{}, deliveryWorkerCount)
	release := make(chan struct{})
	dispatcher := newDeliveryDispatcher(func(context.Context, string, string) error {
		// Only the initial worker-saturating observations matter. Later queued
		// jobs must not block test instrumentation while dispatcher.close drains.
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return nil
	})

	for i := 0; i < deliveryWorkerCount; i++ {
		if !dispatcher.dispatch(deliveryJob{ctx: context.Background(), subscriberID: "sub", url: "https://example.invalid", payload: "worker"}) {
			t.Fatal("failed to dispatch worker-saturating job")
		}
	}
	for i := 0; i < deliveryWorkerCount; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("delivery worker did not start")
		}
	}

	for i := 0; i < deliveryQueueSize; i++ {
		if !dispatcher.dispatch(deliveryJob{ctx: context.Background(), subscriberID: "sub", url: "https://example.invalid", payload: "queued"}) {
			t.Fatalf("queue rejected job %d before reaching configured capacity", i)
		}
	}

	begin := time.Now()
	if dispatcher.dispatch(deliveryJob{ctx: context.Background(), subscriberID: "sub", url: "https://example.invalid", payload: "overflow"}) {
		t.Fatal("dispatcher accepted a job beyond its configured queue bound")
	}
	if elapsed := time.Since(begin); elapsed > 50*time.Millisecond {
		t.Fatalf("full dispatcher blocked for %s instead of dropping immediately", elapsed)
	}

	close(release)
	dispatcher.close()
}

func TestDeliveryDispatcherRejectsCanceledAndClosedWork(t *testing.T) {
	dispatcher := newDeliveryDispatcher(func(context.Context, string, string) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if dispatcher.dispatch(deliveryJob{ctx: ctx, subscriberID: "sub", url: "https://example.invalid", payload: "canceled"}) {
		t.Fatal("dispatcher accepted already-canceled delivery")
	}
	dispatcher.close()
	if dispatcher.dispatch(deliveryJob{ctx: context.Background(), subscriberID: "sub", url: "https://example.invalid", payload: "closed"}) {
		t.Fatal("closed dispatcher accepted delivery")
	}
}
