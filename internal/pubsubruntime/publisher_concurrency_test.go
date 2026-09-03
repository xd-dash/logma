package pubsubruntime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xd-dash/logma/internal/pubsubmodel"
)

type blockingPublisherProvider struct {
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
}

func (p *blockingPublisherProvider) EnsureActive(context.Context, pubsubmodel.Publisher, pubsubmodel.Channel) error {
	if p.calls.Add(1) == 1 {
		close(p.started)
	}
	<-p.release
	return nil
}

func TestPublisherReconcilerCoalescesConcurrentSameIdentity(t *testing.T) {
	store := publisherTestStore{
		publisher: pubsubmodel.Publisher{ID: "stonks-live", Channel: "market:quotes", Type: "stonks"},
		channel:   pubsubmodel.Channel{Name: "market:quotes"},
	}
	provider := &blockingPublisherProvider{started: make(chan struct{}), release: make(chan struct{})}
	registry := NewPublisherRegistry()
	if err := registry.Register("stonks", provider); err != nil {
		t.Fatal(err)
	}
	reconciler, err := NewPublisherReconciler(store, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer reconciler.Close()

	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- reconciler.Reconcile(context.Background(), "stonks-live") }()
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("provider did not start")
	}
	go func() { second <- reconciler.Reconcile(context.Background(), "stonks-live") }()

	time.Sleep(20 * time.Millisecond)
	if got := provider.calls.Load(); got != 1 {
		t.Fatalf("concurrent same-Publisher reconcile entered provider %d times, want 1", got)
	}
	close(provider.release)
	if err := <-first; err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("coalesced reconcile: %v", err)
	}
	if got := provider.calls.Load(); got != 1 {
		t.Fatalf("provider calls after completion = %d, want 1", got)
	}
}
