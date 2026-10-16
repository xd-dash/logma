package pubsubruntime

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xd-dash/logma/internal/pubsubmodel"
)

type mutablePublisherStore struct {
	mu        sync.RWMutex
	publisher pubsubmodel.Publisher
	channels  map[string]pubsubmodel.Channel
}

func (s *mutablePublisherStore) GetPublisher(context.Context, string) (pubsubmodel.Publisher, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.publisher, nil
}

func (s *mutablePublisherStore) GetChannel(_ context.Context, name string) (pubsubmodel.Channel, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, ok := s.channels[name]
	if !ok {
		return pubsubmodel.Channel{}, pubsubmodel.ErrNotFound
	}
	return channel, nil
}

func (s *mutablePublisherStore) move(channel string) {
	s.mu.Lock()
	s.publisher.Channel = channel
	s.mu.Unlock()
}

type blockingPublisherProvider struct {
	mu       sync.Mutex
	channels []string
	started  chan struct{}
	release  chan struct{}
}

func (p *blockingPublisherProvider) EnsureActive(_ context.Context, _ pubsubmodel.Publisher, channel pubsubmodel.Channel) error {
	p.mu.Lock()
	p.channels = append(p.channels, channel.Name)
	call := len(p.channels)
	p.mu.Unlock()
	if call == 1 {
		close(p.started)
	}
	<-p.release
	return nil
}

func (p *blockingPublisherProvider) observedChannels() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.channels...)
}

func TestPublisherReconcilerSerializesAndReReadsConcurrentSameIdentity(t *testing.T) {
	store := &mutablePublisherStore{
		publisher: pubsubmodel.Publisher{ID: "stonks-live", Channel: "market:quotes", Type: "stonks"},
		channels: map[string]pubsubmodel.Channel{
			"market:quotes": {Name: "market:quotes"},
			"market:alt":    {Name: "market:alt"},
		},
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

	store.move("market:alt")
	go func() { second <- reconciler.Reconcile(context.Background(), "stonks-live") }()
	time.Sleep(20 * time.Millisecond)
	if got := provider.observedChannels(); len(got) != 1 || got[0] != "market:quotes" {
		t.Fatalf("provider entered before first reconcile completed: %v", got)
	}

	close(provider.release)
	if err := <-first; err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("serialized reconcile: %v", err)
	}
	if got := provider.observedChannels(); len(got) != 2 || got[0] != "market:quotes" || got[1] != "market:alt" {
		t.Fatalf("provider observed channels %v, want [market:quotes market:alt]", got)
	}
}
