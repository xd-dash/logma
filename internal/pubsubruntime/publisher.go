package pubsubruntime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/xd-dash/logma/internal/pubsubmodel"
)

// PublisherStore is the control-plane view required to reconcile a Publisher.
type PublisherStore interface {
	GetPublisher(context.Context, string) (pubsubmodel.Publisher, error)
	GetChannel(context.Context, string) (pubsubmodel.Channel, error)
}

// ChannelActivator is intentionally separate from the graph store credential:
// channel activation requires transport SUBSCRIBE authority while graph storage
// does not.
type ChannelActivator interface {
	Active(string) bool
	Activate(context.Context, string, func(string)) (*Handle, error)
}

// PublisherProvider adapts one producer type (stonks, news, socket, etc.) to
// the generic Logma Publisher resource. EnsureActive must be idempotent for a
// stable Publisher identity/configuration.
type PublisherProvider interface {
	EnsureActive(context.Context, pubsubmodel.Publisher, pubsubmodel.Channel) error
}

type PublisherRegistry struct {
	mu        sync.RWMutex
	providers map[string]PublisherProvider
}

func NewPublisherRegistry() *PublisherRegistry {
	return &PublisherRegistry{providers: make(map[string]PublisherProvider)}
}

func (r *PublisherRegistry) Register(kind string, provider PublisherProvider) error {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		return errors.New("publisher provider type is required")
	}
	if provider == nil {
		return errors.New("publisher provider is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.providers[kind]; exists {
		return fmt.Errorf("publisher provider %q is already registered", kind)
	}
	r.providers[kind] = provider
	return nil
}

func (r *PublisherRegistry) provider(kind string) (PublisherProvider, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	provider, ok := r.providers[strings.TrimSpace(kind)]
	return provider, ok
}

type PublisherReconciler struct {
	store     PublisherStore
	channels  ChannelActivator
	providers *PublisherRegistry
}

func NewPublisherReconciler(store PublisherStore, channels ChannelActivator, providers *PublisherRegistry) (*PublisherReconciler, error) {
	if store == nil {
		return nil, errors.New("publisher store is required")
	}
	if channels == nil {
		return nil, errors.New("channel activator is required")
	}
	if providers == nil {
		return nil, errors.New("publisher provider registry is required")
	}
	return &PublisherReconciler{store: store, channels: channels, providers: providers}, nil
}

func (r *PublisherReconciler) Reconcile(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("publisher id is required")
	}
	publisher, err := r.store.GetPublisher(ctx, id)
	if err != nil {
		return err
	}
	channel, err := r.store.GetChannel(ctx, publisher.Channel)
	if err != nil {
		return err
	}
	provider, ok := r.providers.provider(publisher.Type)
	if !ok {
		return fmt.Errorf("publisher provider %q is not registered", publisher.Type)
	}
	if !r.channels.Active(channel.Name) {
		if _, err := r.channels.Activate(ctx, channel.Name, nil); err != nil {
			return fmt.Errorf("activate publisher channel %s: %w", channel.Name, err)
		}
	}
	if err := provider.EnsureActive(ctx, publisher, channel); err != nil {
		return fmt.Errorf("activate publisher %s with provider %s: %w", publisher.ID, publisher.Type, err)
	}
	return nil
}
