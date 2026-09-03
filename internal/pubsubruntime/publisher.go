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
// stable Publisher identity/configuration. The supplied context is owned by the
// reconciler runtime, not by the HTTP request that requested reconciliation.
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
	ctx       context.Context
	cancel    context.CancelFunc

	mu     sync.Mutex
	closed bool
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
	ctx, cancel := context.WithCancel(context.Background())
	return &PublisherReconciler{store: store, channels: channels, providers: providers, ctx: ctx, cancel: cancel}, nil
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
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return errors.New("publisher reconciler is closed")
	}
	runtimeCtx := r.ctx
	r.mu.Unlock()

	if !r.channels.Active(channel.Name) {
		if _, err := r.channels.Activate(runtimeCtx, channel.Name, nil); err != nil {
			return fmt.Errorf("activate publisher channel %s: %w", channel.Name, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := provider.EnsureActive(runtimeCtx, publisher, channel); err != nil {
		return fmt.Errorf("activate publisher %s with provider %s: %w", publisher.ID, publisher.Type, err)
	}
	return nil
}

// Close ends reconciler-owned producer/channel lifetime. It is intentionally
// separate from any individual reconcile request so a successful HTTP command
// does not become the lifetime owner of the producer it started.
func (r *PublisherReconciler) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.cancel()
	r.mu.Unlock()
}
