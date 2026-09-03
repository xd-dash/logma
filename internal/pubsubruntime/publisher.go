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

type publisherOperation struct {
	done chan struct{}
	err  error
}

type PublisherReconciler struct {
	store     PublisherStore
	providers *PublisherRegistry
	ctx       context.Context
	cancel    context.CancelFunc

	mu      sync.Mutex
	pending map[string]*publisherOperation
	closed  bool
}

// NewPublisherReconciler deliberately does not own a generic Channel listener.
// A durable Publisher must reference an existing logical Channel, but starting
// an empty Redis SUBSCRIBE listener does not create delivery durability and is
// not a producer prerequisite. Concrete providers own any stronger readiness
// contract they actually require.
func NewPublisherReconciler(store PublisherStore, providers *PublisherRegistry) (*PublisherReconciler, error) {
	if store == nil {
		return nil, errors.New("publisher store is required")
	}
	if providers == nil {
		return nil, errors.New("publisher provider registry is required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &PublisherReconciler{
		store:     store,
		providers: providers,
		ctx:       ctx,
		cancel:    cancel,
		pending:   make(map[string]*publisherOperation),
	}, nil
}

// Reconcile serializes work for the same Publisher identity. A caller arriving
// behind successful work re-reads current desired state instead of assuming the
// earlier command reconciled the declaration it now observes. Providers remain
// idempotent across time; different Publisher identities remain independent.
func (r *PublisherReconciler) Reconcile(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("publisher id is required")
	}

	for {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return errors.New("publisher reconciler is closed")
		}
		if pending, ok := r.pending[id]; ok {
			done := pending.done
			r.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-r.ctx.Done():
				return errors.New("publisher reconciler is closed")
			case <-done:
				if pending.err != nil {
					return pending.err
				}
				continue
			}
		}
		pending := &publisherOperation{done: make(chan struct{})}
		r.pending[id] = pending
		r.mu.Unlock()

		err := r.reconcile(ctx, id)

		r.mu.Lock()
		delete(r.pending, id)
		pending.err = err
		close(pending.done)
		r.mu.Unlock()
		return err
	}
}

func (r *PublisherReconciler) reconcile(ctx context.Context, id string) error {
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

	if err := provider.EnsureActive(runtimeCtx, publisher, channel); err != nil {
		return fmt.Errorf("activate publisher %s with provider %s: %w", publisher.ID, publisher.Type, err)
	}
	if err := runtimeCtx.Err(); err != nil {
		return fmt.Errorf("publisher reconciler closed during activation: %w", err)
	}
	return nil
}

// Close ends reconciler-owned producer lifetime. It is intentionally separate
// from any individual reconcile request so a successful HTTP command does not
// become the lifetime owner of the producer it started.
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
