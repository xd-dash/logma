package pubsubruntime

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/internal/pubsubmodel"
	serverlesspubsub "github.com/xd-dash/logma/serverless/pubsub"
)

// ChannelStore is the persisted control-plane boundary required to activate a
// Channel. Runtime activation never manufactures a Channel implicitly.
type ChannelStore interface {
	GetChannel(context.Context, string) (pubsubmodel.Channel, error)
}

// Subscriber is the small runtime surface needed from the Redis listener.
type Subscriber interface {
	Ready() <-chan struct{}
	Stopped() <-chan struct{}
	LastError() error
}

type subscribeFunc func(context.Context, *redis.Client, string, func(string)) Subscriber

// Runtime activates persisted Logma Channel resources independently of
// Callback and Subscriber resources. A Channel with no callbacks is therefore
// still a valid active Redis listener.
type Runtime struct {
	client    *redis.Client
	store     ChannelStore
	subscribe subscribeFunc

	mu     sync.Mutex
	active map[string]*channelActivation
}

type channelActivation struct {
	cancel     context.CancelFunc
	subscriber Subscriber
}

// Handle is an instance-local activation handle. Closing it deactivates only
// this Runtime's listener; it does not delete the persisted Channel resource.
type Handle struct {
	runtime *Runtime
	channel string
	sub     Subscriber
}

func New(client *redis.Client, store ChannelStore) (*Runtime, error) {
	if client == nil {
		return nil, errors.New("redis client is required")
	}
	if store == nil {
		return nil, errors.New("channel store is required")
	}
	return newWithSubscriber(client, store, func(ctx context.Context, client *redis.Client, channel string, onMessage func(string)) Subscriber {
		return serverlesspubsub.Subscribe(ctx, client, channel, onMessage)
	}), nil
}

func newWithSubscriber(client *redis.Client, store ChannelStore, subscribe subscribeFunc) *Runtime {
	return &Runtime{
		client:    client,
		store:     store,
		subscribe: subscribe,
		active:    make(map[string]*channelActivation),
	}
}

// Activate verifies that the Channel exists in the persisted resource graph,
// then starts one instance-local Redis listener. onMessage may be nil when the
// caller only needs an active Channel with zero callbacks.
func (r *Runtime) Activate(ctx context.Context, name string, onMessage func(string)) (*Handle, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, errors.New("channel name is required")
	}
	if _, err := r.store.GetChannel(ctx, name); err != nil {
		return nil, err
	}
	if onMessage == nil {
		onMessage = func(string) {}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if activation, ok := r.active[name]; ok {
		return &Handle{runtime: r, channel: name, sub: activation.subscriber}, nil
	}

	subCtx, cancel := context.WithCancel(ctx)
	sub := r.subscribe(subCtx, r.client, name, onMessage)
	r.active[name] = &channelActivation{cancel: cancel, subscriber: sub}

	go r.removeWhenStopped(name, sub)
	return &Handle{runtime: r, channel: name, sub: sub}, nil
}

func (r *Runtime) removeWhenStopped(name string, sub Subscriber) {
	<-sub.Stopped()
	r.mu.Lock()
	defer r.mu.Unlock()
	if activation, ok := r.active[name]; ok && activation.subscriber == sub {
		delete(r.active, name)
	}
}

func (r *Runtime) Deactivate(name string) bool {
	name = strings.TrimSpace(name)
	r.mu.Lock()
	activation, ok := r.active[name]
	if ok {
		delete(r.active, name)
	}
	r.mu.Unlock()
	if !ok {
		return false
	}
	activation.cancel()
	return true
}

func (r *Runtime) Active(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.active[strings.TrimSpace(name)]
	return ok
}

func (h *Handle) Ready() <-chan struct{}   { return h.sub.Ready() }
func (h *Handle) Stopped() <-chan struct{} { return h.sub.Stopped() }
func (h *Handle) LastError() error         { return h.sub.LastError() }
func (h *Handle) Close() bool              { return h.runtime.Deactivate(h.channel) }
