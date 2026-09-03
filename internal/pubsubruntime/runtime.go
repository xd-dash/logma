package pubsubruntime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/internal/pubsubmodel"
	"github.com/xd-dash/logma/serverless/keyspace"
	serverlesspubsub "github.com/xd-dash/logma/serverless/pubsub"
)

type ResourceStore interface {
	GetChannel(context.Context, string) (pubsubmodel.Channel, error)
	GetSubscriber(context.Context, string) (pubsubmodel.Subscriber, error)
	GetCallback(context.Context, string) (pubsubmodel.Callback, error)
}

type Subscriber interface {
	Ready() <-chan struct{}
	Stopped() <-chan struct{}
	LastError() error
}

type subscribeFunc func(context.Context, *redis.Client, string, func(string)) Subscriber
type webhookSender func(context.Context, string, string) error
type transportAddressFunc func(string) (string, error)

type Runtime struct {
	client           *redis.Client
	store            ResourceStore
	subscribe        subscribeFunc
	delivery         *deliveryDispatcher
	transportAddress transportAddressFunc

	mu     sync.Mutex
	active map[string]*channelActivation
	closed bool
}

type handlerEntry struct {
	token uint64
	fn    func(string)
}

type channelActivation struct {
	ctx        context.Context
	cancel     context.CancelFunc
	subscriber Subscriber

	mu        sync.RWMutex
	nextToken uint64
	handlers  map[string]handlerEntry
	leases    int
}

func (a *channelActivation) dispatch(payload string) {
	a.mu.RLock()
	handlers := make([]func(string), 0, len(a.handlers))
	for _, handler := range a.handlers {
		handlers = append(handlers, handler.fn)
	}
	a.mu.RUnlock()
	for _, handler := range handlers {
		handler(payload)
	}
}

func (a *channelActivation) putHandler(id string, handler func(string)) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nextToken++
	token := a.nextToken
	a.handlers[id] = handlerEntry{token: token, fn: handler}
	return token
}

func (a *channelActivation) removeHandler(id string, token uint64) (bool, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	entry, ok := a.handlers[id]
	if !ok || entry.token != token {
		return false, len(a.handlers)
	}
	delete(a.handlers, id)
	return true, len(a.handlers)
}

func (a *channelActivation) handlerCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.handlers)
}

type Handle struct {
	runtime      *Runtime
	channel      string
	activation   *channelActivation
	sub          Subscriber
	handlerID    string
	handlerToken uint64
	once         sync.Once
}

type SubscriberHandle struct {
	runtime      *Runtime
	channel      string
	activation   *channelActivation
	subscriberID string
	token        uint64
	cancel       context.CancelFunc
}

func New(client *redis.Client, store ResourceStore) (*Runtime, error) {
	if client == nil {
		return nil, errors.New("redis client is required")
	}
	if store == nil {
		return nil, errors.New("resource store is required")
	}
	return newWithDependencies(client, store, func(ctx context.Context, client *redis.Client, channel string, onMessage func(string)) Subscriber {
		return serverlesspubsub.Subscribe(ctx, client, channel, onMessage)
	}, postWebhook), nil
}

func NewScoped(client *redis.Client, store ResourceStore, scope string) (*Runtime, error) {
	runtime, err := New(client, store)
	if err != nil {
		return nil, err
	}
	parsedScope, err := keyspace.ParseScope(strings.TrimSpace(scope))
	if err != nil {
		runtime.Close()
		return nil, err
	}
	runtime.transportAddress = func(channel string) (string, error) {
		return keyspace.LogmaPubSubTransportChannel(parsedScope, channel)
	}
	return runtime, nil
}

func newWithDependencies(client *redis.Client, store ResourceStore, subscribe subscribeFunc, sendWebhook webhookSender) *Runtime {
	return &Runtime{
		client:    client,
		store:     store,
		subscribe: subscribe,
		delivery:  newDeliveryDispatcher(sendWebhook),
		active:    make(map[string]*channelActivation),
		transportAddress: func(channel string) (string, error) {
			return channel, nil
		},
	}
}

// Activate acquires a temporary lease on a shared Channel listener. Callers
// release that lease with Handle.Close. The listener remains alive while any
// lease or runtime handler still needs it, so concurrent activation operations
// cannot tear down one another's empty-but-in-progress listener.
func (r *Runtime) Activate(ctx context.Context, name string, onMessage func(string)) (*Handle, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, errors.New("channel name is required")
	}
	if _, err := r.store.GetChannel(ctx, name); err != nil {
		return nil, err
	}
	transportName, err := r.transportAddress(name)
	if err != nil {
		return nil, fmt.Errorf("resolve channel transport address %s: %w", name, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("runtime is closed")
	}
	if activation, ok := r.active[name]; ok {
		activation.leases++
		handle := &Handle{runtime: r, channel: name, activation: activation, sub: activation.subscriber}
		if onMessage != nil {
			handle.handlerID = "__channel__"
			handle.handlerToken = activation.putHandler(handle.handlerID, onMessage)
		}
		return handle, nil
	}

	subCtx, cancel := context.WithCancel(ctx)
	activation := &channelActivation{ctx: subCtx, cancel: cancel, handlers: make(map[string]handlerEntry), leases: 1}
	handle := &Handle{runtime: r, channel: name, activation: activation}
	if onMessage != nil {
		handle.handlerID = "__channel__"
		handle.handlerToken = activation.putHandler(handle.handlerID, onMessage)
	}
	activation.subscriber = r.subscribe(subCtx, r.client, transportName, activation.dispatch)
	handle.sub = activation.subscriber
	r.active[name] = activation
	go r.removeWhenStopped(name, activation)
	return handle, nil
}

func (r *Runtime) WaitReady(ctx context.Context, name string) error {
	name = strings.TrimSpace(name)
	r.mu.Lock()
	activation, ok := r.active[name]
	r.mu.Unlock()
	if !ok {
		return fmt.Errorf("channel %s is not active", name)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-activation.subscriber.Ready():
		return nil
	case <-activation.subscriber.Stopped():
		if err := activation.subscriber.LastError(); err != nil {
			return fmt.Errorf("channel %s stopped before ready: %w", name, err)
		}
		return fmt.Errorf("channel %s stopped before ready", name)
	}
}

func (r *Runtime) AttachSubscriber(ctx context.Context, id string) (*SubscriberHandle, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, errors.New("subscriber id is required")
	}
	subscriber, err := r.store.GetSubscriber(ctx, id)
	if err != nil {
		return nil, err
	}
	urls := make([]string, 0)
	for _, callbackID := range subscriber.CallbackIDs {
		callback, err := r.store.GetCallback(ctx, callbackID)
		if err != nil {
			return nil, err
		}
		if callback.Type != pubsubmodel.CallbackWebhook || callback.Webhook == nil {
			return nil, fmt.Errorf("callback %s type %q is not supported by Subscriber runtime", callback.ID, callback.Type)
		}
		urls = append(urls, callback.Webhook.URLs()...)
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.New("runtime is closed")
	}
	activation, ok := r.active[subscriber.Channel]
	if !ok {
		r.mu.Unlock()
		return nil, fmt.Errorf("channel %s is not active", subscriber.Channel)
	}
	subscriberCtx, cancel := context.WithCancel(activation.ctx)
	handler := func(payload string) {
		for _, url := range urls {
			r.delivery.dispatch(deliveryJob{ctx: subscriberCtx, subscriberID: subscriber.ID, url: url, payload: payload})
		}
	}
	token := activation.putHandler(subscriber.ID, handler)
	r.mu.Unlock()

	return &SubscriberHandle{runtime: r, channel: subscriber.Channel, activation: activation, subscriberID: subscriber.ID, token: token, cancel: cancel}, nil
}

func (r *Runtime) removeWhenStopped(name string, activation *channelActivation) {
	<-activation.subscriber.Stopped()
	r.mu.Lock()
	if current, ok := r.active[name]; ok && current == activation {
		delete(r.active, name)
	}
	r.mu.Unlock()
	activation.cancel()
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

func (r *Runtime) releaseHandle(channel string, activation *channelActivation, handlerID string, handlerToken uint64) bool {
	r.mu.Lock()
	current, ok := r.active[channel]
	if !ok || current != activation {
		r.mu.Unlock()
		return false
	}
	if handlerID != "" {
		activation.removeHandler(handlerID, handlerToken)
	}
	if activation.leases > 0 {
		activation.leases--
	}
	idle := activation.leases == 0 && activation.handlerCount() == 0
	if idle {
		delete(r.active, channel)
	}
	r.mu.Unlock()
	if idle {
		activation.cancel()
	}
	return true
}

func (r *Runtime) detachSubscriber(channel string, activation *channelActivation, subscriberID string, token uint64) bool {
	r.mu.Lock()
	current, ok := r.active[channel]
	if !ok || current != activation {
		r.mu.Unlock()
		return false
	}
	removed, remaining := activation.removeHandler(subscriberID, token)
	idle := removed && remaining == 0 && activation.leases == 0
	if idle {
		delete(r.active, channel)
	}
	r.mu.Unlock()
	if idle {
		activation.cancel()
	}
	return removed
}

func (r *Runtime) Active(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.active[strings.TrimSpace(name)]
	return ok
}

// Close owns runtime cleanup: all active transport listeners are canceled and
// bounded delivery workers are drained. Redis client ownership remains with the
// composition root that supplied the client.
func (r *Runtime) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	activations := make([]*channelActivation, 0, len(r.active))
	for _, activation := range r.active {
		activations = append(activations, activation)
	}
	r.active = make(map[string]*channelActivation)
	r.mu.Unlock()
	for _, activation := range activations {
		activation.cancel()
	}
	r.delivery.close()
}

func (h *Handle) Ready() <-chan struct{}   { return h.sub.Ready() }
func (h *Handle) Stopped() <-chan struct{} { return h.sub.Stopped() }
func (h *Handle) LastError() error         { return h.sub.LastError() }
func (h *Handle) Close() bool {
	released := false
	h.once.Do(func() {
		released = h.runtime.releaseHandle(h.channel, h.activation, h.handlerID, h.handlerToken)
	})
	return released
}
func (h *SubscriberHandle) Close() bool {
	h.cancel()
	return h.runtime.detachSubscriber(h.channel, h.activation, h.subscriberID, h.token)
}
