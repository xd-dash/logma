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
	ctx              context.Context
	cancel           context.CancelFunc

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

func (a *channelActivation) hasHandler(id string, token uint64) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	entry, ok := a.handlers[id]
	return ok && entry.token == token
}

func (a *channelActivation) handlerCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.handlers)
}

type Handle struct {
	runtime    *Runtime
	channel    string
	activation *channelActivation
	sub        Subscriber
}

// listenerLease is an internal generation capability. It pins one exact
// channelActivation while an operator command performs readiness and attachment
// work. Name-based lookup is deliberately not used after acquisition.
type listenerLease struct {
	runtime    *Runtime
	channel    string
	activation *channelActivation
	once       sync.Once
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
	ctx, cancel := context.WithCancel(context.Background())
	return &Runtime{
		client:    client,
		store:     store,
		subscribe: subscribe,
		delivery:  newDeliveryDispatcher(sendWebhook),
		ctx:       ctx,
		cancel:    cancel,
		active:    make(map[string]*channelActivation),
		transportAddress: func(channel string) (string, error) {
			return channel, nil
		},
	}
}

// Activate remains the low-level explicit Channel activation/deactivation
// primitive. Handle.Close retains its historical force-deactivate semantics.
// The caller context bounds declaration lookup only; once created, the listener
// is owned by Runtime rather than by whichever request happened to create it.
func (r *Runtime) Activate(ctx context.Context, name string, onMessage func(string)) (*Handle, error) {
	name = strings.TrimSpace(name)
	activation, err := r.ensureListener(ctx, name)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	current, ok := r.active[name]
	if r.closed || !ok || current != activation || activation.ctx.Err() != nil {
		r.mu.Unlock()
		return nil, fmt.Errorf("channel %s listener generation became unavailable during activation", name)
	}
	if onMessage != nil {
		activation.putHandler("__channel__", onMessage)
	}
	r.mu.Unlock()
	return &Handle{runtime: r, channel: name, activation: activation, sub: activation.subscriber}, nil
}

func (r *Runtime) acquireListener(ctx context.Context, name string) (*listenerLease, error) {
	name = strings.TrimSpace(name)
	activation, err := r.ensureListener(ctx, name)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	current, ok := r.active[name]
	if !ok || current != activation || r.closed || activation.ctx.Err() != nil {
		r.mu.Unlock()
		return nil, errors.New("listener became unavailable during acquisition")
	}
	activation.leases++
	r.mu.Unlock()
	return &listenerLease{runtime: r, channel: name, activation: activation}, nil
}

func (r *Runtime) ensureListener(ctx context.Context, name string) (*channelActivation, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, errors.New("channel name is required")
	}
	if _, err := r.store.GetChannel(ctx, name); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
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
		return activation, nil
	}

	// Listener lifetime is Runtime-owned. Request/controller contexts may decide
	// whether their command waits, but they cannot become transport owners.
	subCtx, cancel := context.WithCancel(r.ctx)
	activation := &channelActivation{ctx: subCtx, cancel: cancel, handlers: make(map[string]handlerEntry)}
	activation.subscriber = r.subscribe(subCtx, r.client, transportName, activation.dispatch)
	r.active[name] = activation
	go r.removeWhenStopped(name, activation)
	return activation, nil
}

func waitReadyActivation(ctx context.Context, channel string, activation *channelActivation) error {
	stoppedError := func() error {
		if err := activation.subscriber.LastError(); err != nil {
			return fmt.Errorf("channel %s stopped before ready: %w", channel, err)
		}
		return fmt.Errorf("channel %s stopped before ready", channel)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-activation.ctx.Done():
		if err := activation.subscriber.LastError(); err != nil {
			return fmt.Errorf("channel %s listener generation canceled before ready: %w", channel, err)
		}
		return fmt.Errorf("channel %s listener generation canceled before ready", channel)
	case <-activation.subscriber.Stopped():
		return stoppedError()
	case <-activation.subscriber.Ready():
		select {
		case <-activation.ctx.Done():
			return fmt.Errorf("channel %s listener generation canceled at readiness boundary", channel)
		case <-activation.subscriber.Stopped():
			return stoppedError()
		default:
			return nil
		}
	}
}

func (r *Runtime) generationCurrent(channel string, activation *channelActivation) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.active[channel]
	return !r.closed && ok && current == activation && activation.ctx.Err() == nil
}

func (r *Runtime) WaitReady(ctx context.Context, name string) error {
	name = strings.TrimSpace(name)
	r.mu.Lock()
	activation, ok := r.active[name]
	r.mu.Unlock()
	if !ok {
		return fmt.Errorf("channel %s is not active", name)
	}
	if err := waitReadyActivation(ctx, name, activation); err != nil {
		return err
	}
	if !r.generationCurrent(name, activation) {
		return fmt.Errorf("channel %s listener generation changed before readiness completed", name)
	}
	return nil
}

func (r *Runtime) materializeSubscriber(ctx context.Context, id string) (pubsubmodel.Subscriber, []string, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return pubsubmodel.Subscriber{}, nil, errors.New("subscriber id is required")
	}
	subscriber, err := r.store.GetSubscriber(ctx, id)
	if err != nil {
		return pubsubmodel.Subscriber{}, nil, err
	}
	urls := make([]string, 0)
	for _, callbackID := range subscriber.CallbackIDs {
		callback, err := r.store.GetCallback(ctx, callbackID)
		if err != nil {
			return pubsubmodel.Subscriber{}, nil, err
		}
		if callback.Type != pubsubmodel.CallbackWebhook || callback.Webhook == nil {
			return pubsubmodel.Subscriber{}, nil, fmt.Errorf("callback %s type %q is not supported by Subscriber runtime", callback.ID, callback.Type)
		}
		urls = append(urls, callback.Webhook.URLs()...)
	}
	return subscriber, urls, nil
}

func (r *Runtime) attachSubscriberTo(ctx context.Context, id, channel string, activation *channelActivation) (*SubscriberHandle, error) {
	subscriber, urls, err := r.materializeSubscriber(ctx, id)
	if err != nil {
		return nil, err
	}
	if subscriber.Channel != channel {
		return nil, fmt.Errorf("subscriber %s moved from channel %s to %s during activation", subscriber.ID, channel, subscriber.Channel)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.New("runtime is closed")
	}
	current, ok := r.active[channel]
	if !ok || current != activation {
		r.mu.Unlock()
		return nil, fmt.Errorf("channel %s listener generation changed before subscriber attachment", channel)
	}
	if err := activation.ctx.Err(); err != nil {
		r.mu.Unlock()
		return nil, fmt.Errorf("channel %s listener generation is stopping: %w", channel, err)
	}
	subscriberCtx, cancel := context.WithCancel(activation.ctx)
	handler := func(payload string) {
		for _, url := range urls {
			r.delivery.dispatch(deliveryJob{ctx: subscriberCtx, subscriberID: subscriber.ID, url: url, payload: payload})
		}
	}
	token := activation.putHandler(subscriber.ID, handler)
	r.mu.Unlock()

	return &SubscriberHandle{runtime: r, channel: channel, activation: activation, subscriberID: subscriber.ID, token: token, cancel: cancel}, nil
}

func (r *Runtime) AttachSubscriber(ctx context.Context, id string) (*SubscriberHandle, error) {
	subscriber, _, err := r.materializeSubscriber(ctx, id)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	activation, ok := r.active[subscriber.Channel]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("channel %s is not active", subscriber.Channel)
	}
	return r.attachSubscriberTo(ctx, id, subscriber.Channel, activation)
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

func (r *Runtime) deactivateActivation(channel string, activation *channelActivation) bool {
	r.mu.Lock()
	current, ok := r.active[channel]
	if !ok || current != activation {
		r.mu.Unlock()
		return false
	}
	delete(r.active, channel)
	r.mu.Unlock()
	activation.cancel()
	return true
}

func (r *Runtime) releaseLease(channel string, activation *channelActivation) bool {
	r.mu.Lock()
	current, ok := r.active[channel]
	if !ok || current != activation || activation.leases == 0 {
		r.mu.Unlock()
		return false
	}
	activation.leases--
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

func (r *Runtime) subscriberHandleCurrent(handle *SubscriberHandle) bool {
	if handle == nil {
		return false
	}
	r.mu.Lock()
	current, ok := r.active[handle.channel]
	if !ok || current != handle.activation || r.closed || handle.activation.ctx.Err() != nil {
		r.mu.Unlock()
		return false
	}
	active := handle.activation.hasHandler(handle.subscriberID, handle.token)
	r.mu.Unlock()
	return active
}

func (r *Runtime) Active(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	activation, ok := r.active[strings.TrimSpace(name)]
	return ok && activation.ctx.Err() == nil
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
	r.cancel()
	r.mu.Unlock()
	for _, activation := range activations {
		activation.cancel()
	}
	r.delivery.close()
}

func (h *Handle) Ready() <-chan struct{}   { return h.sub.Ready() }
func (h *Handle) Stopped() <-chan struct{} { return h.sub.Stopped() }
func (h *Handle) LastError() error         { return h.sub.LastError() }
func (h *Handle) Close() bool              { return h.runtime.deactivateActivation(h.channel, h.activation) }

func (l *listenerLease) WaitReady(ctx context.Context) error {
	if !l.runtime.generationCurrent(l.channel, l.activation) {
		return fmt.Errorf("channel %s listener generation is no longer current", l.channel)
	}
	if err := waitReadyActivation(ctx, l.channel, l.activation); err != nil {
		return err
	}
	if !l.runtime.generationCurrent(l.channel, l.activation) {
		return fmt.Errorf("channel %s listener generation changed before readiness completed", l.channel)
	}
	return nil
}

func (l *listenerLease) AttachSubscriber(ctx context.Context, id string) (*SubscriberHandle, error) {
	return l.runtime.attachSubscriberTo(ctx, id, l.channel, l.activation)
}

func (l *listenerLease) Close() bool {
	released := false
	l.once.Do(func() {
		released = l.runtime.releaseLease(l.channel, l.activation)
	})
	return released
}

func (h *SubscriberHandle) current() bool {
	return h.runtime.subscriberHandleCurrent(h)
}

func (h *SubscriberHandle) Close() bool {
	h.cancel()
	return h.runtime.detachSubscriber(h.channel, h.activation, h.subscriberID, h.token)
}
