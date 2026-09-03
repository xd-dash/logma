package pubsubruntime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/internal/pubsubmodel"
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

type Runtime struct {
	client      *redis.Client
	store       ResourceStore
	subscribe   subscribeFunc
	sendWebhook webhookSender

	mu     sync.Mutex
	active map[string]*channelActivation
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

func (a *channelActivation) removeHandler(id string, token uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	entry, ok := a.handlers[id]
	if !ok || entry.token != token {
		return false
	}
	delete(a.handlers, id)
	return true
}

type Handle struct {
	runtime    *Runtime
	channel    string
	activation *channelActivation
	sub        Subscriber
}

type SubscriberHandle struct {
	runtime      *Runtime
	channel      string
	activation   *channelActivation
	subscriberID string
	token        uint64
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

func newWithDependencies(client *redis.Client, store ResourceStore, subscribe subscribeFunc, sendWebhook webhookSender) *Runtime {
	return &Runtime{
		client:      client,
		store:       store,
		subscribe:   subscribe,
		sendWebhook: sendWebhook,
		active:      make(map[string]*channelActivation),
	}
}

func (r *Runtime) Activate(ctx context.Context, name string, onMessage func(string)) (*Handle, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, errors.New("channel name is required")
	}
	if _, err := r.store.GetChannel(ctx, name); err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if activation, ok := r.active[name]; ok {
		if onMessage != nil {
			activation.putHandler("__channel__", onMessage)
		}
		return &Handle{runtime: r, channel: name, activation: activation, sub: activation.subscriber}, nil
	}

	subCtx, cancel := context.WithCancel(ctx)
	activation := &channelActivation{
		ctx:      subCtx,
		cancel:   cancel,
		handlers: make(map[string]handlerEntry),
	}
	if onMessage != nil {
		activation.putHandler("__channel__", onMessage)
	}
	activation.subscriber = r.subscribe(subCtx, r.client, name, activation.dispatch)
	r.active[name] = activation

	go r.removeWhenStopped(name, activation)
	return &Handle{runtime: r, channel: name, activation: activation, sub: activation.subscriber}, nil
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
	activation, ok := r.active[subscriber.Channel]
	if !ok {
		r.mu.Unlock()
		return nil, fmt.Errorf("channel %s is not active", subscriber.Channel)
	}
	handler := func(payload string) {
		for _, url := range urls {
			if err := r.sendWebhook(activation.ctx, url, payload); err != nil && !errors.Is(err, context.Canceled) {
				fmt.Printf("Subscriber %s webhook %s failed: %v\n", subscriber.ID, url, err)
			}
		}
	}
	token := activation.putHandler(subscriber.ID, handler)
	r.mu.Unlock()

	return &SubscriberHandle{
		runtime:      r,
		channel:      subscriber.Channel,
		activation:   activation,
		subscriberID: subscriber.ID,
		token:        token,
	}, nil
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

func (r *Runtime) detachSubscriber(channel string, activation *channelActivation, subscriberID string, token uint64) bool {
	r.mu.Lock()
	current, ok := r.active[channel]
	if !ok || current != activation {
		r.mu.Unlock()
		return false
	}
	removed := activation.removeHandler(subscriberID, token)
	r.mu.Unlock()
	return removed
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
func (h *Handle) Close() bool              { return h.runtime.deactivateActivation(h.channel, h.activation) }

func (h *SubscriberHandle) Close() bool {
	return h.runtime.detachSubscriber(h.channel, h.activation, h.subscriberID, h.token)
}
