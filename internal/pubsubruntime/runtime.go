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

// ResourceStore is the persisted control-plane boundary required by runtime
// Channel and Subscriber activation. Runtime state never manufactures these
// durable resources implicitly.
type ResourceStore interface {
	GetChannel(context.Context, string) (pubsubmodel.Channel, error)
	GetSubscriber(context.Context, string) (pubsubmodel.Subscriber, error)
	GetCallback(context.Context, string) (pubsubmodel.Callback, error)
}

// Subscriber is the small runtime surface needed from the Redis listener.
type Subscriber interface {
	Ready() <-chan struct{}
	Stopped() <-chan struct{}
	LastError() error
}

type subscribeFunc func(context.Context, *redis.Client, string, func(string)) Subscriber

type webhookSender func(context.Context, string, string) error

// Runtime activates persisted Logma Channel resources independently of
// Callback and Subscriber resources. A Channel with no callbacks is therefore
// still a valid active Redis listener. Subscriber attachments add delivery
// handlers to that single instance-local listener rather than opening another
// Redis subscription.
type Runtime struct {
	client      *redis.Client
	store       ResourceStore
	subscribe   subscribeFunc
	sendWebhook webhookSender

	mu     sync.Mutex
	active map[string]*channelActivation
}

type channelActivation struct {
	cancel     context.CancelFunc
	subscriber Subscriber

	mu       sync.RWMutex
	handlers map[string]func(string)
}

func (a *channelActivation) dispatch(payload string) {
	a.mu.RLock()
	handlers := make([]func(string), 0, len(a.handlers))
	for _, handler := range a.handlers {
		handlers = append(handlers, handler)
	}
	a.mu.RUnlock()

	for _, handler := range handlers {
		handler(payload)
	}
}

func (a *channelActivation) putHandler(id string, handler func(string)) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, existed := a.handlers[id]
	a.handlers[id] = handler
	return !existed
}

func (a *channelActivation) removeHandler(id string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.handlers[id]; !ok {
		return false
	}
	delete(a.handlers, id)
	return true
}

// Handle is an instance-local Channel activation handle. Closing it
// deactivates only this Runtime's listener; it does not delete the persisted
// Channel resource.
type Handle struct {
	runtime *Runtime
	channel string
	sub     Subscriber
}

// SubscriberHandle is an instance-local delivery attachment. Closing it
// removes runtime delivery only; it does not delete the persisted Subscriber
// or Callback resources.
type SubscriberHandle struct {
	runtime      *Runtime
	channel      string
	subscriberID string
}

func New(client *redis.Client, store ResourceStore) (*Runtime, error) {
	if client == nil {
		return nil, errors.New("redis client is required")
	}
	if store == nil {
		return nil, errors.New("resource store is required")
	}
	return newWithDependencies(
		client,
		store,
		func(ctx context.Context, client *redis.Client, channel string, onMessage func(string)) Subscriber {
			return serverlesspubsub.Subscribe(ctx, client, channel, onMessage)
		},
		postWebhook,
	), nil
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

	r.mu.Lock()
	defer r.mu.Unlock()
	if activation, ok := r.active[name]; ok {
		return &Handle{runtime: r, channel: name, sub: activation.subscriber}, nil
	}

	subCtx, cancel := context.WithCancel(ctx)
	activation := &channelActivation{
		cancel:   cancel,
		handlers: make(map[string]func(string)),
	}
	if onMessage != nil {
		activation.handlers["__channel__"] = onMessage
	}
	activation.subscriber = r.subscribe(subCtx, r.client, name, activation.dispatch)
	r.active[name] = activation

	go r.removeWhenStopped(name, activation.subscriber)
	return &Handle{runtime: r, channel: name, sub: activation.subscriber}, nil
}

// AttachSubscriber resolves one persisted Subscriber and its Callback
// resources, then attaches its delivery behavior to an already-active Channel.
// This first runtime slice supports webhook callbacks only.
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
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("channel %s is not active", subscriber.Channel)
	}

	handler := func(payload string) {
		for _, url := range urls {
			if err := r.sendWebhook(context.Background(), url, payload); err != nil {
				fmt.Printf("Subscriber %s webhook %s failed: %v\n", subscriber.ID, url, err)
			}
		}
	}
	activation.putHandler(subscriber.ID, handler)
	return &SubscriberHandle{runtime: r, channel: subscriber.Channel, subscriberID: subscriber.ID}, nil
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

func (r *Runtime) detachSubscriber(channel, subscriberID string) bool {
	r.mu.Lock()
	activation, ok := r.active[channel]
	r.mu.Unlock()
	if !ok {
		return false
	}
	return activation.removeHandler(subscriberID)
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

func (h *SubscriberHandle) Close() bool {
	return h.runtime.detachSubscriber(h.channel, h.subscriberID)
}
