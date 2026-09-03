package pubsubruntime

import (
	"context"
	"errors"
	"strings"
	"sync"
)

// SubscriptionController translates the operator-level activate/shutdown
// operation into Runtime handle management. It owns activation lifetime so an
// HTTP request context ending does not tear down a successfully activated
// subscription.
type SubscriptionController struct {
	runtime *Runtime
	ctx     context.Context
	cancel  context.CancelFunc

	mu     sync.Mutex
	active map[string]*SubscriberHandle
	closed bool
}

func NewSubscriptionController(runtime *Runtime) (*SubscriptionController, error) {
	if runtime == nil {
		return nil, errors.New("runtime is required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &SubscriptionController{
		runtime: runtime,
		ctx:     ctx,
		cancel:  cancel,
		active:  make(map[string]*SubscriberHandle),
	}, nil
}

func (c *SubscriptionController) ActivateSubscription(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("subscriber id is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("subscription controller is closed")
	}
	if _, ok := c.active[id]; ok {
		return nil
	}

	subscriber, err := c.runtime.store.GetSubscriber(ctx, id)
	if err != nil {
		return err
	}
	if _, err := c.runtime.store.GetChannel(ctx, subscriber.Channel); err != nil {
		return err
	}
	if !c.runtime.Active(subscriber.Channel) {
		if _, err := c.runtime.Activate(c.ctx, subscriber.Channel, nil); err != nil {
			return err
		}
	}
	handle, err := c.runtime.AttachSubscriber(ctx, id)
	if err != nil {
		return err
	}
	c.active[id] = handle
	return nil
}

func (c *SubscriptionController) ShutdownSubscription(_ context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("subscriber id is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("subscription controller is closed")
	}
	handle, ok := c.active[id]
	if !ok {
		return nil
	}
	delete(c.active, id)
	handle.Close()
	return nil
}

// Close is idempotent. It detaches controller-owned Subscriber handlers and
// cancels Channel activations that were started with the controller lifetime.
func (c *SubscriptionController) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	handles := make([]*SubscriberHandle, 0, len(c.active))
	for _, handle := range c.active {
		handles = append(handles, handle)
	}
	c.active = make(map[string]*SubscriberHandle)
	c.cancel()
	c.mu.Unlock()

	for _, handle := range handles {
		handle.Close()
	}
}
