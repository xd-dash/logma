package pubsubruntime

import (
	"context"
	"errors"
	"strings"
	"sync"
)

type subscriptionOperation struct {
	done chan struct{}
	err  error
}

// SubscriptionController translates the operator-level activate/shutdown
// operation into Runtime handle management. It owns activation lifetime so an
// HTTP request context ending does not tear down a successfully activated
// subscription. Operations for different Subscriber identities are independent;
// only concurrent operations for the same identity are serialized.
type SubscriptionController struct {
	runtime *Runtime
	ctx     context.Context
	cancel  context.CancelFunc

	mu      sync.Mutex
	active  map[string]*SubscriberHandle
	pending map[string]*subscriptionOperation
	closed  bool
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
		pending: make(map[string]*subscriptionOperation),
	}, nil
}

// ActivateSubscription is ensure-current rather than merely ensure-present.
// Repeated activation re-reads the Subscriber and Callback declarations and
// installs the new handler before detaching the old one. Failed reconciliation
// therefore leaves the previously active handler intact.
func (c *SubscriptionController) ActivateSubscription(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("subscriber id is required")
	}

	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return errors.New("subscription controller is closed")
		}
		if pending, ok := c.pending[id]; ok {
			done := pending.done
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-done:
				if pending.err != nil {
					return pending.err
				}
				// This caller requested its own reconciliation. Once the earlier
				// same-ID operation completes successfully, loop and re-read the
				// current declaration rather than inheriting an older snapshot.
				continue
			}
		}
		pending := &subscriptionOperation{done: make(chan struct{})}
		c.pending[id] = pending
		c.mu.Unlock()

		newHandle, err := c.activate(ctx, id)

		c.mu.Lock()
		oldHandle := c.active[id]
		if err == nil && c.closed {
			err = errors.New("subscription controller is closed")
		}
		if err == nil {
			c.active[id] = newHandle
		}
		delete(c.pending, id)
		pending.err = err
		close(pending.done)
		c.mu.Unlock()

		if err != nil {
			if newHandle != nil {
				newHandle.Close()
			}
			return err
		}
		if oldHandle != nil && oldHandle != newHandle {
			oldHandle.Close()
		}
		return nil
	}
}

func (c *SubscriptionController) activate(ctx context.Context, id string) (*SubscriberHandle, error) {
	subscriber, err := c.runtime.store.GetSubscriber(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, err := c.runtime.store.GetChannel(ctx, subscriber.Channel); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !c.runtime.Active(subscriber.Channel) {
		if _, err := c.runtime.Activate(c.ctx, subscriber.Channel, nil); err != nil {
			return nil, err
		}
	}
	return c.runtime.AttachSubscriber(ctx, id)
}

// ShutdownSubscription is idempotent for a declared but already-inactive
// Subscription. A missing durable declaration remains distinguishable as
// ErrNotFound so weak Group execution can report it as missing rather than
// falsely claiming a completed shutdown.
func (c *SubscriptionController) ShutdownSubscription(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("subscriber id is required")
	}

	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return errors.New("subscription controller is closed")
		}
		if pending, ok := c.pending[id]; ok {
			done := pending.done
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-done:
				continue
			}
		}
		handle, active := c.active[id]
		if active {
			delete(c.active, id)
		}
		c.mu.Unlock()

		if active {
			handle.Close()
			return nil
		}
		if _, err := c.runtime.store.GetSubscriber(ctx, id); err != nil {
			return err
		}
		return nil
	}
}

// Close is idempotent. It detaches controller-owned Subscriber handlers and
// cancels Channel activations that were started with the controller lifetime.
// In-flight activations observe the closed state before publishing their handle.
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
