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
// Repeated activation re-reads the Subscriber and Callback declarations. When a
// replacement needs a new listener, the previous known-good handler remains
// installed until that listener has completed its initial SUBSCRIBE ACK. First
// activation installs its handler before waiting for the initial ACK so there is
// no ACK-before-handler window. Same-identity operations serialize, while
// different identities remain independent.
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
				continue
			}
		}
		pending := &subscriptionOperation{done: make(chan struct{})}
		c.pending[id] = pending
		oldHandle := c.active[id]
		c.mu.Unlock()

		newHandle, err := c.activate(ctx, id, oldHandle != nil)

		c.mu.Lock()
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

func (c *SubscriptionController) activate(ctx context.Context, id string, preserveExisting bool) (*SubscriberHandle, error) {
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

	var channelHandle *Handle
	if !c.runtime.Active(subscriber.Channel) {
		channelHandle, err = c.runtime.Activate(c.ctx, subscriber.Channel, nil)
		if err != nil {
			return nil, err
		}
	}

	if preserveExisting {
		// A reconciliation already has a known-good handler. If the target is a
		// newly-created listener (for example after a Channel move), keep the old
		// handler until that target receives its initial ACK. For an already-
		// established listener Ready is intentionally only historical initial
		// readiness; go-redis owns transparent reconnect/resubscribe afterward.
		if err := c.runtime.WaitReady(ctx, subscriber.Channel); err != nil {
			if channelHandle != nil {
				c.runtime.deactivateIfIdle(subscriber.Channel, channelHandle.activation)
			}
			return nil, err
		}
		return c.runtime.AttachSubscriber(ctx, id)
	}

	// First activation has no known-good handler to preserve. Install before
	// waiting for the initial Redis acknowledgement because Redis may deliver as
	// soon as SUBSCRIBE is acknowledged.
	newHandle, err := c.runtime.AttachSubscriber(ctx, id)
	if err != nil {
		if channelHandle != nil {
			c.runtime.deactivateIfIdle(subscriber.Channel, channelHandle.activation)
		}
		return nil, err
	}
	if err := c.runtime.WaitReady(ctx, subscriber.Channel); err != nil {
		newHandle.Close()
		return nil, err
	}
	return newHandle, nil
}

// ShutdownSubscription is idempotent for a declared but already-inactive
// Subscription. A missing durable declaration remains distinguishable as
// ErrNotFound so weak Group execution can report it as missing rather than
// falsely claiming a completed shutdown. Closing the last handler also releases
// the shared Redis listener; durable Channel existence remains independent.
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
// cancels Channel listeners that were started with the controller lifetime.
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
