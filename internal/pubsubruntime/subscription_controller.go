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
// operation into Subscriber handle management. Runtime owns shared Channel
// listener lifetime; the controller owns only its Subscriber handles and the
// cancellation of commands currently executing through it.
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

func (c *SubscriptionController) operationContext(requestCtx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(requestCtx)
	stop := context.AfterFunc(c.ctx, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

// ActivateSubscription is ensure-current rather than merely ensure-present.
// Same-identity operations serialize. A caller arriving behind successful work
// re-reads current desired state instead of assuming the peer reconciled the
// declaration it now observes. Different identities remain independent.
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
			case <-c.ctx.Done():
				return errors.New("subscription controller is closed")
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
		preserveExisting := oldHandle != nil && oldHandle.current()
		c.mu.Unlock()

		operationCtx, cancelOperation := c.operationContext(ctx)
		newHandle, err := c.activate(operationCtx, id, preserveExisting)
		cancelOperation()

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
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// The lease is an exact listener-generation capability. From this point on,
	// readiness and attachment must operate through the lease rather than by
	// looking the Channel name up again.
	lease, err := c.runtime.acquireListener(ctx, subscriber.Channel)
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	if preserveExisting {
		// Keep the previous known-good handler until the replacement target has
		// reached its initial readiness boundary. If the leased generation is
		// force-deactivated or replaced, WaitReady fails rather than migrating to
		// another generation with the same logical Channel name.
		if err := lease.WaitReady(ctx); err != nil {
			return nil, err
		}
		newHandle, err := lease.AttachSubscriber(ctx, id)
		if err != nil {
			return nil, err
		}
		return newHandle, nil
	}

	// First activation has no known-good handler to preserve. Install before
	// waiting for the initial Redis acknowledgement because Redis may deliver as
	// soon as SUBSCRIBE is acknowledged. Failure detaches the staged handler;
	// releasing the final lease then removes an otherwise-idle listener.
	newHandle, err := lease.AttachSubscriber(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := lease.WaitReady(ctx); err != nil {
		newHandle.Close()
		return nil, err
	}
	return newHandle, nil
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
			case <-c.ctx.Done():
				return errors.New("subscription controller is closed")
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

// Close is idempotent. It cancels in-flight controller commands and detaches
// controller-owned Subscriber handles. Shared Channel listener lifetime remains
// Runtime-owned and is released only by the normal zero-handler/zero-lease rule,
// explicit force-deactivation, transport terminal failure, or Runtime.Close.
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
