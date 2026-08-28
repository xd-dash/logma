package pubsub

import (
	"context"
	"sync"
)

type Lifecycle interface {
	Start(ctx context.Context)
	Cancel()
	Done() <-chan struct{}
	Claim() bool
}

type Holder[T Lifecycle] struct {
	mu          sync.Mutex
	newFn       func() T
	cur         T
	initialized bool
}

func NewHolder[T Lifecycle](newFn func() T) *Holder[T] {
	return &Holder[T]{newFn: newFn}
}

func (h *Holder[T]) Claim() (T, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.initialized || isDone(h.cur) {
		h.cur = h.newFn()
		h.initialized = true
	}
	if h.cur.Claim() {
		return h.cur, true
	}
	var zero T
	return zero, false
}

func isDone(l Lifecycle) bool {
	select {
	case <-l.Done():
		return true
	default:
		return false
	}
}
