package pubsub

import (
	"context"
	"sync"
	"sync/atomic"
)

const (
	sessionIdle int32 = iota
	sessionRunning
	sessionDone
)

type Session struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	state  atomic.Int32
	startOnce sync.Once
}

func NewSession() Session {
	ctx, cancel := context.WithCancel(context.Background())
	return Session{ctx: ctx, cancel: cancel, done: make(chan struct{})}
}

func (s *Session) Context() context.Context { return s.ctx }
func (s *Session) Claim() bool              { return s.state.CompareAndSwap(sessionIdle, sessionRunning) }
func (s *Session) Done() <-chan struct{}    { return s.done }
func (s *Session) Cancel()                  { s.cancel() }

func (s *Session) Begin(ctx context.Context, fn func()) {
	s.startOnce.Do(func() {
		defer s.state.Store(sessionDone)
		defer close(s.done)
		go func() {
			select {
			case <-ctx.Done():
				s.cancel()
			case <-s.ctx.Done():
			}
		}()
		fn()
	})
}
