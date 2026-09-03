package pubsub

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	reconnectMinDelay     = 500 * time.Millisecond
	reconnectMaxDelay     = 30 * time.Second
	redisOperationTimeout = 10 * time.Second
)

type Subscriber struct {
	stopped chan struct{}

	readyMu   sync.Mutex
	ready     chan struct{}
	connected bool

	errMu   sync.RWMutex
	lastErr error
}

func Subscribe(ctx context.Context, client *redis.Client, channel string, onMessage func(payload string)) *Subscriber {
	s := &Subscriber{stopped: make(chan struct{}), ready: make(chan struct{})}
	go s.run(ctx, client, channel, onMessage)
	return s
}

func (s *Subscriber) Stopped() <-chan struct{} { return s.stopped }

// Ready returns the readiness signal for the current Redis connection state.
// The returned channel is closed while Redis has acknowledged the current
// subscription and is replaced with a new open channel after that connection is
// lost. Callers that reconcile an already-running Subscriber can therefore wait
// for current readiness rather than relying on a one-time historical ACK.
func (s *Subscriber) Ready() <-chan struct{} {
	s.readyMu.Lock()
	defer s.readyMu.Unlock()
	return s.ready
}

// LastError returns the most recent Redis subscription acknowledgement error.
// It is diagnostic state only; Subscribe continues its bounded reconnect loop.
func (s *Subscriber) LastError() error {
	if s == nil {
		return nil
	}
	s.errMu.RLock()
	defer s.errMu.RUnlock()
	return s.lastErr
}

func (s *Subscriber) setLastError(err error) {
	s.errMu.Lock()
	s.lastErr = err
	s.errMu.Unlock()
}

func (s *Subscriber) markReady() {
	s.readyMu.Lock()
	defer s.readyMu.Unlock()
	if s.connected {
		return
	}
	close(s.ready)
	s.connected = true
}

func (s *Subscriber) markNotReady() {
	s.readyMu.Lock()
	defer s.readyMu.Unlock()
	if !s.connected {
		return
	}
	s.ready = make(chan struct{})
	s.connected = false
}

func (s *Subscriber) run(ctx context.Context, client *redis.Client, channel string, onMessage func(payload string)) {
	defer func() {
		s.markNotReady()
		close(s.stopped)
	}()
	delay := reconnectMinDelay
	for {
		if ctx.Err() != nil {
			return
		}
		ps := client.Subscribe(ctx, channel)
		receiveCtx, cancel := context.WithTimeout(ctx, redisOperationTimeout)
		_, err := ps.Receive(receiveCtx)
		cancel()
		if err != nil {
			s.setLastError(err)
			_ = ps.Close()
			if !sleepContext(ctx, delay) {
				return
			}
			delay *= 2
			if delay > reconnectMaxDelay {
				delay = reconnectMaxDelay
			}
			continue
		}
		s.setLastError(nil)
		s.markReady()
		delay = reconnectMinDelay

	receive:
		for {
			select {
			case <-ctx.Done():
				_ = ps.Close()
				return
			case message, ok := <-ps.Channel():
				if !ok {
					s.markNotReady()
					_ = ps.Close()
					break receive
				}
				onMessage(message.Payload)
			}
		}
	}
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
