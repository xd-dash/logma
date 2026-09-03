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
	ready   chan struct{}

	readyOnce sync.Once
	errMu     sync.RWMutex
	lastErr   error
}

func Subscribe(ctx context.Context, client *redis.Client, channel string, onMessage func(payload string)) *Subscriber {
	s := &Subscriber{stopped: make(chan struct{}), ready: make(chan struct{})}
	go s.run(ctx, client, channel, onMessage)
	return s
}

func (s *Subscriber) Stopped() <-chan struct{} { return s.stopped }

// Ready closes after the initial Redis SUBSCRIBE acknowledgement for this
// Subscriber lifetime. go-redis owns transparent reconnect/resubscribe after
// that point, so Ready is intentionally not presented as continuous socket
// health or as a fresh readiness generation after every transport interruption.
func (s *Subscriber) Ready() <-chan struct{} { return s.ready }

// LastError returns the most recent initial-subscription acknowledgement error.
// It is diagnostic state only; Subscribe continues its bounded retry loop until
// the initial subscription is established or the lifetime context is canceled.
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

func (s *Subscriber) markInitiallyReady() {
	s.readyOnce.Do(func() { close(s.ready) })
}

func (s *Subscriber) run(ctx context.Context, client *redis.Client, channel string, onMessage func(payload string)) {
	defer close(s.stopped)
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
		s.markInitiallyReady()

		// go-redis PubSub.Channel owns transparent network reconnect and
		// re-subscription. This outer loop is re-entered only if the PubSub itself
		// closes unexpectedly; initial readiness is not reset for the same
		// Subscriber lifetime because Channel does not expose an unready edge.
		for {
			select {
			case <-ctx.Done():
				_ = ps.Close()
				return
			case message, ok := <-ps.Channel():
				if !ok {
					_ = ps.Close()
					break
				}
				onMessage(message.Payload)
			}
			if ps.Channel() == nil {
				break
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
