package pubsub

import (
	"context"
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
}

func Subscribe(ctx context.Context, client *redis.Client, channel string, onMessage func(payload string)) *Subscriber {
	s := &Subscriber{stopped: make(chan struct{})}
	go s.run(ctx, client, channel, onMessage)
	return s
}

func (s *Subscriber) Stopped() <-chan struct{} { return s.stopped }

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
		delay = reconnectMinDelay

	receive:
		for {
			select {
			case <-ctx.Done():
				_ = ps.Close()
				return
			case message, ok := <-ps.Channel():
				if !ok {
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
