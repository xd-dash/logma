package pubsubmodel

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const redisWatchMaxAttempts = 8

// watch retries the normal optimistic-lock collision reported by Redis as
// TxFailedErr. The store owns this provider detail so callers do not have to
// distinguish an expected concurrent graph update from an infrastructure
// failure. A bounded retry keeps sustained contention visible instead of
// spinning indefinitely.
func (s *RedisStore) watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error {
	for attempt := 1; attempt <= redisWatchMaxAttempts; attempt++ {
		err := s.client.Watch(ctx, fn, keys...)
		if err == nil {
			return nil
		}
		if !errors.Is(err, redis.TxFailedErr) {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return fmt.Errorf("Pub/Sub graph transaction remained contended after %d attempts: %w", redisWatchMaxAttempts, redis.TxFailedErr)
}
