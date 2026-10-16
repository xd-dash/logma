package pubsubmodel

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRedisStoreRetriesWatchConflict(t *testing.T) {
	addr := os.Getenv("LOGMA_PUBSUBMODEL_REDIS_ADDR")
	if addr == "" {
		t.Skip("LOGMA_PUBSUBMODEL_REDIS_ADDR is not set")
	}

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: addr})
	interferer := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	t.Cleanup(func() { _ = interferer.Close() })

	scope := "huram-local-pubsubmodel-watch-retry"
	store, err := NewRedisStore(client, scope)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := NewResourceKeysV2(scope)
	if err != nil {
		t.Fatal(err)
	}
	key := keys.Channel("conflict")
	t.Cleanup(func() { _ = interferer.Del(context.Background(), key).Err() })

	attempts := 0
	err = store.watch(ctx, func(tx *redis.Tx) error {
		attempts++
		if attempts == 1 {
			// Mutate the watched key from an independent connection after WATCH
			// has been established but before EXEC. Redis must reject this first
			// transaction with TxFailedErr, which the store should retry.
			if err := interferer.HSet(ctx, key, "interferer", "1").Err(); err != nil {
				return err
			}
		}
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, key, "store", attempts)
			return nil
		})
		return err
	}, key)
	if err != nil {
		t.Fatalf("watch retry failed: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("watch callback attempts = %d, want 2", attempts)
	}
	got, err := client.HGet(ctx, key, "store").Int()
	if err != nil {
		t.Fatalf("read committed retry value: %v", err)
	}
	if got != 2 {
		t.Fatalf("committed retry value = %d, want 2", got)
	}
}
