package pubsubmodel

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRedisStoreRejectsMismatchedStoredIdentity(t *testing.T) {
	addr := os.Getenv("LOGMA_PUBSUBMODEL_REDIS_ADDR")
	if addr == "" {
		t.Skip("LOGMA_PUBSUBMODEL_REDIS_ADDR is not set")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	scope := "huram-local-pubsubmodel-identity"
	store, err := NewRedisStore(client, scope)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := NewResourceKeysV2(scope)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		remaining, _ := scanKeys(context.Background(), client, scope+":logma:pubsub:*")
		if len(remaining) > 0 {
			_ = client.Del(context.Background(), remaining...).Err()
		}
	})

	if err := client.HSet(ctx, keys.Channel("expected"), "name", "different").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetChannel(ctx, "expected"); err == nil || !strings.Contains(err.Error(), "does not match requested identity") {
		t.Fatalf("GetChannel mismatched stored identity = %v", err)
	}

	if err := client.HSet(ctx, keys.Subscriber("expected-sub"), map[string]any{
		"id":      "different-sub",
		"channel": "expected",
	}).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetSubscriber(ctx, "expected-sub"); err == nil || !strings.Contains(err.Error(), "does not match requested identity") {
		t.Fatalf("GetSubscriber mismatched stored identity = %v", err)
	}
}
