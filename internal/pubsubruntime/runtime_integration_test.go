package pubsubruntime

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/internal/pubsubmodel"
)

func TestRuntimeActivatesEmptyChannelAgainstRedis(t *testing.T) {
	addr := os.Getenv("LOGMA_PUBSUBMODEL_REDIS_ADDR")
	if addr == "" {
		t.Skip("LOGMA_PUBSUBMODEL_REDIS_ADDR is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping Redis: %v", err)
	}

	const scope = "huram-local-channel-runtime"
	const channelName = "events-empty"
	store, err := pubsubmodel.NewRedisStore(client, scope)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutChannel(ctx, pubsubmodel.Channel{Name: channelName}); err != nil {
		t.Fatalf("put Channel: %v", err)
	}

	runtime, err := New(client, store)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := runtime.Activate(ctx, channelName, nil)
	if err != nil {
		t.Fatalf("activate Channel: %v", err)
	}

	select {
	case <-handle.Ready():
	case <-ctx.Done():
		t.Fatalf("wait for Channel readiness: %v; last Redis error: %v", ctx.Err(), handle.LastError())
	}

	numsub, err := client.PubSubNumSub(ctx, channelName).Result()
	if err != nil {
		t.Fatalf("PUBSUB NUMSUB: %v", err)
	}
	if numsub[channelName] != 1 {
		t.Fatalf("PUBSUB NUMSUB %s = %d, want 1", channelName, numsub[channelName])
	}

	if !handle.Close() {
		t.Fatal("Close did not deactivate Channel")
	}
	select {
	case <-handle.Stopped():
	case <-ctx.Done():
		t.Fatalf("wait for Channel stop: %v", ctx.Err())
	}

	if runtime.Active(channelName) {
		t.Fatal("Channel remains active after Close")
	}
	if _, err := store.GetChannel(ctx, channelName); err != nil {
		t.Fatalf("persisted Channel was removed by runtime deactivation: %v", err)
	}

	numsub, err = client.PubSubNumSub(ctx, channelName).Result()
	if err != nil {
		t.Fatalf("PUBSUB NUMSUB after close: %v", err)
	}
	if numsub[channelName] != 0 {
		t.Fatalf("PUBSUB NUMSUB %s after close = %d, want 0", channelName, numsub[channelName])
	}

	if err := store.DeleteChannel(ctx, channelName); err != nil {
		t.Fatalf("delete Channel: %v", err)
	}
}
