package keyspace

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestLogmaPubSubTransportSubscribeACL(t *testing.T) {
	addr := os.Getenv("LOGMA_PUBSUBMODEL_REDIS_ADDR")
	if addr == "" {
		t.Skip("LOGMA_PUBSUBMODEL_REDIS_ADDR is not set")
	}
	ctx := context.Background()
	admin := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = admin.Close() })
	scope, err := ParseScope("huram-local-transport-acl")
	if err != nil {
		t.Fatal(err)
	}
	req, err := CompileRedisRequirements(scope, Grant{Capability: CapabilityLogmaPubSubTransport, Access: AccessSubscribe})
	if err != nil {
		t.Fatal(err)
	}
	username := fmt.Sprintf("logma-transport-%d", time.Now().UnixNano())
	password := fmt.Sprintf("pw-%d", time.Now().UnixNano())
	args := []any{"ACL", "SETUSER", username, "reset", "on", ">" + password, "-@all"}
	for _, command := range req.Commands {
		args = append(args, "+"+command)
	}
	for _, pattern := range req.ChannelPatterns {
		args = append(args, pattern)
	}
	if err := admin.Do(ctx, args...).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Do(context.Background(), "ACL", "DELUSER", username).Err() })

	restricted := redis.NewClient(&redis.Options{Addr: addr, Username: username, Password: password})
	t.Cleanup(func() { _ = restricted.Close() })
	scoped, err := LogmaPubSubTransportChannel(scope, "market:quotes")
	if err != nil {
		t.Fatal(err)
	}
	pubsub := restricted.Subscribe(ctx, scoped)
	if _, err := pubsub.ReceiveTimeout(ctx, 2*time.Second); err != nil {
		t.Fatalf("scoped SUBSCRIBE failed: %v", err)
	}
	_ = pubsub.Close()

	if err := restricted.Publish(ctx, scoped, "probe").Err(); err == nil || !strings.Contains(strings.ToUpper(err.Error()), "NOPERM") {
		t.Fatalf("Subscribe-only principal PUBLISH = %v, want NOPERM", err)
	}
	foreign := "foreign-scope:logma:transport:channel:market%3Aquotes"
	if err := restricted.Do(ctx, "SUBSCRIBE", foreign).Err(); err == nil || !strings.Contains(strings.ToUpper(err.Error()), "NOPERM") {
		t.Fatalf("foreign-scope SUBSCRIBE = %v, want NOPERM", err)
	}
}
