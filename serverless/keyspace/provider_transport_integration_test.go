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
	scoped, err := LogmaPubSubTransportChannel(scope, "market:quotes")
	if err != nil {
		t.Fatal(err)
	}

	subscriber := newTransportACLClient(t, ctx, admin, addr, scope, AccessSubscribe)
	pubsub := subscriber.Subscribe(ctx, scoped)
	if _, err := pubsub.ReceiveTimeout(ctx, 2*time.Second); err != nil {
		t.Fatalf("scoped SUBSCRIBE failed: %v", err)
	}
	_ = pubsub.Close()
	if err := subscriber.Publish(ctx, scoped, "probe").Err(); !isNOPERM(err) {
		t.Fatalf("Subscribe-only principal PUBLISH = %v, want NOPERM", err)
	}
	foreign := "foreign-scope:logma:transport:channel:market%3Aquotes"
	if err := subscriber.Do(ctx, "SUBSCRIBE", foreign).Err(); !isNOPERM(err) {
		t.Fatalf("foreign-scope SUBSCRIBE = %v, want NOPERM", err)
	}
}

func TestLogmaPubSubTransportPublishACL(t *testing.T) {
	addr := os.Getenv("LOGMA_PUBSUBMODEL_REDIS_ADDR")
	if addr == "" {
		t.Skip("LOGMA_PUBSUBMODEL_REDIS_ADDR is not set")
	}
	ctx := context.Background()
	admin := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = admin.Close() })
	scope, err := ParseScope("huram-local-transport-publish-acl")
	if err != nil {
		t.Fatal(err)
	}
	scoped, err := LogmaPubSubTransportChannel(scope, "market:quotes")
	if err != nil {
		t.Fatal(err)
	}

	adminSub := admin.Subscribe(ctx, scoped)
	if _, err := adminSub.ReceiveTimeout(ctx, 2*time.Second); err != nil {
		t.Fatalf("admin SUBSCRIBE setup failed: %v", err)
	}
	defer adminSub.Close()

	publisher := newTransportACLClient(t, ctx, admin, addr, scope, AccessPublish)
	if err := publisher.Publish(ctx, scoped, "probe").Err(); err != nil {
		t.Fatalf("scoped PUBLISH failed: %v", err)
	}
	message, err := adminSub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("receive published message: %v", err)
	}
	if message.Channel != scoped || message.Payload != "probe" {
		t.Fatalf("published message = %#v", message)
	}
	if err := publisher.Do(ctx, "SUBSCRIBE", scoped).Err(); !isNOPERM(err) {
		t.Fatalf("Publish-only principal SUBSCRIBE = %v, want NOPERM", err)
	}
	foreign := "foreign-scope:logma:transport:channel:market%3Aquotes"
	if err := publisher.Publish(ctx, foreign, "probe").Err(); !isNOPERM(err) {
		t.Fatalf("foreign-scope PUBLISH = %v, want NOPERM", err)
	}
}

func newTransportACLClient(t *testing.T, ctx context.Context, admin *redis.Client, addr string, scope Scope, access Access) *redis.Client {
	t.Helper()
	req, err := CompileRedisRequirements(scope, Grant{Capability: CapabilityLogmaPubSubTransport, Access: access})
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
	client := redis.NewClient(&redis.Options{Addr: addr, Username: username, Password: password})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("restricted PING: %v", err)
	}
	return client
}

func isNOPERM(err error) bool {
	return err != nil && strings.Contains(strings.ToUpper(err.Error()), "NOPERM")
}
