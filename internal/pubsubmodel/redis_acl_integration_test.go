package pubsubmodel

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/serverless/keyspace"
)

func TestRedisStoreLogmaPubSubGraphACL(t *testing.T) {
	addr := os.Getenv("LOGMA_PUBSUBMODEL_REDIS_ADDR")
	if addr == "" {
		t.Skip("LOGMA_PUBSUBMODEL_REDIS_ADDR is not set")
	}

	ctx := context.Background()
	admin := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = admin.Close() })
	if err := admin.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping Redis as admin: %v", err)
	}

	scopeName := "huram-local-pubsubmodel-acl"
	scope, err := keyspace.ParseScope(scopeName)
	if err != nil {
		t.Fatal(err)
	}
	req, err := keyspace.CompileRedisRequirements(scope, keyspace.Grant{
		Capability: keyspace.CapabilityLogmaPubSubGraph,
		Access:     keyspace.AccessRead | keyspace.AccessWrite,
	})
	if err != nil {
		t.Fatalf("compile Redis requirements: %v", err)
	}
	if len(req.ChannelPatterns) != 0 {
		t.Fatalf("graph capability unexpectedly contains channel patterns: %v", req.ChannelPatterns)
	}

	username := fmt.Sprintf("logma-pubsub-graph-%d", time.Now().UnixNano())
	password := fmt.Sprintf("pw-%d", time.Now().UnixNano())
	setUser := []any{"ACL", "SETUSER", username, "reset", "on", ">" + password, "-@all"}
	for _, command := range req.Commands {
		setUser = append(setUser, "+"+command)
	}
	for _, pattern := range req.KeyPatterns {
		setUser = append(setUser, pattern)
	}
	if err := admin.Do(ctx, setUser...).Err(); err != nil {
		t.Fatalf("ACL SETUSER: %v", err)
	}
	t.Cleanup(func() {
		_ = admin.Do(context.Background(), "ACL", "DELUSER", username).Err()
		keys, err := scanKeys(context.Background(), admin, scopeName+":logma:pubsub:*")
		if err == nil && len(keys) > 0 {
			_ = admin.Del(context.Background(), keys...).Err()
		}
	})

	restricted := redis.NewClient(&redis.Options{Addr: addr, Username: username, Password: password})
	t.Cleanup(func() { _ = restricted.Close() })
	if err := restricted.Ping(ctx).Err(); err != nil {
		t.Fatalf("restricted PING: %v", err)
	}

	store, err := NewRedisStore(restricted, scopeName)
	if err != nil {
		t.Fatal(err)
	}
	channel := Channel{Name: "market:oil"}
	callback := Callback{
		ID:   "webhook:primary",
		Type: CallbackWebhook,
		Webhook: &WebhookCallback{
			CallbackURL: "https://example.invalid/callback",
		},
	}
	subscriber := Subscriber{
		ID:          "subscriber:primary",
		Channel:     channel.Name,
		CallbackIDs: []string{callback.ID},
	}

	if err := store.PutChannel(ctx, channel); err != nil {
		t.Fatalf("graph PutChannel under generated ACL: %v", err)
	}
	if err := store.PutCallback(ctx, callback); err != nil {
		t.Fatalf("graph PutCallback under generated ACL: %v", err)
	}
	if err := store.PutSubscriber(ctx, subscriber); err != nil {
		t.Fatalf("graph PutSubscriber under generated ACL: %v", err)
	}
	got, err := store.GetSubscriber(ctx, subscriber.ID)
	if err != nil {
		t.Fatalf("graph GetSubscriber under generated ACL: %v", err)
	}
	if got.ID != subscriber.ID || got.Channel != subscriber.Channel || len(got.CallbackIDs) != 1 || got.CallbackIDs[0] != callback.ID {
		t.Fatalf("unexpected subscriber round-trip: %#v", got)
	}

	assertRedisNOPERM(t, restricted.HSet(ctx, scopeName+":logma:runtime:activation:probe", "state", "active").Err(), "neighboring logma runtime key")
	assertRedisNOPERM(t, restricted.HSet(ctx, "foreign-scope:logma:pubsub:channel:probe", "name", "probe").Err(), "foreign FATLINE_SCOPE")
	assertRedisNOPERM(t, restricted.Publish(ctx, scopeName+":transport:events", "probe").Err(), "Pub/Sub publish transport")
	assertRedisNOPERM(t, restricted.Do(ctx, "SUBSCRIBE", scopeName+":transport:events").Err(), "Pub/Sub subscribe transport")
}

func assertRedisNOPERM(t *testing.T, err error, operation string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s unexpectedly succeeded", operation)
	}
	if !strings.Contains(strings.ToUpper(err.Error()), "NOPERM") {
		t.Fatalf("%s error = %v; want Redis NOPERM", operation, err)
	}
}
