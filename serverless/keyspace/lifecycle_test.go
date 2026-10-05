package keyspace

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestLogmaLifecycleRequirements(t *testing.T) {
	scope, _ := ParseScope("probot-test")
	for _, access := range []Access{AccessPublish, AccessSubscribe, AccessPublish | AccessSubscribe} {
		req, err := CompileRedisRequirements(scope, Grant{Capability: CapabilityLogmaLifecycle, Access: access})
		if err != nil {
			t.Fatal(err)
		}
		if len(req.KeyPatterns) != 0 || !reflect.DeepEqual(req.ChannelPatterns, []string{"&probot-test:logma:lifecycle:*"}) {
			t.Fatalf("unexpected lifecycle authority: %+v", req)
		}
		want := []string{"ping", "hello", "client"}
		if access&AccessPublish != 0 {
			want = append(want, "publish")
		}
		if access&AccessSubscribe != 0 {
			want = append(want, "subscribe", "unsubscribe")
		}
		if !reflect.DeepEqual(req.Commands, want) {
			t.Fatalf("commands=%v, want %v", req.Commands, want)
		}
	}
	for _, access := range []Access{0, AccessRead, AccessWrite, AccessInvoke, AccessPublish | AccessRead} {
		if _, err := CompileRedisRequirements(scope, Grant{Capability: CapabilityLogmaLifecycle, Access: access}); err == nil {
			t.Fatalf("unsupported access %v compiled", access)
		}
	}
	if _, err := CompileRedisRequirements(Scope("foreign:*"), Grant{Capability: CapabilityLogmaLifecycle, Access: AccessPublish}); err == nil {
		t.Fatal("invalid scope compiled")
	}
}

func TestLogmaLifecycleACL(t *testing.T) {
	addr := os.Getenv("LOGMA_PUBSUBMODEL_REDIS_ADDR")
	if addr == "" {
		t.Skip("LOGMA_PUBSUBMODEL_REDIS_ADDR is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = admin.Close() })
	scope, _ := ParseScope("huram-local-lifecycle-acl")
	newClient := func(access Access) *redis.Client {
		req, err := CompileRedisRequirements(scope, Grant{Capability: CapabilityLogmaLifecycle, Access: access})
		if err != nil {
			t.Fatal(err)
		}
		user := fmt.Sprintf("logma-lifecycle-%d", time.Now().UnixNano())
		password := fmt.Sprintf("pw-%d", time.Now().UnixNano())
		args := []any{"ACL", "SETUSER", user, "reset", "on", ">" + password, "-@all"}
		for _, command := range req.Commands {
			args = append(args, "+"+command)
		}
		for _, pattern := range req.ChannelPatterns {
			args = append(args, pattern)
		}
		if err := admin.Do(ctx, args...).Err(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = admin.Do(context.Background(), "ACL", "DELUSER", user).Err() })
		client := redis.NewClient(&redis.Options{Addr: addr, Username: user, Password: password})
		t.Cleanup(func() { _ = client.Close() })
		return client
	}
	publisher, subscriber := newClient(AccessPublish), newClient(AccessSubscribe)
	channel := string(scope) + ":logma:lifecycle:probot-runtime:execution.succeeded"
	listener := subscriber.Subscribe(ctx, channel)
	defer listener.Close()
	if _, err := listener.ReceiveTimeout(ctx, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := publisher.Publish(ctx, channel, "correlated-event").Err(); err != nil {
		t.Fatal(err)
	}
	message, err := listener.ReceiveMessage(ctx)
	if err != nil || message.Channel != channel || message.Payload != "correlated-event" {
		t.Fatalf("delivery=%v err=%v", message, err)
	}
	for _, forbidden := range []string{
		"foreign:logma:lifecycle:probot-runtime:execution.succeeded",
		string(scope) + ":logma:transport:channel:probe",
		"global:probe",
	} {
		if err := publisher.Publish(ctx, forbidden, "probe").Err(); !isNOPERM(err) {
			t.Fatalf("PUBLISH %q = %v, want NOPERM", forbidden, err)
		}
		if err := subscriber.Do(ctx, "SUBSCRIBE", forbidden).Err(); !isNOPERM(err) {
			t.Fatalf("SUBSCRIBE %q = %v, want NOPERM", forbidden, err)
		}
	}
	for _, client := range []*redis.Client{publisher, subscriber} {
		for _, args := range [][]any{
			{"GET", string(scope) + ":probot:delivery:probe"},
			{"HGETALL", string(scope) + ":logma:pubsub:channel:probe"},
			{"XADD", string(scope) + ":skymill:stream:probe", "*", "data", "probe"},
			{"CONFIG", "GET", "save"},
			{"ACL", "USERS"},
		} {
			if err := client.Do(ctx, args...).Err(); !isNOPERM(err) {
				t.Fatalf("%v = %v, want NOPERM", args[0], err)
			}
		}
	}
	if err := subscriber.Publish(ctx, channel, "probe").Err(); !isNOPERM(err) {
		t.Fatalf("subscribe principal PUBLISH=%v", err)
	}
	if err := publisher.Do(ctx, "SUBSCRIBE", channel).Err(); !isNOPERM(err) {
		t.Fatalf("publish principal SUBSCRIBE=%v", err)
	}
}
