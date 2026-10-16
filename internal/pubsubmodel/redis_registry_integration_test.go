package pubsubmodel

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRedisStoreResourceRegistries(t *testing.T) {
	addr := os.Getenv("LOGMA_PUBSUBMODEL_REDIS_ADDR")
	if addr == "" {
		t.Skip("LOGMA_PUBSUBMODEL_REDIS_ADDR is not set")
	}

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping Redis: %v", err)
	}

	scope := "huram-local-pubsubmodel-registry"
	store, err := NewRedisStore(client, scope)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := NewResourceKeysV2(scope)
	if err != nil {
		t.Fatal(err)
	}

	channelA := Channel{Name: "events-a"}
	channelB := Channel{Name: "events-b"}
	callbackA := Callback{ID: "hook-a", Type: CallbackWebhook, Webhook: &WebhookCallback{CallbackURL: "https://one.example/callback"}}
	callbackB := Callback{ID: "hook-b", Type: CallbackLua, Lua: &LuaCallback{Name: "callback_b"}}
	subscriber := Subscriber{ID: "subscriber-a", Channel: channelA.Name, CallbackIDs: []string{callbackA.ID}}

	for _, channel := range []Channel{channelB, channelA} {
		if err := store.PutChannel(ctx, channel); err != nil {
			t.Fatalf("put channel %s: %v", channel.Name, err)
		}
	}
	for _, callback := range []Callback{callbackB, callbackA} {
		if err := store.PutCallback(ctx, callback); err != nil {
			t.Fatalf("put callback %s: %v", callback.ID, err)
		}
	}
	if err := store.PutSubscriber(ctx, subscriber); err != nil {
		t.Fatalf("put subscriber: %v", err)
	}

	assertRegistryIDs(t, func() ([]string, error) { return store.ChannelIDs(ctx) }, []string{channelA.Name, channelB.Name})
	assertRegistryIDs(t, func() ([]string, error) { return store.CallbackIDs(ctx) }, []string{callbackA.ID, callbackB.ID})
	assertRegistryIDs(t, func() ([]string, error) { return store.SubscriberIDs(ctx) }, []string{subscriber.ID})
	assertSet(t, ctx, client, keys.Channels(), []string{channelA.Name, channelB.Name})
	assertSet(t, ctx, client, keys.Callbacks(), []string{callbackA.ID, callbackB.ID})
	assertSet(t, ctx, client, keys.Subscribers(), []string{subscriber.ID})

	for _, key := range []string{keys.Channels(), keys.Callbacks(), keys.Subscribers()} {
		if len(key) <= len(scope+":logma:pubsub:registry:") || key[:len(scope+":logma:pubsub:registry:")] != scope+":logma:pubsub:registry:" {
			t.Fatalf("registry key %q does not use v2 registry grammar", key)
		}
	}

	if err := store.PutChannel(ctx, channelA); err != nil {
		t.Fatalf("repeat put channel: %v", err)
	}
	if err := store.PutCallback(ctx, callbackA); err != nil {
		t.Fatalf("repeat put callback: %v", err)
	}
	if err := store.PutSubscriber(ctx, subscriber); err != nil {
		t.Fatalf("repeat put subscriber: %v", err)
	}
	assertRegistryIDs(t, func() ([]string, error) { return store.ChannelIDs(ctx) }, []string{channelA.Name, channelB.Name})
	assertRegistryIDs(t, func() ([]string, error) { return store.CallbackIDs(ctx) }, []string{callbackA.ID, callbackB.ID})
	assertRegistryIDs(t, func() ([]string, error) { return store.SubscriberIDs(ctx) }, []string{subscriber.ID})

	if err := store.DeleteCallback(ctx, callbackA.ID); err == nil {
		t.Fatal("DeleteCallback succeeded while callback remained referenced")
	}
	if err := store.DeleteChannel(ctx, channelA.Name); err == nil {
		t.Fatal("DeleteChannel succeeded while channel remained referenced")
	}
	assertRegistryIDs(t, func() ([]string, error) { return store.ChannelIDs(ctx) }, []string{channelA.Name, channelB.Name})
	assertRegistryIDs(t, func() ([]string, error) { return store.CallbackIDs(ctx) }, []string{callbackA.ID, callbackB.ID})

	if err := store.DeleteSubscriber(ctx, subscriber.ID); err != nil {
		t.Fatalf("delete subscriber: %v", err)
	}
	assertRegistryIDs(t, func() ([]string, error) { return store.SubscriberIDs(ctx) }, nil)
	if err := store.DeleteCallback(ctx, callbackA.ID); err != nil {
		t.Fatalf("delete callback A: %v", err)
	}
	if err := store.DeleteCallback(ctx, callbackB.ID); err != nil {
		t.Fatalf("delete callback B: %v", err)
	}
	if err := store.DeleteChannel(ctx, channelA.Name); err != nil {
		t.Fatalf("delete channel A: %v", err)
	}
	if err := store.DeleteChannel(ctx, channelB.Name); err != nil {
		t.Fatalf("delete channel B: %v", err)
	}
	assertRegistryIDs(t, func() ([]string, error) { return store.ChannelIDs(ctx) }, nil)
	assertRegistryIDs(t, func() ([]string, error) { return store.CallbackIDs(ctx) }, nil)

	remaining, err := scanKeys(ctx, client, scope+":logma:pubsub:*")
	if err != nil {
		t.Fatalf("scan residue: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("Pub/Sub registry residue remains: %v", remaining)
	}
}

func assertRegistryIDs(t *testing.T, read func() ([]string, error), want []string) {
	t.Helper()
	got, err := read()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("registry IDs = %#v, want %#v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("registry IDs = %#v, want %#v", got, want)
		}
	}
}
