package pubsubmodel

import (
	"context"
	"os"
	"reflect"
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
	keys, err := NewRedisKeys(scope)
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

	assertRegistryIDs(t, []string{channelA.Name, channelB.Name}, store.ChannelIDs(ctx))
	assertRegistryIDs(t, []string{callbackA.ID, callbackB.ID}, store.CallbackIDs(ctx))
	assertRegistryIDs(t, []string{subscriber.ID}, store.SubscriberIDs(ctx))
	assertSet(t, ctx, client, keys.Channels(), []string{channelA.Name, channelB.Name})
	assertSet(t, ctx, client, keys.Callbacks(), []string{callbackA.ID, callbackB.ID})
	assertSet(t, ctx, client, keys.Subscribers(), []string{subscriber.ID})

	if err := store.PutChannel(ctx, channelA); err != nil {
		t.Fatalf("repeat put channel: %v", err)
	}
	if err := store.PutCallback(ctx, callbackA); err != nil {
		t.Fatalf("repeat put callback: %v", err)
	}
	if err := store.PutSubscriber(ctx, subscriber); err != nil {
		t.Fatalf("repeat put subscriber: %v", err)
	}
	assertRegistryIDs(t, []string{channelA.Name, channelB.Name}, store.ChannelIDs(ctx))
	assertRegistryIDs(t, []string{callbackA.ID, callbackB.ID}, store.CallbackIDs(ctx))
	assertRegistryIDs(t, []string{subscriber.ID}, store.SubscriberIDs(ctx))

	if err := store.DeleteCallback(ctx, callbackA.ID); err == nil {
		t.Fatal("DeleteCallback succeeded while callback remained referenced")
	}
	if err := store.DeleteChannel(ctx, channelA.Name); err == nil {
		t.Fatal("DeleteChannel succeeded while channel remained referenced")
	}
	assertRegistryIDs(t, []string{channelA.Name, channelB.Name}, store.ChannelIDs(ctx))
	assertRegistryIDs(t, []string{callbackA.ID, callbackB.ID}, store.CallbackIDs(ctx))

	if err := store.DeleteSubscriber(ctx, subscriber.ID); err != nil {
		t.Fatalf("delete subscriber: %v", err)
	}
	assertRegistryIDs(t, nil, store.SubscriberIDs(ctx))
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
	assertRegistryIDs(t, nil, store.ChannelIDs(ctx))
	assertRegistryIDs(t, nil, store.CallbackIDs(ctx))

	remaining, err := scanKeys(ctx, client, scope+":logma:pubsub:*")
	if err != nil {
		t.Fatalf("scan residue: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("Pub/Sub registry residue remains: %v", remaining)
	}
}

func assertRegistryIDs(t *testing.T, want []string, got []string, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("registry IDs = %#v, want %#v", got, want)
	}
}
