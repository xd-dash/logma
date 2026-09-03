package pubsubmodel

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRedisStoreGraphRoundTrip(t *testing.T) {
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

	scope := "huram-local-pubsubmodel"
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
	callbackA := Callback{ID: "webhook-a", Type: CallbackWebhook, Webhook: &WebhookCallback{CallbackURLs: []string{"https://one.example/callback", "https://two.example/callback"}}}
	callbackB := Callback{ID: "webhook-b", Type: CallbackWebhook, Webhook: &WebhookCallback{CallbackURL: "https://three.example/callback"}}
	subscriber := Subscriber{ID: "subscriber-a", Channel: channelA.Name, CallbackIDs: []string{callbackA.ID, callbackB.ID}}
	publisher := Publisher{ID: "publisher-a", Channel: channelA.Name, Type: "fixture"}
	group := SubscriptionGroup{ID: "group-a", SubscriberIDs: []string{subscriber.ID}}

	for _, channel := range []Channel{channelA, channelB} {
		if err := store.PutChannel(ctx, channel); err != nil {
			t.Fatalf("put channel %s: %v", channel.Name, err)
		}
	}
	for _, callback := range []Callback{callbackA, callbackB} {
		if err := store.PutCallback(ctx, callback); err != nil {
			t.Fatalf("put callback %s: %v", callback.ID, err)
		}
	}
	if err := store.PutSubscriber(ctx, subscriber); err != nil {
		t.Fatalf("put subscriber: %v", err)
	}
	if err := store.PutPublisher(ctx, publisher); err != nil {
		t.Fatalf("put publisher: %v", err)
	}
	if err := store.PutSubscriptionGroup(ctx, group); err != nil {
		t.Fatalf("put group: %v", err)
	}

	assertHash(t, ctx, client, keys.Channel(channelA.Name), map[string]string{"name": channelA.Name})
	assertHash(t, ctx, client, keys.Callback(callbackA.ID), map[string]string{"id": callbackA.ID, "type": string(CallbackWebhook)})
	assertSet(t, ctx, client, keys.CallbackURLs(callbackA.ID), []string{"https://one.example/callback", "https://two.example/callback"})
	assertHash(t, ctx, client, keys.Subscriber(subscriber.ID), map[string]string{"id": subscriber.ID, "channel": channelA.Name})
	assertSet(t, ctx, client, keys.SubscriberCallbacks(subscriber.ID), []string{callbackA.ID, callbackB.ID})
	assertSet(t, ctx, client, keys.ChannelSubscribers(channelA.Name), []string{subscriber.ID})
	assertSet(t, ctx, client, keys.CallbackSubscribers(callbackA.ID), []string{subscriber.ID})
	assertSet(t, ctx, client, keys.CallbackSubscribers(callbackB.ID), []string{subscriber.ID})
	assertSet(t, ctx, client, keys.ChannelPublishers(channelA.Name), []string{publisher.ID})
	assertSet(t, ctx, client, keys.SubscriptionGroupSubscribers(group.ID), []string{subscriber.ID})

	updatedSubscriber := Subscriber{ID: subscriber.ID, Channel: channelB.Name, CallbackIDs: []string{callbackB.ID}}
	if err := store.PutSubscriber(ctx, updatedSubscriber); err != nil {
		t.Fatalf("update subscriber: %v", err)
	}
	assertSet(t, ctx, client, keys.ChannelSubscribers(channelA.Name), nil)
	assertSet(t, ctx, client, keys.ChannelSubscribers(channelB.Name), []string{subscriber.ID})
	assertSet(t, ctx, client, keys.CallbackSubscribers(callbackA.ID), nil)
	assertSet(t, ctx, client, keys.CallbackSubscribers(callbackB.ID), []string{subscriber.ID})
	assertSet(t, ctx, client, keys.SubscriberCallbacks(subscriber.ID), []string{callbackB.ID})

	if err := store.DeleteCallback(ctx, callbackB.ID); err == nil {
		t.Fatal("DeleteCallback succeeded while callback remained referenced")
	}
	if err := store.DeleteChannel(ctx, channelB.Name); err == nil {
		t.Fatal("DeleteChannel succeeded while channel remained referenced")
	}

	if err := store.DeleteSubscriber(ctx, subscriber.ID); err != nil {
		t.Fatalf("delete subscriber: %v", err)
	}
	if err := store.DeletePublisher(ctx, publisher.ID); err != nil {
		t.Fatalf("delete publisher: %v", err)
	}
	for _, callbackID := range []string{callbackA.ID, callbackB.ID} {
		if err := store.DeleteCallback(ctx, callbackID); err != nil {
			t.Fatalf("delete callback %s: %v", callbackID, err)
		}
	}
	for _, channelName := range []string{channelA.Name, channelB.Name} {
		if err := store.DeleteChannel(ctx, channelName); err != nil {
			t.Fatalf("delete channel %s: %v", channelName, err)
		}
	}
	if err := store.DeleteSubscriptionGroup(ctx, group.ID); err != nil {
		t.Fatalf("delete group: %v", err)
	}

	remaining, err := scanKeys(ctx, client, scope+":logma:pubsub:*")
	if err != nil {
		t.Fatalf("scan residue: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("Pub/Sub graph residue remains: %v", remaining)
	}

	if err := store.DeleteSubscriber(ctx, subscriber.ID); err != nil && !errors.Is(err, redis.Nil) {
		t.Fatalf("idempotent subscriber delete: %v", err)
	}
}

func assertHash(t *testing.T, ctx context.Context, client *redis.Client, key string, want map[string]string) {
	t.Helper()
	got, err := client.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatalf("HGETALL %s: %v", key, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("HGETALL %s = %#v, want %#v", key, got, want)
	}
}

func assertSet(t *testing.T, ctx context.Context, client *redis.Client, key string, want []string) {
	t.Helper()
	got, err := client.SMembers(ctx, key).Result()
	if err != nil {
		t.Fatalf("SMEMBERS %s: %v", key, err)
	}
	sort.Strings(got)
	want = append([]string(nil), want...)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("SMEMBERS %s = %#v, want %#v", key, got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("SMEMBERS %s = %#v, want %#v", key, got, want)
		}
	}
}

func scanKeys(ctx context.Context, client *redis.Client, pattern string) ([]string, error) {
	var cursor uint64
	var keys []string
	for {
		batch, next, err := client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, batch...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	sort.Strings(keys)
	return keys, nil
}
