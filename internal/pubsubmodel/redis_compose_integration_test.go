package pubsubmodel

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRedisStoreCreateWebhookSubscriptionAtomic(t *testing.T) {
	addr := os.Getenv("LOGMA_PUBSUBMODEL_REDIS_ADDR")
	if addr == "" {
		t.Skip("LOGMA_PUBSUBMODEL_REDIS_ADDR is not set")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	scope := "huram-local-pubsubmodel-compose"
	store, err := NewRedisStore(client, scope)
	if err != nil {
		t.Fatal(err)
	}
	keys, _ := NewResourceKeysV2(scope)
	t.Cleanup(func() {
		remaining, _ := scanKeys(context.Background(), client, scope+":logma:pubsub:*")
		if len(remaining) > 0 {
			_ = client.Del(context.Background(), remaining...).Err()
		}
	})

	channel := Channel{Name: "market:quotes"}
	callback := Callback{ID: "hook-a", Type: CallbackWebhook, Webhook: &WebhookCallback{CallbackURL: "https://one.example/hook"}}
	subscriber := Subscriber{ID: "sub-a", Channel: channel.Name, CallbackIDs: []string{callback.ID}}
	if err := store.CreateWebhookSubscription(ctx, channel, callback, subscriber); err != nil {
		t.Fatalf("CreateWebhookSubscription: %v", err)
	}
	assertHash(t, ctx, client, keys.Channel(channel.Name), map[string]string{"name": channel.Name})
	assertHash(t, ctx, client, keys.Callback(callback.ID), map[string]string{"id": callback.ID, "type": string(CallbackWebhook)})
	assertHash(t, ctx, client, keys.Subscriber(subscriber.ID), map[string]string{"id": subscriber.ID, "channel": channel.Name})
	assertSet(t, ctx, client, keys.ChannelSubscribers(channel.Name), []string{subscriber.ID})
	assertSet(t, ctx, client, keys.CallbackSubscribers(callback.ID), []string{subscriber.ID})

	if err := store.CreateWebhookSubscription(ctx,
		Channel{Name: "must-not-exist"},
		Callback{ID: callback.ID, Type: CallbackWebhook, Webhook: &WebhookCallback{CallbackURL: "https://replacement.example/hook"}},
		Subscriber{ID: "sub-b", Channel: "must-not-exist", CallbackIDs: []string{callback.ID}},
	); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("Callback conflict = %v, want ErrAlreadyExists", err)
	}
	if exists, _ := client.Exists(ctx, keys.Channel("must-not-exist"), keys.Subscriber("sub-b")).Result(); exists != 0 {
		t.Fatal("Callback conflict left partial Channel/Subscriber state")
	}
	gotCallback, err := store.GetCallback(ctx, callback.ID)
	if err != nil || gotCallback.Webhook == nil || len(gotCallback.Webhook.URLs()) != 1 || gotCallback.Webhook.URLs()[0] != "https://one.example/hook" {
		t.Fatalf("existing Callback changed after conflict: %#v %v", gotCallback, err)
	}

	if err := store.CreateWebhookSubscription(ctx,
		Channel{Name: "also-must-not-exist"},
		Callback{ID: "new-hook", Type: CallbackWebhook, Webhook: &WebhookCallback{CallbackURL: "https://new.example/hook"}},
		Subscriber{ID: subscriber.ID, Channel: "also-must-not-exist", CallbackIDs: []string{"new-hook"}},
	); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("Subscriber conflict = %v, want ErrAlreadyExists", err)
	}
	if exists, _ := client.Exists(ctx, keys.Channel("also-must-not-exist"), keys.Callback("new-hook")).Result(); exists != 0 {
		t.Fatal("Subscriber conflict left partial Channel/Callback state")
	}
}
