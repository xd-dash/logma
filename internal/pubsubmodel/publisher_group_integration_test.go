package pubsubmodel

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRedisStorePublisherGroupRoundTrip(t *testing.T) {
	addr := os.Getenv("LOGMA_PUBSUBMODEL_REDIS_ADDR")
	if addr == "" {
		t.Skip("LOGMA_PUBSUBMODEL_REDIS_ADDR is not set")
	}

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })

	scope := "huram-local-publisher-groups"
	store, err := NewRedisStore(client, scope)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := NewResourceKeysV2(scope)
	if err != nil {
		t.Fatal(err)
	}

	channel := Channel{Name: "market"}
	publisher := Publisher{ID: "stonks-live", Channel: "market", Type: "stonks"}
	group := PublisherGroup{ID: "market-producers", PublisherIDs: []string{publisher.ID, "news-later"}}

	if err := store.PutChannel(ctx, channel); err != nil {
		t.Fatal(err)
	}
	if err := store.PutPublisher(ctx, publisher); err != nil {
		t.Fatal(err)
	}
	if err := store.PutPublisherGroup(ctx, group); err != nil {
		t.Fatalf("weak PublisherGroup should accept absent members: %v", err)
	}

	assertSet(t, ctx, client, keys.Publishers(), []string{publisher.ID})
	assertSet(t, ctx, client, keys.PublisherGroups(), []string{group.ID})
	assertSet(t, ctx, client, keys.PublisherGroupPublishers(group.ID), []string{publisher.ID, "news-later"})

	got, err := store.GetPublisherGroup(ctx, group.ID)
	if err != nil || len(got.PublisherIDs) != 2 {
		t.Fatalf("GetPublisherGroup=%#v %v", got, err)
	}
	if err := store.DeletePublisher(ctx, publisher.ID); err != nil {
		t.Fatalf("weak PublisherGroup blocked Publisher deletion: %v", err)
	}
	got, err = store.GetPublisherGroup(ctx, group.ID)
	if err != nil || len(got.PublisherIDs) != 2 {
		t.Fatalf("group declaration changed after member deletion: %#v %v", got, err)
	}
	if err := store.DeletePublisherGroup(ctx, group.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteChannel(ctx, channel.Name); err != nil {
		t.Fatal(err)
	}

	remaining, err := scanKeys(ctx, client, scope+":logma:pubsub:*")
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("residue=%v", remaining)
	}
}
