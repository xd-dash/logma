package pubsubmodel

import (
	"context"
	"errors"
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
	group := PublisherGroup{ID: "market-producers", PublisherIDs: []string{publisher.ID}}

	if err := store.PutChannel(ctx, channel); err != nil {
		t.Fatal(err)
	}
	if err := store.PutPublisher(ctx, publisher); err != nil {
		t.Fatal(err)
	}
	if err := store.PutPublisherGroup(ctx, group); err != nil {
		t.Fatal(err)
	}

	assertSet(t, ctx, client, keys.Publishers(), []string{publisher.ID})
	assertSet(t, ctx, client, keys.PublisherGroups(), []string{group.ID})
	assertSet(t, ctx, client, keys.PublisherGroupPublishers(group.ID), []string{publisher.ID})
	assertSet(t, ctx, client, keys.PublisherGroupsForPublisher(publisher.ID), []string{group.ID})

	got, err := store.GetPublisherGroup(ctx, group.ID)
	if err != nil || len(got.PublisherIDs) != 1 || got.PublisherIDs[0] != publisher.ID {
		t.Fatalf("GetPublisherGroup=%#v %v", got, err)
	}
	if err := store.DeletePublisher(ctx, publisher.ID); !errors.Is(err, ErrReferenced) {
		t.Fatalf("DeletePublisher while grouped=%v want ErrReferenced", err)
	}
	if err := store.DeletePublisherGroup(ctx, group.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.DeletePublisher(ctx, publisher.ID); err != nil {
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
