package pubsubmodel

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/redis/go-redis/v9"
)

// PutPublisherGroup stores a durable, mutable operational collection of
// Publisher identities. Membership is weak: a Publisher may be absent now or
// later, and membership does not protect a Publisher from deletion.
func (s *RedisStore) PutPublisherGroup(ctx context.Context, group PublisherGroup) error {
	if err := group.Validate(); err != nil {
		return err
	}
	id := normalizeIdentity(group.ID)
	publishers := uniqueTrimmed(group.PublisherIDs)
	groupKey := s.keys.PublisherGroup(id)
	membersKey := s.keys.PublisherGroupPublishers(id)
	_, err := s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, groupKey, map[string]any{"id": id})
		pipe.Del(ctx, membersKey)
		if len(publishers) > 0 {
			pipe.SAdd(ctx, membersKey, stringsToAny(publishers)...)
		}
		pipe.SAdd(ctx, s.keys.PublisherGroups(), id)
		return nil
	})
	return err
}

func (s *RedisStore) GetPublisherGroup(ctx context.Context, id string) (PublisherGroup, error) {
	id = normalizeIdentity(id)
	if id == "" {
		return PublisherGroup{}, errors.New("publisher group id is required")
	}
	fields, err := s.client.HGetAll(ctx, s.keys.PublisherGroup(id)).Result()
	if err != nil {
		return PublisherGroup{}, err
	}
	if len(fields) == 0 {
		return PublisherGroup{}, fmt.Errorf("%w: publisher group %s", ErrNotFound, id)
	}
	if err := storedIdentity("publisher group", id, fields["id"]); err != nil {
		return PublisherGroup{}, err
	}
	publishers, err := s.client.SMembers(ctx, s.keys.PublisherGroupPublishers(id)).Result()
	if err != nil {
		return PublisherGroup{}, err
	}
	sort.Strings(publishers)
	group := PublisherGroup{ID: id, PublisherIDs: publishers}
	if err := group.Validate(); err != nil {
		return PublisherGroup{}, fmt.Errorf("decode publisher group %s: %w", id, err)
	}
	return group, nil
}

func (s *RedisStore) DeletePublisherGroup(ctx context.Context, id string) error {
	id = normalizeIdentity(id)
	if id == "" {
		return errors.New("publisher group id is required")
	}
	_, err := s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, s.keys.PublisherGroup(id), s.keys.PublisherGroupPublishers(id))
		pipe.SRem(ctx, s.keys.PublisherGroups(), id)
		return nil
	})
	return err
}

func (s *RedisStore) PublisherIDs(ctx context.Context) ([]string, error) {
	return s.sortedMembers(ctx, s.keys.Publishers())
}

func (s *RedisStore) SubscriptionGroupIDs(ctx context.Context) ([]string, error) {
	return s.sortedMembers(ctx, s.keys.SubscriptionGroups())
}

func (s *RedisStore) PublisherGroupIDs(ctx context.Context) ([]string, error) {
	return s.sortedMembers(ctx, s.keys.PublisherGroups())
}
