package pubsubmodel

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/redis/go-redis/v9"
)

func (s *RedisStore) PutPublisherGroup(ctx context.Context, group PublisherGroup) error {
	if err := group.Validate(); err != nil {
		return err
	}
	id := normalizeIdentity(group.ID)
	publishers := uniqueTrimmed(group.PublisherIDs)
	groupKey := s.keys.PublisherGroup(id)
	membersKey := s.keys.PublisherGroupPublishers(id)
	watchKeys := []string{groupKey, membersKey}
	for _, publisherID := range publishers {
		watchKeys = append(watchKeys, s.keys.Publisher(publisherID), s.keys.PublisherGroupsForPublisher(publisherID))
	}
	return s.watch(ctx, func(tx *redis.Tx) error {
		oldPublishers, err := tx.SMembers(ctx, membersKey).Result()
		if err != nil {
			return err
		}
		if len(publishers) > 0 {
			references := make([]string, 0, len(publishers))
			for _, publisherID := range publishers {
				references = append(references, s.keys.Publisher(publisherID))
			}
			exists, err := tx.Exists(ctx, references...).Result()
			if err != nil {
				return err
			}
			if exists != int64(len(references)) {
				return fmt.Errorf("%w: publisher group %s references missing publisher", ErrMissingReference, id)
			}
		}
		oldSet, newSet := stringSet(oldPublishers), stringSet(publishers)
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, groupKey, map[string]any{"id": id})
			pipe.Del(ctx, membersKey)
			if len(publishers) > 0 {
				pipe.SAdd(ctx, membersKey, stringsToAny(publishers)...)
			}
			pipe.SAdd(ctx, s.keys.PublisherGroups(), id)
			for publisherID := range oldSet {
				if _, retained := newSet[publisherID]; !retained {
					pipe.SRem(ctx, s.keys.PublisherGroupsForPublisher(publisherID), id)
				}
			}
			for _, publisherID := range publishers {
				pipe.SAdd(ctx, s.keys.PublisherGroupsForPublisher(publisherID), id)
			}
			return nil
		})
		return err
	}, watchKeys...)
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
	publishers, err := s.client.SMembers(ctx, s.keys.PublisherGroupPublishers(id)).Result()
	if err != nil {
		return PublisherGroup{}, err
	}
	sort.Strings(publishers)
	group := PublisherGroup{ID: fields["id"], PublisherIDs: publishers}
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
	groupKey := s.keys.PublisherGroup(id)
	membersKey := s.keys.PublisherGroupPublishers(id)
	return s.watch(ctx, func(tx *redis.Tx) error {
		members, err := tx.SMembers(ctx, membersKey).Result()
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, groupKey, membersKey)
			pipe.SRem(ctx, s.keys.PublisherGroups(), id)
			for _, publisherID := range members {
				pipe.SRem(ctx, s.keys.PublisherGroupsForPublisher(publisherID), id)
			}
			return nil
		})
		return err
	}, groupKey, membersKey)
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

func (s *RedisStore) PublisherGroupIDsForPublisher(ctx context.Context, publisherID string) ([]string, error) {
	publisherID = normalizeIdentity(publisherID)
	if publisherID == "" {
		return nil, errors.New("publisher id is required")
	}
	return s.sortedMembers(ctx, s.keys.PublisherGroupsForPublisher(publisherID))
}
