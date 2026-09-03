package pubsubmodel

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// DeleteSubscriber removes a Subscriber and reconciles its reverse Channel,
// Callback, and discovery indexes in the same optimistic Redis transaction.
// A Subscriber referenced by a SubscriptionGroup cannot be deleted because
// doing so would invalidate the same graph invariant enforced by group writes.
func (s *RedisStore) DeleteSubscriber(ctx context.Context, id string) error {
	id = normalizeIdentity(id)
	if id == "" {
		return errors.New("subscriber id is required")
	}

	subscriberKey := s.keys.Subscriber(id)
	callbacksKey := s.keys.SubscriberCallbacks(id)
	groupsKey := s.keys.SubscriberGroups(id)

	return s.client.Watch(ctx, func(tx *redis.Tx) error {
		groups, err := tx.SCard(ctx, groupsKey).Result()
		if err != nil {
			return err
		}
		if groups != 0 {
			return fmt.Errorf("%w: subscriber %s belongs to subscription groups", ErrReferenced, id)
		}

		channel, err := tx.HGet(ctx, subscriberKey, "channel").Result()
		if err == redis.Nil {
			// Idempotent deletion should also heal stale discovery/forward indexes
			// when the resource HASH has already disappeared. A non-empty groups
			// reverse index is still authoritative and was rejected above.
			_, cleanupErr := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Del(ctx, callbacksKey, groupsKey)
				pipe.SRem(ctx, s.keys.Subscribers(), id)
				return nil
			})
			return cleanupErr
		}
		if err != nil {
			return err
		}

		callbacks, err := tx.SMembers(ctx, callbacksKey).Result()
		if err != nil {
			return err
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, subscriberKey, callbacksKey, groupsKey)
			pipe.SRem(ctx, s.keys.Subscribers(), id)
			if channel != "" {
				pipe.SRem(ctx, s.keys.ChannelSubscribers(channel), id)
			}
			for _, callbackID := range callbacks {
				pipe.SRem(ctx, s.keys.CallbackSubscribers(callbackID), id)
			}
			return nil
		})
		return err
	}, subscriberKey, callbacksKey, groupsKey)
}

// DeletePublisher removes a Publisher and its reverse Channel index.
func (s *RedisStore) DeletePublisher(ctx context.Context, id string) error {
	id = normalizeIdentity(id)
	if id == "" {
		return errors.New("publisher id is required")
	}

	publisherKey := s.keys.Publisher(id)
	return s.client.Watch(ctx, func(tx *redis.Tx) error {
		channel, err := tx.HGet(ctx, publisherKey, "channel").Result()
		if err == redis.Nil {
			return nil
		}
		if err != nil {
			return err
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, publisherKey)
			if channel != "" {
				pipe.SRem(ctx, s.keys.ChannelPublishers(channel), id)
			}
			return nil
		})
		return err
	}, publisherKey)
}

// DeleteCallback refuses to remove a Callback while any Subscriber still
// references it. This keeps the reverse index authoritative instead of
// silently producing dangling Subscriber callback IDs.
func (s *RedisStore) DeleteCallback(ctx context.Context, id string) error {
	id = normalizeIdentity(id)
	if id == "" {
		return errors.New("callback id is required")
	}

	resourceKey := s.keys.Callback(id)
	subscribersKey := s.keys.CallbackSubscribers(id)
	urlsKey := s.keys.CallbackURLs(id)

	return s.client.Watch(ctx, func(tx *redis.Tx) error {
		references, err := tx.SCard(ctx, subscribersKey).Result()
		if err != nil {
			return err
		}
		if references != 0 {
			return fmt.Errorf("%w: callback %s has subscribers", ErrReferenced, id)
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, resourceKey, subscribersKey, urlsKey)
			pipe.SRem(ctx, s.keys.Callbacks(), id)
			return nil
		})
		return err
	}, resourceKey, subscribersKey, urlsKey)
}

// DeleteChannel refuses to remove an active Channel while Subscribers or
// Publishers still reference it. Callers must detach those graph edges first.
func (s *RedisStore) DeleteChannel(ctx context.Context, name string) error {
	name = normalizeIdentity(name)
	if name == "" {
		return errors.New("channel name is required")
	}

	resourceKey := s.keys.Channel(name)
	subscribersKey := s.keys.ChannelSubscribers(name)
	publishersKey := s.keys.ChannelPublishers(name)

	return s.client.Watch(ctx, func(tx *redis.Tx) error {
		subscribers, err := tx.SCard(ctx, subscribersKey).Result()
		if err != nil {
			return err
		}
		publishers, err := tx.SCard(ctx, publishersKey).Result()
		if err != nil {
			return err
		}
		if subscribers != 0 || publishers != 0 {
			return fmt.Errorf("%w: channel %s has subscribers or publishers", ErrReferenced, name)
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, resourceKey, subscribersKey, publishersKey)
			pipe.SRem(ctx, s.keys.Channels(), name)
			return nil
		})
		return err
	}, resourceKey, subscribersKey, publishersKey)
}

// DeleteSubscriptionGroup removes group metadata and membership and reconciles
// every Subscriber reverse membership edge in the same optimistic transaction.
func (s *RedisStore) DeleteSubscriptionGroup(ctx context.Context, id string) error {
	id = normalizeIdentity(id)
	if id == "" {
		return errors.New("subscription group id is required")
	}
	groupKey := s.keys.SubscriptionGroup(id)
	membersKey := s.keys.SubscriptionGroupSubscribers(id)
	return s.client.Watch(ctx, func(tx *redis.Tx) error {
		members, err := tx.SMembers(ctx, membersKey).Result()
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, groupKey, membersKey)
			for _, subscriberID := range members {
				pipe.SRem(ctx, s.keys.SubscriberGroups(subscriberID), id)
			}
			return nil
		})
		return err
	}, groupKey, membersKey)
}

func normalizeIdentity(value string) string {
	return strings.TrimSpace(value)
}
