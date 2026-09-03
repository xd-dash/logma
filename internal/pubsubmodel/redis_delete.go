package pubsubmodel

import (
	"context"
	"errors"
	"strings"

	"github.com/redis/go-redis/v9"
)

// DeleteSubscriber removes a Subscriber and reconciles its reverse Channel,
// Callback, and discovery indexes in the same optimistic Redis transaction.
func (s *RedisStore) DeleteSubscriber(ctx context.Context, id string) error {
	id = normalizeIdentity(id)
	if id == "" {
		return errors.New("subscriber id is required")
	}

	subscriberKey := s.keys.Subscriber(id)
	callbacksKey := s.keys.SubscriberCallbacks(id)

	return s.client.Watch(ctx, func(tx *redis.Tx) error {
		channel, err := tx.HGet(ctx, subscriberKey, "channel").Result()
		if err == redis.Nil {
			return nil
		}
		if err != nil {
			return err
		}

		callbacks, err := tx.SMembers(ctx, callbacksKey).Result()
		if err != nil {
			return err
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, subscriberKey, callbacksKey)
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
	}, subscriberKey, callbacksKey)
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
			return errors.New("callback is still referenced by subscribers")
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
			return errors.New("channel is still referenced by subscribers or publishers")
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, resourceKey, subscribersKey, publishersKey)
			pipe.SRem(ctx, s.keys.Channels(), name)
			return nil
		})
		return err
	}, resourceKey, subscribersKey, publishersKey)
}

// DeleteSubscriptionGroup removes group metadata and membership. Group
// membership has no reverse index, so no other graph resources are mutated.
func (s *RedisStore) DeleteSubscriptionGroup(ctx context.Context, id string) error {
	id = normalizeIdentity(id)
	if id == "" {
		return errors.New("subscription group id is required")
	}
	return s.client.Del(
		ctx,
		s.keys.SubscriptionGroup(id),
		s.keys.SubscriptionGroupSubscribers(id),
	).Err()
}

func normalizeIdentity(value string) string {
	return strings.TrimSpace(value)
}
