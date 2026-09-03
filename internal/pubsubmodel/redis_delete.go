package pubsubmodel

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

func (s *RedisStore) DeleteSubscriber(ctx context.Context, id string) error {
	id = normalizeIdentity(id)
	if id == "" {
		return errors.New("subscriber id is required")
	}
	subscriberKey := s.keys.Subscriber(id)
	callbacksKey := s.keys.SubscriberCallbacks(id)
	groupsKey := s.keys.SubscriberGroups(id)
	return s.watch(ctx, func(tx *redis.Tx) error {
		channel, err := tx.HGet(ctx, subscriberKey, "channel").Result()
		if err == redis.Nil {
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

// DeletePublisher removes a Publisher and reconciles its Channel reverse edge
// and discovery index. Weak PublisherGroup membership does not block deletion;
// group declarations intentionally retain the Publisher identity for later
// best-effort resolution.
func (s *RedisStore) DeletePublisher(ctx context.Context, id string) error {
	id = normalizeIdentity(id)
	if id == "" {
		return errors.New("publisher id is required")
	}
	publisherKey := s.keys.Publisher(id)
	groupsKey := s.keys.PublisherGroupsForPublisher(id)
	return s.watch(ctx, func(tx *redis.Tx) error {
		channel, err := tx.HGet(ctx, publisherKey, "channel").Result()
		if err == redis.Nil {
			_, cleanupErr := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Del(ctx, groupsKey)
				pipe.SRem(ctx, s.keys.Publishers(), id)
				return nil
			})
			return cleanupErr
		}
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, publisherKey, groupsKey)
			pipe.SRem(ctx, s.keys.Publishers(), id)
			if channel != "" {
				pipe.SRem(ctx, s.keys.ChannelPublishers(channel), id)
			}
			return nil
		})
		return err
	}, publisherKey, groupsKey)
}

func (s *RedisStore) DeleteCallback(ctx context.Context, id string) error {
	id = normalizeIdentity(id)
	if id == "" {
		return errors.New("callback id is required")
	}
	resourceKey := s.keys.Callback(id)
	subscribersKey := s.keys.CallbackSubscribers(id)
	urlsKey := s.keys.CallbackURLs(id)
	return s.watch(ctx, func(tx *redis.Tx) error {
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

func (s *RedisStore) DeleteChannel(ctx context.Context, name string) error {
	name = normalizeIdentity(name)
	if name == "" {
		return errors.New("channel name is required")
	}
	resourceKey := s.keys.Channel(name)
	subscribersKey := s.keys.ChannelSubscribers(name)
	publishersKey := s.keys.ChannelPublishers(name)
	return s.watch(ctx, func(tx *redis.Tx) error {
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

func (s *RedisStore) DeleteSubscriptionGroup(ctx context.Context, id string) error {
	id = normalizeIdentity(id)
	if id == "" {
		return errors.New("subscription group id is required")
	}
	_, err := s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, s.keys.SubscriptionGroup(id), s.keys.SubscriptionGroupSubscribers(id))
		pipe.SRem(ctx, s.keys.SubscriptionGroups(), id)
		return nil
	})
	return err
}

func normalizeIdentity(value string) string {
	return strings.TrimSpace(value)
}
