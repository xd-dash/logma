package pubsubmodel

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// RedisStore persists the Logma Pub/Sub control-plane graph without JSON
// documents. HTTP serialization is intentionally independent of this storage
// representation. The store uses the canonical Fatline v2 resource grammar;
// there is no dual-read or legacy-key fallback on this branch.
type RedisStore struct {
	client redis.UniversalClient
	keys   ResourceKeysV2
}

func NewRedisStore(client redis.UniversalClient, scope string) (*RedisStore, error) {
	if client == nil {
		return nil, errors.New("redis client is required")
	}
	keys, err := NewResourceKeysV2(scope)
	if err != nil {
		return nil, err
	}
	return &RedisStore{client: client, keys: keys}, nil
}

func (s *RedisStore) PutChannel(ctx context.Context, channel Channel) error {
	if err := channel.Validate(); err != nil { return err }
	name := strings.TrimSpace(channel.Name)
	_, err := s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, s.keys.Channel(name), map[string]any{"name": name})
		pipe.SAdd(ctx, s.keys.Channels(), name)
		return nil
	})
	return err
}

func (s *RedisStore) PutCallback(ctx context.Context, callback Callback) error {
	if err := callback.Validate(); err != nil { return err }
	id := strings.TrimSpace(callback.ID)
	resourceKey := s.keys.Callback(id)
	urlsKey := s.keys.CallbackURLs(id)
	_, err := s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, resourceKey)
		fields := map[string]any{"id": id, "type": string(callback.Type)}
		switch callback.Type {
		case CallbackWebhook:
			pipe.Del(ctx, urlsKey)
			urls := uniqueTrimmed(callback.Webhook.URLs())
			if len(urls) > 0 { pipe.SAdd(ctx, urlsKey, stringsToAny(urls)...) }
		case CallbackLua:
			fields["name"] = strings.TrimSpace(callback.Lua.Name)
			pipe.Del(ctx, urlsKey)
		}
		pipe.HSet(ctx, resourceKey, fields)
		pipe.SAdd(ctx, s.keys.Callbacks(), id)
		return nil
	})
	return err
}

func (s *RedisStore) PutSubscriber(ctx context.Context, subscriber Subscriber) error {
	if err := subscriber.Validate(); err != nil { return err }
	id := strings.TrimSpace(subscriber.ID)
	channel := strings.TrimSpace(subscriber.Channel)
	callbacks := uniqueTrimmed(subscriber.CallbackIDs)
	if len(callbacks) == 0 { return errors.New("subscriber requires at least one callback") }
	subscriberKey := s.keys.Subscriber(id)
	callbacksKey := s.keys.SubscriberCallbacks(id)
	watchKeys := []string{subscriberKey, callbacksKey, s.keys.Channel(channel)}
	for _, callbackID := range callbacks { watchKeys = append(watchKeys, s.keys.Callback(callbackID)) }
	return s.watch(ctx, func(tx *redis.Tx) error {
		oldChannel, err := tx.HGet(ctx, subscriberKey, "channel").Result()
		if err != nil && err != redis.Nil { return err }
		oldCallbacks, err := tx.SMembers(ctx, callbacksKey).Result()
		if err != nil { return err }
		references := []string{s.keys.Channel(channel)}
		for _, callbackID := range callbacks { references = append(references, s.keys.Callback(callbackID)) }
		exists, err := tx.Exists(ctx, references...).Result()
		if err != nil { return err }
		if exists != int64(len(references)) { return fmt.Errorf("%w: subscriber %s references missing channel or callback", ErrMissingReference, id) }
		oldCallbackSet := stringSet(oldCallbacks)
		newCallbackSet := stringSet(callbacks)
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, subscriberKey, map[string]any{"id": id, "channel": channel})
			pipe.Del(ctx, callbacksKey)
			pipe.SAdd(ctx, callbacksKey, stringsToAny(callbacks)...)
			pipe.SAdd(ctx, s.keys.Subscribers(), id)
			if oldChannel != "" && oldChannel != channel { pipe.SRem(ctx, s.keys.ChannelSubscribers(oldChannel), id) }
			pipe.SAdd(ctx, s.keys.ChannelSubscribers(channel), id)
			for callbackID := range oldCallbackSet { if _, retained := newCallbackSet[callbackID]; !retained { pipe.SRem(ctx, s.keys.CallbackSubscribers(callbackID), id) } }
			for _, callbackID := range callbacks { pipe.SAdd(ctx, s.keys.CallbackSubscribers(callbackID), id) }
			return nil
		})
		return err
	}, watchKeys...)
}

func (s *RedisStore) PutPublisher(ctx context.Context, publisher Publisher) error {
	if err := publisher.Validate(); err != nil { return err }
	id := strings.TrimSpace(publisher.ID)
	channel := strings.TrimSpace(publisher.Channel)
	publisherKey := s.keys.Publisher(id)
	channelKey := s.keys.Channel(channel)
	return s.watch(ctx, func(tx *redis.Tx) error {
		oldChannel, err := tx.HGet(ctx, publisherKey, "channel").Result()
		if err != nil && err != redis.Nil { return err }
		exists, err := tx.Exists(ctx, channelKey).Result()
		if err != nil { return err }
		if exists != 1 { return fmt.Errorf("%w: publisher %s references missing channel", ErrMissingReference, id) }
		fields := map[string]any{"id": id, "channel": channel, "type": strings.TrimSpace(publisher.Type)}
		if len(publisher.Config) > 0 { fields["config"] = []byte(publisher.Config) }
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, publisherKey)
			pipe.HSet(ctx, publisherKey, fields)
			pipe.SAdd(ctx, s.keys.Publishers(), id)
			if oldChannel != "" && oldChannel != channel { pipe.SRem(ctx, s.keys.ChannelPublishers(oldChannel), id) }
			pipe.SAdd(ctx, s.keys.ChannelPublishers(channel), id)
			return nil
		})
		return err
	}, publisherKey, channelKey)
}

func (s *RedisStore) PutSubscriptionGroup(ctx context.Context, group SubscriptionGroup) error {
	if err := group.Validate(); err != nil { return err }
	id := strings.TrimSpace(group.ID)
	subscribers := uniqueTrimmed(group.SubscriberIDs)
	groupKey := s.keys.SubscriptionGroup(id)
	membersKey := s.keys.SubscriptionGroupSubscribers(id)
	watchKeys := []string{groupKey, membersKey}
	for _, subscriberID := range subscribers { watchKeys = append(watchKeys, s.keys.Subscriber(subscriberID), s.keys.SubscriberGroups(subscriberID)) }
	return s.watch(ctx, func(tx *redis.Tx) error {
		oldSubscribers, err := tx.SMembers(ctx, membersKey).Result(); if err != nil { return err }
		if len(subscribers) > 0 {
			references := make([]string, 0, len(subscribers)); for _, subscriberID := range subscribers { references = append(references, s.keys.Subscriber(subscriberID)) }
			exists, err := tx.Exists(ctx, references...).Result(); if err != nil { return err }
			if exists != int64(len(references)) { return fmt.Errorf("%w: subscription group %s references missing subscriber", ErrMissingReference, id) }
		}
		oldSet, newSet := stringSet(oldSubscribers), stringSet(subscribers)
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, groupKey, map[string]any{"id": id})
			pipe.Del(ctx, membersKey)
			if len(subscribers) > 0 { pipe.SAdd(ctx, membersKey, stringsToAny(subscribers)...) }
			pipe.SAdd(ctx, s.keys.SubscriptionGroups(), id)
			for subscriberID := range oldSet { if _, retained := newSet[subscriberID]; !retained { pipe.SRem(ctx, s.keys.SubscriberGroups(subscriberID), id) } }
			for _, subscriberID := range subscribers { pipe.SAdd(ctx, s.keys.SubscriberGroups(subscriberID), id) }
			return nil
		})
		return err
	}, watchKeys...)
}

func uniqueTrimmed(values []string) []string {
	seen := make(map[string]struct{}, len(values)); out := make([]string, 0, len(values))
	for _, value := range values { value = strings.TrimSpace(value); if value == "" { continue }; if _, exists := seen[value]; exists { continue }; seen[value] = struct{}{}; out = append(out, value) }
	return out
}
func stringsToAny(values []string) []any { out := make([]any, 0, len(values)); for _, value := range values { out = append(out, value) }; return out }
func stringSet(values []string) map[string]struct{} { out := make(map[string]struct{}, len(values)); for _, value := range values { out[value] = struct{}{} }; return out }
