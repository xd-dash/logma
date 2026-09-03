package pubsubmodel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/redis/go-redis/v9"
)

func (s *RedisStore) GetChannel(ctx context.Context, name string) (Channel, error) {
	name = normalizeIdentity(name)
	if name == "" {
		return Channel{}, errors.New("channel name is required")
	}
	fields, err := s.client.HGetAll(ctx, s.keys.Channel(name)).Result()
	if err != nil {
		return Channel{}, err
	}
	if len(fields) == 0 {
		return Channel{}, fmt.Errorf("%w: channel %s", ErrNotFound, name)
	}
	channel := Channel{Name: fields["name"]}
	if err := channel.Validate(); err != nil {
		return Channel{}, fmt.Errorf("decode channel %s: %w", name, err)
	}
	return channel, nil
}

func (s *RedisStore) GetCallback(ctx context.Context, id string) (Callback, error) {
	id = normalizeIdentity(id)
	if id == "" {
		return Callback{}, errors.New("callback id is required")
	}
	fields, err := s.client.HGetAll(ctx, s.keys.Callback(id)).Result()
	if err != nil {
		return Callback{}, err
	}
	if len(fields) == 0 {
		return Callback{}, fmt.Errorf("%w: callback %s", ErrNotFound, id)
	}

	callback := Callback{ID: fields["id"], Type: CallbackType(fields["type"])}
	switch callback.Type {
	case CallbackWebhook:
		urls, err := s.client.SMembers(ctx, s.keys.CallbackURLs(id)).Result()
		if err != nil {
			return Callback{}, err
		}
		sort.Strings(urls)
		callback.Webhook = &WebhookCallback{CallbackURLs: urls}
	case CallbackLua:
		callback.Lua = &LuaCallback{Name: fields["name"]}
	}
	if err := callback.Validate(); err != nil {
		return Callback{}, fmt.Errorf("decode callback %s: %w", id, err)
	}
	return callback, nil
}

func (s *RedisStore) GetSubscriber(ctx context.Context, id string) (Subscriber, error) {
	id = normalizeIdentity(id)
	if id == "" {
		return Subscriber{}, errors.New("subscriber id is required")
	}
	fields, err := s.client.HGetAll(ctx, s.keys.Subscriber(id)).Result()
	if err != nil {
		return Subscriber{}, err
	}
	if len(fields) == 0 {
		return Subscriber{}, fmt.Errorf("%w: subscriber %s", ErrNotFound, id)
	}
	callbacks, err := s.client.SMembers(ctx, s.keys.SubscriberCallbacks(id)).Result()
	if err != nil {
		return Subscriber{}, err
	}
	sort.Strings(callbacks)
	subscriber := Subscriber{ID: fields["id"], Channel: fields["channel"], CallbackIDs: callbacks}
	if err := subscriber.Validate(); err != nil {
		return Subscriber{}, fmt.Errorf("decode subscriber %s: %w", id, err)
	}
	return subscriber, nil
}

func (s *RedisStore) GetPublisher(ctx context.Context, id string) (Publisher, error) {
	id = normalizeIdentity(id)
	if id == "" {
		return Publisher{}, errors.New("publisher id is required")
	}
	fields, err := s.client.HGetAll(ctx, s.keys.Publisher(id)).Result()
	if err != nil {
		return Publisher{}, err
	}
	if len(fields) == 0 {
		return Publisher{}, fmt.Errorf("%w: publisher %s", ErrNotFound, id)
	}
	publisher := Publisher{ID: fields["id"], Channel: fields["channel"], Type: fields["type"]}
	if config := fields["config"]; config != "" {
		publisher.Config = json.RawMessage(config)
	}
	if err := publisher.Validate(); err != nil {
		return Publisher{}, fmt.Errorf("decode publisher %s: %w", id, err)
	}
	return publisher, nil
}

func (s *RedisStore) GetSubscriptionGroup(ctx context.Context, id string) (SubscriptionGroup, error) {
	id = normalizeIdentity(id)
	if id == "" {
		return SubscriptionGroup{}, errors.New("subscription group id is required")
	}
	fields, err := s.client.HGetAll(ctx, s.keys.SubscriptionGroup(id)).Result()
	if err != nil {
		return SubscriptionGroup{}, err
	}
	if len(fields) == 0 {
		return SubscriptionGroup{}, fmt.Errorf("%w: subscription group %s", ErrNotFound, id)
	}
	subscribers, err := s.client.SMembers(ctx, s.keys.SubscriptionGroupSubscribers(id)).Result()
	if err != nil {
		return SubscriptionGroup{}, err
	}
	sort.Strings(subscribers)
	group := SubscriptionGroup{ID: fields["id"], SubscriberIDs: subscribers}
	if err := group.Validate(); err != nil {
		return SubscriptionGroup{}, fmt.Errorf("decode subscription group %s: %w", id, err)
	}
	return group, nil
}

func (s *RedisStore) ChannelSubscriberIDs(ctx context.Context, channel string) ([]string, error) {
	channel = normalizeIdentity(channel)
	if channel == "" {
		return nil, errors.New("channel name is required")
	}
	return s.sortedMembers(ctx, s.keys.ChannelSubscribers(channel))
}

func (s *RedisStore) ChannelPublisherIDs(ctx context.Context, channel string) ([]string, error) {
	channel = normalizeIdentity(channel)
	if channel == "" {
		return nil, errors.New("channel name is required")
	}
	return s.sortedMembers(ctx, s.keys.ChannelPublishers(channel))
}

func (s *RedisStore) CallbackSubscriberIDs(ctx context.Context, callbackID string) ([]string, error) {
	callbackID = normalizeIdentity(callbackID)
	if callbackID == "" {
		return nil, errors.New("callback id is required")
	}
	return s.sortedMembers(ctx, s.keys.CallbackSubscribers(callbackID))
}

func (s *RedisStore) SubscriberGroupIDs(ctx context.Context, subscriberID string) ([]string, error) {
	subscriberID = normalizeIdentity(subscriberID)
	if subscriberID == "" {
		return nil, errors.New("subscriber id is required")
	}
	return s.sortedMembers(ctx, s.keys.SubscriberGroups(subscriberID))
}

func (s *RedisStore) sortedMembers(ctx context.Context, key string) ([]string, error) {
	members, err := s.client.SMembers(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	sort.Strings(members)
	return members, nil
}
