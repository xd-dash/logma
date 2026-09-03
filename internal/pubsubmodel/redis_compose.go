package pubsubmodel

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// CreateWebhookSubscription atomically creates the common operator-level
// Channel + webhook Callback + Subscriber composition. Channel creation is
// ensure-style: an existing canonical Channel is reused. Callback and
// Subscriber identities are create-only so the convenience operation can never
// overwrite an independently managed resource after a late conflict.
func (s *RedisStore) CreateWebhookSubscription(ctx context.Context, channel Channel, callback Callback, subscriber Subscriber) error {
	if err := channel.Validate(); err != nil {
		return err
	}
	if err := callback.Validate(); err != nil {
		return err
	}
	if err := subscriber.Validate(); err != nil {
		return err
	}
	channelName := normalizeIdentity(channel.Name)
	callbackID := normalizeIdentity(callback.ID)
	subscriberID := normalizeIdentity(subscriber.ID)
	if callback.Type != CallbackWebhook || callback.Webhook == nil {
		return fmt.Errorf("simple subscription callback must be webhook")
	}
	if normalizeIdentity(subscriber.Channel) != channelName {
		return fmt.Errorf("simple subscription Subscriber channel must match Channel")
	}
	callbacks := uniqueTrimmed(subscriber.CallbackIDs)
	if len(callbacks) != 1 || callbacks[0] != callbackID {
		return fmt.Errorf("simple subscription Subscriber must reference exactly its Callback")
	}

	channelKey := s.keys.Channel(channelName)
	callbackKey := s.keys.Callback(callbackID)
	callbackURLsKey := s.keys.CallbackURLs(callbackID)
	subscriberKey := s.keys.Subscriber(subscriberID)
	subscriberCallbacksKey := s.keys.SubscriberCallbacks(subscriberID)
	watchKeys := []string{
		channelKey,
		s.keys.ChannelSubscribers(channelName),
		callbackKey,
		callbackURLsKey,
		s.keys.CallbackSubscribers(callbackID),
		subscriberKey,
		subscriberCallbacksKey,
	}

	return s.watch(ctx, func(tx *redis.Tx) error {
		callbackExists, err := tx.Exists(ctx, callbackKey).Result()
		if err != nil {
			return err
		}
		if callbackExists != 0 {
			return fmt.Errorf("%w: callback %s", ErrAlreadyExists, callbackID)
		}
		subscriberExists, err := tx.Exists(ctx, subscriberKey).Result()
		if err != nil {
			return err
		}
		if subscriberExists != 0 {
			return fmt.Errorf("%w: subscriber %s", ErrAlreadyExists, subscriberID)
		}

		channelExists, err := tx.Exists(ctx, channelKey).Result()
		if err != nil {
			return err
		}
		if channelExists != 0 {
			storedChannelName, err := tx.HGet(ctx, channelKey, "name").Result()
			if err != nil {
				return fmt.Errorf("decode existing channel %s: %w", channelName, err)
			}
			if normalizeIdentity(storedChannelName) != channelName {
				return fmt.Errorf("channel %s has mismatched stored identity %q", channelName, storedChannelName)
			}
		}

		urls := uniqueTrimmed(callback.Webhook.URLs())
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			if channelExists == 0 {
				pipe.HSet(ctx, channelKey, map[string]any{"name": channelName})
				pipe.SAdd(ctx, s.keys.Channels(), channelName)
			}

			pipe.HSet(ctx, callbackKey, map[string]any{"id": callbackID, "type": string(CallbackWebhook)})
			pipe.SAdd(ctx, callbackURLsKey, stringsToAny(urls)...)
			pipe.SAdd(ctx, s.keys.Callbacks(), callbackID)

			pipe.HSet(ctx, subscriberKey, map[string]any{"id": subscriberID, "channel": channelName})
			pipe.SAdd(ctx, subscriberCallbacksKey, callbackID)
			pipe.SAdd(ctx, s.keys.Subscribers(), subscriberID)
			pipe.SAdd(ctx, s.keys.ChannelSubscribers(channelName), subscriberID)
			pipe.SAdd(ctx, s.keys.CallbackSubscribers(callbackID), subscriberID)
			return nil
		})
		return err
	}, watchKeys...)
}
