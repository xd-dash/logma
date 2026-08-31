package pubsub

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

// ObserveSubscriberMessage mirrors one message as observed by a Redis Pub/Sub
// subscriber. It is intentionally separate from Runtime.Publish observation so
// callers can compare publisher-side and subscriber-side delivery without
// making either observer authoritative.
func ObserveSubscriberMessage(observer Observer, ctx context.Context, channel, payload string) {
	if observer == nil {
		return
	}
	raw := json.RawMessage(payload)
	if !json.Valid(raw) {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return
		}
		raw = encoded
	}
	observe(observer, ctx, ObservabilityEvent{
		Time:    time.Now().UTC(),
		Kind:    "publication",
		Phase:   "subscriber",
		Status:  "received",
		Channel: channel,
		Payload: raw,
	})
}

// SubscribeObserved is the subscriber-side counterpart to Runtime.Publish
// observation. Redis subscription readiness and reconnect behavior remain owned
// by Subscribe; observation is best-effort and cannot reject or delay delivery.
func SubscribeObserved(ctx context.Context, client *redis.Client, channel string, observer Observer) *Subscriber {
	return Subscribe(ctx, client, channel, func(payload string) {
		ObserveSubscriberMessage(observer, ctx, channel, payload)
	})
}
