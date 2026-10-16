package pubsubmodel

import "context"

// ChannelIDs returns the durable Channel identities in the current FATLINE
// scope without scanning Redis keys.
func (s *RedisStore) ChannelIDs(ctx context.Context) ([]string, error) {
	return s.sortedMembers(ctx, s.keys.Channels())
}

// CallbackIDs returns the durable Callback identities in the current FATLINE
// scope without scanning Redis keys.
func (s *RedisStore) CallbackIDs(ctx context.Context) ([]string, error) {
	return s.sortedMembers(ctx, s.keys.Callbacks())
}

// SubscriberIDs returns the durable Subscriber identities in the current
// FATLINE scope without scanning Redis keys.
func (s *RedisStore) SubscriberIDs(ctx context.Context) ([]string, error) {
	return s.sortedMembers(ctx, s.keys.Subscribers())
}
