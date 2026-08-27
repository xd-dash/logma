package routes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/redis/go-redis/v9"
)

const (
	defaultMaraiCacheDB        = 1
	defaultMaraiKeyID          = "logma"
	defaultMaraiNamespace      = "logma"
	defaultCallbackTokenScheme = "Bearer"
)

type callbackSecret struct {
	URL         string `json:"url"`
	AccessToken string `json:"accessToken,omitempty"`
	TokenScheme string `json:"tokenScheme,omitempty"`
}

type storedSubscription struct {
	Version  int            `json:"version"`
	ID       string         `json:"id"`
	Channel  string         `json:"channel"`
	Callback callbackSecret `json:"callback"`
}

type storedGroup struct {
	Version       int                  `json:"version"`
	ID            string               `json:"id"`
	Subscriptions []storedSubscription `json:"subscriptions"`
}

type maraiStateStore struct {
	client    *redis.Client
	db        int
	keyID     string
	namespace string
}

func newMaraiStateStore(client *redis.Client) *maraiStateStore {
	db := defaultMaraiCacheDB
	if raw := os.Getenv("MARAI_CACHE_DB"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 1 && parsed <= 12 {
			db = parsed
		}
	}

	keyID := os.Getenv("MARAI_KMS_KEY_ID")
	if keyID == "" {
		keyID = defaultMaraiKeyID
	}
	namespace := os.Getenv("MARAI_CACHE_NAMESPACE")
	if namespace == "" {
		namespace = defaultMaraiNamespace
	}

	return &maraiStateStore{
		client:    client,
		db:        db,
		keyID:     keyID,
		namespace: namespace,
	}
}

func (s *maraiStateStore) cacheSet(ctx context.Context, key string, value []byte, ttlMS int64) error {
	_, err := s.client.FCall(
		ctx,
		"marai_cache_set",
		[]string{},
		s.keyID,
		s.db,
		s.namespace,
		key,
		value,
		ttlMS,
	).Result()
	if err != nil {
		return fmt.Errorf("marai cache set %q: %w", key, err)
	}
	return nil
}

func (s *maraiStateStore) cacheGet(ctx context.Context, key string) ([]byte, error) {
	result, err := s.client.FCall(
		ctx,
		"marai_cache_get",
		[]string{},
		s.keyID,
		s.db,
		s.namespace,
		key,
	).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("marai cache get %q: %w", key, err)
	}

	switch value := result.(type) {
	case string:
		return []byte(value), nil
	case []byte:
		return value, nil
	default:
		return nil, fmt.Errorf("marai cache get %q returned %T, want bulk string", key, result)
	}
}

func (s *maraiStateStore) cacheDelete(ctx context.Context, key string) error {
	if _, err := s.client.FCall(
		ctx,
		"marai_cache_delete",
		[]string{},
		s.keyID,
		s.db,
		s.namespace,
		key,
	).Result(); err != nil {
		return fmt.Errorf("marai cache delete %q: %w", key, err)
	}
	return nil
}

func (s *maraiStateStore) saveActive(ctx context.Context, sub storedSubscription) error {
	if sub.Version == 0 {
		sub.Version = 1
	}
	payload, err := json.Marshal(sub)
	if err != nil {
		return err
	}
	return s.cacheSet(ctx, "active:"+sub.ID, payload, 0)
}

func (s *maraiStateStore) deleteActive(ctx context.Context, id string) error {
	return s.cacheDelete(ctx, "active:"+id)
}

func (s *maraiStateStore) indexAdd(ctx context.Context, index, member string) error {
	if _, err := s.client.FCall(
		ctx,
		"marai_index_add",
		[]string{},
		s.keyID,
		s.db,
		s.namespace,
		index,
		member,
	).Result(); err != nil {
		return fmt.Errorf("marai index add %q/%q: %w", index, member, err)
	}
	return nil
}

func (s *maraiStateStore) indexRemove(ctx context.Context, index, member string) error {
	if _, err := s.client.FCall(
		ctx,
		"marai_index_remove",
		[]string{},
		s.keyID,
		s.db,
		s.namespace,
		index,
		member,
	).Result(); err != nil {
		return fmt.Errorf("marai index remove %q/%q: %w", index, member, err)
	}
	return nil
}

func (s *maraiStateStore) indexList(ctx context.Context, index string, cursor, count int64) ([]string, int64, error) {
	result, err := s.client.FCall(
		ctx,
		"marai_index_list",
		[]string{},
		s.keyID,
		s.db,
		s.namespace,
		index,
		cursor,
		count,
	).Result()
	if err != nil {
		return nil, 0, fmt.Errorf("marai index list %q: %w", index, err)
	}

	parts, ok := result.([]interface{})
	if !ok || len(parts) != 2 {
		return nil, 0, fmt.Errorf("marai index list %q returned %T, want two-element array", index, result)
	}

	next, ok := parts[0].(int64)
	if !ok {
		return nil, 0, fmt.Errorf("marai index list %q returned cursor %T, want integer", index, parts[0])
	}

	rawMembers, ok := parts[1].([]interface{})
	if !ok {
		return nil, 0, fmt.Errorf("marai index list %q returned members %T, want array", index, parts[1])
	}
	members := make([]string, 0, len(rawMembers))
	for _, raw := range rawMembers {
		switch value := raw.(type) {
		case string:
			members = append(members, value)
		case []byte:
			members = append(members, string(value))
		default:
			return nil, 0, fmt.Errorf("marai index list %q returned member %T, want string", index, raw)
		}
	}
	return members, next, nil
}

func (s *maraiStateStore) saveGroup(ctx context.Context, group storedGroup) error {
	if group.Version == 0 {
		group.Version = 1
	}
	for i := range group.Subscriptions {
		if group.Subscriptions[i].Version == 0 {
			group.Subscriptions[i].Version = 1
		}
	}
	payload, err := json.Marshal(group)
	if err != nil {
		return err
	}
	if err := s.cacheSet(ctx, "group:"+group.ID, payload, 0); err != nil {
		return err
	}
	if err := s.indexAdd(ctx, "groups", group.ID); err != nil {
		_ = s.cacheDelete(context.Background(), "group:"+group.ID)
		return err
	}
	return nil
}

func (s *maraiStateStore) loadGroup(ctx context.Context, id string) (storedGroup, bool, error) {
	payload, err := s.cacheGet(ctx, "group:"+id)
	if err != nil {
		return storedGroup{}, false, err
	}
	if payload == nil {
		return storedGroup{}, false, nil
	}
	var group storedGroup
	if err := json.Unmarshal(payload, &group); err != nil {
		return storedGroup{}, false, fmt.Errorf("decode group %q: %w", id, err)
	}
	return group, true, nil
}

func (s *maraiStateStore) listGroups(ctx context.Context, cursor, count int64) ([]string, int64, error) {
	return s.indexList(ctx, "groups", cursor, count)
}
