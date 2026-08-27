package routes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"

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
	mu        sync.Mutex
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

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.cacheSet(ctx, "group:"+group.ID, payload, 0); err != nil {
		return err
	}

	catalog, err := s.loadCatalog(ctx)
	if err != nil {
		return err
	}
	for _, id := range catalog {
		if id == group.ID {
			return nil
		}
	}
	catalog = append(catalog, group.ID)
	catalogPayload, err := json.Marshal(catalog)
	if err != nil {
		return err
	}
	return s.cacheSet(ctx, "groups", catalogPayload, 0)
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

func (s *maraiStateStore) listGroups(ctx context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadCatalog(ctx)
}

func (s *maraiStateStore) loadCatalog(ctx context.Context) ([]string, error) {
	payload, err := s.cacheGet(ctx, "groups")
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return []string{}, nil
	}
	var ids []string
	if err := json.Unmarshal(payload, &ids); err != nil {
		return nil, fmt.Errorf("decode group catalog: %w", err)
	}
	return ids, nil
}
