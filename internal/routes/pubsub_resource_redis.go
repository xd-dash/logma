package routes

import (
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/internal/pubsubmodel"
)

func newPubSubResourceRedisStore() (pubSubResourceStore, error) {
	addr := strings.TrimSpace(os.Getenv("PUBSUB_RESOURCE_REDIS_URI"))
	if addr == "" {
		return nil, errors.New("PUBSUB_RESOURCE_REDIS_URI is required")
	}

	db := 0
	if raw := strings.TrimSpace(os.Getenv("PUBSUB_RESOURCE_REDIS_DB")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return nil, errors.New("PUBSUB_RESOURCE_REDIS_DB must be a non-negative integer")
		}
		db = parsed
	}

	client := redis.NewClient(&redis.Options{
		Network:  "tcp",
		Addr:     addr,
		Username: os.Getenv("PUBSUB_RESOURCE_REDIS_USERNAME"),
		Password: os.Getenv("PUBSUB_RESOURCE_REDIS_PASSWORD"),
		DB:       db,
	})

	store, err := pubsubmodel.NewRedisStore(client, os.Getenv("FATLINE_SCOPE"))
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	return store, nil
}
