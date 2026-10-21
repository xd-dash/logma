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
 return newPubSubResourceRedisStoreWithScope(os.Getenv("FATLINE_SCOPE"))
}

func newPubSubResourceRedisStoreForProject(project string) (pubSubResourceStore, error) {
 if project == "" || strings.ContainsAny(project, `/\\: `) || project == "." || project == ".." { return nil, errors.New("invalid verified project identity") }
 scope := strings.TrimSpace(os.Getenv("FATLINE_SCOPE"))
 if scope == "" { return nil, errors.New("FATLINE_SCOPE is required for project-scoped storage") }
 return newPubSubResourceRedisStoreWithScope(scope + ":project:" + project)
}

func newPubSubResourceRedisStoreWithScope(scope string) (pubSubResourceStore, error) {
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
		Network:  redisNetwork(addr),
		Addr:     addr,
		Username: os.Getenv("PUBSUB_RESOURCE_REDIS_USERNAME"),
		Password: os.Getenv("PUBSUB_RESOURCE_REDIS_PASSWORD"),
		DB:       db,
	})

	store, err := pubsubmodel.NewRedisStore(client, scope)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	return store, nil
}

func redisNetwork(addr string) string { if strings.HasPrefix(addr, "/") { return "unix" }; return "tcp" }
