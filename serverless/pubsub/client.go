package pubsub

import (
	"net/http"
	"os"
	"strings"

	"github.com/redis/go-redis/v9"
)

const HeaderRedisAuth = "X-Rediscli-Auth"
const HeaderRedisUsername = "X-Redis-Username"
const HeaderRedisURI = "X-Redis-Uri"
const HeaderRedisSocket = "X-Redis-Socket"

func redisAuthFromEnv() string {
	if path := strings.TrimSpace(os.Getenv("REDISCLI_AUTH_FILE")); path != "" {
		if value, err := os.ReadFile(path); err == nil {
			return strings.TrimSpace(string(value))
		}
	}
	return os.Getenv("REDISCLI_AUTH")
}

func NewClientFromEnv() *redis.Client {
	return newClient(os.Getenv("REDIS_URI"), os.Getenv("REDIS_SOCKET"), os.Getenv("REDIS_USERNAME"), redisAuthFromEnv())
}

func NewClientFromRequest(r *http.Request) *redis.Client {
	addr := os.Getenv("REDIS_URI")
	if addr == "" {
		addr = r.Header.Get(HeaderRedisURI)
	}
	socket := os.Getenv("REDIS_SOCKET")
	if socket == "" {
		socket = r.Header.Get(HeaderRedisSocket)
	}
	username := os.Getenv("REDIS_USERNAME")
	if username == "" {
		username = r.Header.Get(HeaderRedisUsername)
	}
	auth := redisAuthFromEnv()
	if auth == "" {
		auth = r.Header.Get(HeaderRedisAuth)
	}
	return newClient(addr, socket, username, auth)
}

func newClient(addr, socket, username, auth string) *redis.Client {
	opts := &redis.Options{Addr: addr, Username: username, Password: auth, DB: 0}
	if socket != "" {
		opts.Network = "unix"
		opts.Addr = socket
	}
	return redis.NewClient(opts)
}
