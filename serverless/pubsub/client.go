package pubsub

import (
	"net/http"
	"os"

	"github.com/redis/go-redis/v9"
)

const HeaderRedisAuth = "X-Rediscli-Auth"
const HeaderRedisURI = "X-Redis-Uri"
const HeaderRedisSocket = "X-Redis-Socket"

func NewClientFromEnv() *redis.Client {
	return newClient(os.Getenv("REDIS_URI"), os.Getenv("REDIS_SOCKET"), os.Getenv("REDISCLI_AUTH"))
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

	auth := os.Getenv("REDISCLI_AUTH")
	if auth == "" {
		auth = r.Header.Get(HeaderRedisAuth)
	}

	return newClient(addr, socket, auth)
}

func newClient(addr, socket, auth string) *redis.Client {
	opts := &redis.Options{Addr: addr, Password: auth, DB: 0}
	if socket != "" {
		opts.Network = "unix"
		opts.Addr = socket
	}
	return redis.NewClient(opts)
}
