package pubsub

import (
	"net/http"
	"os"

	"github.com/redis/go-redis/v9"
)

const HeaderRedisAuth = "X-Rediscli-Auth"
const HeaderRedisURI = "X-Redis-Uri"

func NewClientFromEnv() *redis.Client {
	return newClient(os.Getenv("REDIS_URI"), os.Getenv("REDISCLI_AUTH"))
}

func NewClientFromRequest(r *http.Request) *redis.Client {
	addr := os.Getenv("REDIS_URI")
	if addr == "" {
		addr = r.Header.Get(HeaderRedisURI)
	}

	auth := os.Getenv("REDISCLI_AUTH")
	if auth == "" {
		auth = r.Header.Get(HeaderRedisAuth)
	}

	return newClient(addr, auth)
}

func newClient(addr, auth string) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: addr, Password: auth, DB: 0})
}
