package pubsub

import (
	"net/http"
	"os"

	"github.com/redis/go-redis/v9"
)

const HeaderRedisAuth = "X-Rediscli-Auth"
const HeaderRedisUsername = "X-Redis-Username"
const HeaderRedisURI = "X-Redis-Uri"
const HeaderRedisSocket = "X-Redis-Socket"

func NewClientFromEnv() *redis.Client {
	return newClient(os.Getenv("REDIS_URI"), os.Getenv("REDIS_SOCKET"), os.Getenv("REDIS_USERNAME"), os.Getenv("REDISCLI_AUTH"))
}

func NewClientFromRequest(r *http.Request) *redis.Client {
	addr := os.Getenv("REDIS_URI")
	if addr == "" { addr = r.Header.Get(HeaderRedisURI) }
	socket := os.Getenv("REDIS_SOCKET")
	if socket == "" { socket = r.Header.Get(HeaderRedisSocket) }
	username := os.Getenv("REDIS_USERNAME")
	if username == "" { username = r.Header.Get(HeaderRedisUsername) }
	auth := os.Getenv("REDISCLI_AUTH")
	if auth == "" { auth = r.Header.Get(HeaderRedisAuth) }
	return newClient(addr, socket, username, auth)
}

func newClient(addr, socket, username, auth string) *redis.Client {
	opts := &redis.Options{Addr: addr, Username: username, Password: auth, DB: 0}
	if socket != "" { opts.Network = "unix"; opts.Addr = socket }
	return redis.NewClient(opts)
}
