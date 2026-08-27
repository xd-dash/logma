package routes

import (
	"os"
	"strconv"

	"github.com/redis/go-redis/v9"
)

func redisOptionsFromEnv() *redis.Options {
	network := os.Getenv("REDIS_NETWORK")
	addr := os.Getenv("REDIS_URI")

	if socket := os.Getenv("REDIS_SOCKET"); socket != "" {
		network = "unix"
		addr = socket
	}
	if network == "" {
		network = "tcp"
	}
	if addr == "" {
		if network == "unix" {
			addr = "/run/redis/redis.sock"
		} else {
			addr = "127.0.0.1:6379"
		}
	}

	db := 1
	if raw := os.Getenv("REDIS_DB"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 1 && parsed <= 15 {
			db = parsed
		}
	}

	return &redis.Options{
		Network:  network,
		Addr:     addr,
		Username: os.Getenv("REDIS_USERNAME"),
		Password: os.Getenv("REDISCLI_AUTH"),
		DB:       db,
	}
}
