package routes

import "testing"

func TestRedisOptionsFromEnvDefaultsToTCP(t *testing.T) {
	t.Setenv("REDIS_NETWORK", "")
	t.Setenv("REDIS_SOCKET", "")
	t.Setenv("REDIS_URI", "redis.example:6379")
	t.Setenv("REDISCLI_AUTH", "secret")
	t.Setenv("REDIS_USERNAME", "logma")
	t.Setenv("LOGMA_SCOPE_DB", "")
	t.Setenv("REDIS_DB", "3")

	opts := redisOptionsFromEnv()
	if opts.Network != "tcp" || opts.Addr != "redis.example:6379" || opts.Username != "logma" || opts.Password != "secret" || opts.DB != 3 {
		t.Fatalf("unexpected options: %+v", opts)
	}
}

func TestRedisOptionsFromEnvSocketWins(t *testing.T) {
	t.Setenv("REDIS_NETWORK", "tcp")
	t.Setenv("REDIS_URI", "ignored:6379")
	t.Setenv("REDIS_SOCKET", "/run/logma/redis.sock")
	t.Setenv("LOGMA_SCOPE_DB", "")
	t.Setenv("REDIS_DB", "0")

	opts := redisOptionsFromEnv()
	if opts.Network != "unix" || opts.Addr != "/run/logma/redis.sock" {
		t.Fatalf("unexpected unix options: %+v", opts)
	}
}
