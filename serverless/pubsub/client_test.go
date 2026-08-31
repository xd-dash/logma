package pubsub

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestNewClientFromEnvUnixSocket(t *testing.T) {
	t.Setenv("REDIS_URI", "127.0.0.1:6379")
	t.Setenv("REDIS_SOCKET", "/run/redis/redis.sock")
	t.Setenv("REDISCLI_AUTH_FILE", "")
	t.Setenv("REDISCLI_AUTH", "secret")

	client := NewClientFromEnv()
	opt := client.Options()
	if opt.Network != "unix" {
		t.Fatalf("network = %q, want unix", opt.Network)
	}
	if opt.Addr != "/run/redis/redis.sock" {
		t.Fatalf("addr = %q, want unix socket", opt.Addr)
	}
	if opt.Password != "secret" {
		t.Fatalf("password not preserved")
	}
}

func TestNewClientFromRequestUnixSocketHeader(t *testing.T) {
	t.Setenv("REDIS_URI", "")
	t.Setenv("REDIS_SOCKET", "")
	t.Setenv("REDISCLI_AUTH_FILE", "")
	t.Setenv("REDISCLI_AUTH", "")

	req := httptest.NewRequest("GET", "http://example.test", nil)
	req.Header.Set(HeaderRedisURI, "127.0.0.1:6379")
	req.Header.Set(HeaderRedisSocket, "/tmp/redis.sock")
	req.Header.Set(HeaderRedisAuth, "secret")

	client := NewClientFromRequest(req)
	opt := client.Options()
	if opt.Network != "unix" {
		t.Fatalf("network = %q, want unix", opt.Network)
	}
	if opt.Addr != "/tmp/redis.sock" {
		t.Fatalf("addr = %q, want unix socket", opt.Addr)
	}
	if opt.Password != "secret" {
		t.Fatalf("password not preserved")
	}
}

func TestRedisAuthFromEnvPrefersFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "redis-auth")
	if err := os.WriteFile(path, []byte("from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REDISCLI_AUTH_FILE", path)
	t.Setenv("REDISCLI_AUTH", "from-env")

	if got := redisAuthFromEnv(); got != "from-file" {
		t.Fatalf("redisAuthFromEnv() = %q, want file-backed credential", got)
	}
}

func TestRedisAuthFromEnvFallsBackToValue(t *testing.T) {
	t.Setenv("REDISCLI_AUTH_FILE", "")
	t.Setenv("REDISCLI_AUTH", "from-env")

	if got := redisAuthFromEnv(); got != "from-env" {
		t.Fatalf("redisAuthFromEnv() = %q, want env fallback", got)
	}
}
