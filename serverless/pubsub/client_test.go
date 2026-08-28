package pubsub

import (
	"net/http/httptest"
	"testing"
)

func TestNewClientFromEnvUnixSocket(t *testing.T) {
	t.Setenv("REDIS_URI", "127.0.0.1:6379")
	t.Setenv("REDIS_SOCKET", "/run/redis/redis.sock")
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
