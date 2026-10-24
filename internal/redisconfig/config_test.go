package redisconfig

import "testing"

func TestOptionsFromEnv(t *testing.T) {
 cases := []struct { name, network, uri, socket, wantNetwork, wantAddr string }{
  {"tcp", "", "redis-proxy:6379", "", "tcp", "redis-proxy:6379"},
  {"unix", "tcp", "redis-proxy:6379", "/run/redis/redis.sock", "unix", "/run/redis/redis.sock"},
  {"unix default", "unix", "", "", "unix", "/run/redis/redis.sock"},
  {"tcp default", "", "", "", "tcp", "127.0.0.1:6379"},
 }
 for _, tc := range cases {
  t.Run(tc.name, func(t *testing.T) {
   t.Setenv("REDIS_NETWORK", tc.network)
   t.Setenv("REDIS_URI", tc.uri)
   t.Setenv("REDIS_SOCKET", tc.socket)
   t.Setenv("REDIS_USERNAME", "logma")
   t.Setenv("REDISCLI_AUTH", "test-secret")
   t.Setenv("REDIS_DB", "2")
   got := OptionsFromEnv()
   if got.Network != tc.wantNetwork || got.Addr != tc.wantAddr || got.Username != "logma" || got.Password != "test-secret" || got.DB != 2 {
    t.Fatalf("unexpected Redis configuration: network=%q addr=%q user=%q db=%d", got.Network, got.Addr, got.Username, got.DB)
   }
  })
 }
}
