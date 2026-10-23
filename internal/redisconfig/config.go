package redisconfig

import (
 "os"
 "strconv"
 "strings"

 "github.com/redis/go-redis/v9"
)

// OptionsFromEnv resolves the same Redis endpoint for lifecycle and HTTP routes.
// An explicitly configured socket takes precedence over TCP settings.
func OptionsFromEnv() *redis.Options {
 network := strings.TrimSpace(os.Getenv("REDIS_NETWORK"))
 addr := strings.TrimSpace(os.Getenv("REDIS_URI"))
 if socket := strings.TrimSpace(os.Getenv("REDIS_SOCKET")); socket != "" {
  network, addr = "unix", socket
 }
 if network == "" { network = "tcp" }
 if addr == "" {
  if network == "unix" { addr = "/run/redis/redis.sock" } else { addr = "127.0.0.1:6379" }
 }
 db := 0
 if raw := os.Getenv("REDIS_DB"); raw != "" {
  if n, err := strconv.Atoi(raw); err == nil && n >= 0 { db = n }
 }
 return &redis.Options{
  Network: network, Addr: addr, Username: os.Getenv("REDIS_USERNAME"),
  Password: os.Getenv("REDISCLI_AUTH"), DB: db,
 }
}
