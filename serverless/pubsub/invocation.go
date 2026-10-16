package pubsub

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/serverless/keyspace"
)

const invocationTTL = 24 * time.Hour

type InvocationInfo struct {
	Service       string
	Revision      string
	Configuration string
	InstanceID    string
	RequestID     string
	Method        string
	Path          string
	RemoteAddr    string
	StartedAt     time.Time
}

func InvocationInfoFromRequest(r *http.Request, requestID string) InvocationInfo {
	return InvocationInfo{
		Service: os.Getenv("K_SERVICE"), Revision: os.Getenv("K_REVISION"), Configuration: os.Getenv("K_CONFIGURATION"),
		InstanceID: InstanceID(), RequestID: requestID, Method: r.Method, Path: r.URL.Path, RemoteAddr: r.RemoteAddr, StartedAt: time.Now().UTC(),
	}
}

// InvocationKey preserves the legacy unscoped shape for compatibility helpers.
func InvocationKey(info InvocationInfo) string {
	return fmt.Sprintf("instance:%s:%s:%s", orDefault(info.Service, "unknown"), orDefault(info.InstanceID, "unknown"), orDefault(info.RequestID, "unknown"))
}

func InvocationKeyScoped(scope keyspace.Scope, info InvocationInfo) string {
	return scope.Name("logma", "invocation", orDefault(info.Service, "unknown"), orDefault(info.InstanceID, "unknown"), orDefault(info.RequestID, "unknown"))
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func RegisterInvocation(ctx context.Context, client *redis.Client, info InvocationInfo) error {
	return registerInvocation(ctx, client, InvocationKey(info), info)
}

func RegisterInvocationScoped(ctx context.Context, client *redis.Client, scope keyspace.Scope, info InvocationInfo) error {
	return registerInvocation(ctx, client, InvocationKeyScoped(scope, info), info)
}

func registerInvocation(ctx context.Context, client *redis.Client, key string, info InvocationInfo) error {
	fields := map[string]any{
		"service": info.Service, "revision": info.Revision, "configuration": info.Configuration,
		"instance_id": info.InstanceID, "request_id": info.RequestID, "method": info.Method,
		"path": info.Path, "remote_addr": info.RemoteAddr, "started_at": info.StartedAt.Format(time.RFC3339),
	}
	if err := client.HSet(ctx, key, fields).Err(); err != nil {
		return fmt.Errorf("hset %s: %w", key, err)
	}
	if err := client.Expire(ctx, key, invocationTTL).Err(); err != nil {
		return fmt.Errorf("expire %s: %w", key, err)
	}
	return nil
}
