// Package pubsub is the canonical Logma import surface for the
// independently maintained logma-serverless Redis Pub/Sub runtime.
//
// Application code should prefer github.com/xd-dash/logma/serverless/pubsub.
// The implementation remains in github.com/xd-dash/logma-serverless so that
// repository can still build, test, and deploy on its own.
package pubsub

import (
	"context"
	"net/http"

	"github.com/redis/go-redis/v9"
	implementation "github.com/xd-dash/logma-serverless/pubsub"
)

const (
	HeaderRedisAuth = implementation.HeaderRedisAuth
	HeaderRedisURI  = implementation.HeaderRedisURI
)

type (
	Lifecycle       = implementation.Lifecycle
	ChannelHandlers = implementation.ChannelHandlers
	ServiceSpec     = implementation.ServiceSpec
	Runtime         = implementation.Runtime
	ControlPlane    = implementation.ControlPlane
	Session         = implementation.Session
	Subscriber      = implementation.Subscriber
	InvocationInfo  = implementation.InvocationInfo
	ShutdownRequest = implementation.ShutdownRequest
)

// Holder aliases the standalone implementation so values passed through this
// package retain identical method sets and runtime semantics.
type Holder[T Lifecycle] = implementation.Holder[T]

func NewHolder[T Lifecycle](newFn func() T) *Holder[T] {
	return implementation.NewHolder(newFn)
}

func NewClientFromEnv() *redis.Client {
	return implementation.NewClientFromEnv()
}

func NewClientFromRequest(r *http.Request) *redis.Client {
	return implementation.NewClientFromRequest(r)
}

func NewRuntime(client *redis.Client) Runtime {
	return implementation.NewRuntime(client)
}

func NewRuntimeFromEnv() Runtime {
	return implementation.NewRuntimeFromEnv()
}

func NewControlPlane(client *redis.Client) ControlPlane {
	return implementation.NewControlPlane(client)
}

func NewSession() Session {
	return implementation.NewSession()
}

func Subscribe(ctx context.Context, client *redis.Client, channel string, onMessage func(payload string)) *Subscriber {
	return implementation.Subscribe(ctx, client, channel, onMessage)
}

func InvocationInfoFromRequest(r *http.Request, requestID string) InvocationInfo {
	return implementation.InvocationInfoFromRequest(r, requestID)
}

func InvocationKey(info InvocationInfo) string {
	return implementation.InvocationKey(info)
}

func RegisterInvocation(ctx context.Context, client *redis.Client, info InvocationInfo) error {
	return implementation.RegisterInvocation(ctx, client, info)
}

func ParseShutdownRequest(payload string) ShutdownRequest {
	return implementation.ParseShutdownRequest(payload)
}

// InstanceID is process-stable and intentionally shares the standalone
// implementation's cached identity rather than creating a second namespace.
var InstanceID = implementation.InstanceID
