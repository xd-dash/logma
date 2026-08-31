package pubsub

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/xd-dash/logma/serverless/keyspace"
	"github.com/xd-dash/logma/serverless/pubsub/channels"
)

type ControlPlane struct {
	Client             *redis.Client
	InstanceID         string
	Scope              keyspace.Scope
	GlobalRelayEnabled bool
	channels.Defaults
}

func NewControlPlane(client *redis.Client) ControlPlane {
	instanceID := InstanceID()
	return ControlPlane{
		Client:             client,
		InstanceID:         instanceID,
		Scope:              keyspace.FromEnv(instanceID),
		GlobalRelayEnabled: strings.EqualFold(strings.TrimSpace(os.Getenv("LOGMA_GLOBAL_RELAY_ENABLED")), "true"),
		Defaults:           channels.Discover(),
	}
}

// InstanceChannel applies the Fatline security scope as the left-most Redis
// segment so one ACL pattern can constrain both keys and Pub/Sub channels.
func (cp ControlPlane) InstanceChannel(baseChannel string) string {
	return cp.Scope.Prefix(baseChannel)
}

// GlobalChannel is intentionally outside an instance security scope. Code that
// consumes global relay traffic therefore requires an explicit &global:* ACL.
func (cp ControlPlane) GlobalChannel(baseChannel string) string {
	return "global:" + baseChannel
}

func (cp ControlPlane) Relay(ctx context.Context, baseChannel string) *Subscriber {
	if !cp.GlobalRelayEnabled {
		return nil
	}
	instanceChannel := cp.InstanceChannel(baseChannel)
	globalChannel := cp.GlobalChannel(baseChannel)
	return Subscribe(ctx, cp.Client, globalChannel, func(payload string) {
		if err := cp.Client.Publish(ctx, instanceChannel, payload).Err(); err != nil {
			log.Printf("pubsub: failed to relay %s -> %s: %v", globalChannel, instanceChannel, err)
		}
	})
}

func (cp ControlPlane) Subscribe(ctx context.Context, baseChannel string, onMessage func(payload string)) (instance, relay *Subscriber) {
	instance = Subscribe(ctx, cp.Client, cp.InstanceChannel(baseChannel), onMessage)
	relay = cp.Relay(ctx, baseChannel)
	return instance, relay
}
