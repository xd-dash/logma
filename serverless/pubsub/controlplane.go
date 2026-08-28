package pubsub

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"

	"github.com/xd-dash/logma/serverless/pubsub/channels"
)

type ControlPlane struct {
	Client     *redis.Client
	InstanceID string
	channels.Defaults
}

func NewControlPlane(client *redis.Client) ControlPlane {
	return ControlPlane{Client: client, InstanceID: InstanceID(), Defaults: channels.Discover()}
}

func (cp ControlPlane) InstanceChannel(baseChannel string) string {
	return baseChannel + ":" + cp.InstanceID
}

func (cp ControlPlane) GlobalChannel(baseChannel string) string {
	return baseChannel + ":global"
}

func (cp ControlPlane) Relay(ctx context.Context, baseChannel string) *Subscriber {
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
