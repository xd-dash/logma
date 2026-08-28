package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/redis/go-redis/v9"
)

type ChannelHandlers map[string]func(payload string)

func (cp ControlPlane) SubscribeAll(ctx context.Context, handlers ChannelHandlers) func() {
	subs := make([]*Subscriber, 0, len(handlers)*2)
	for baseChannel, onMessage := range handlers {
		instance, relay := cp.Subscribe(ctx, baseChannel, onMessage)
		subs = append(subs, instance, relay)
	}
	return func() {
		for _, s := range subs {
			<-s.Stopped()
		}
	}
}

type ServiceSpec struct {
	Invocation InvocationInfo
	Channels   ChannelHandlers
	Work       func(ctx context.Context) error
}

func (cp ControlPlane) Run(ctx context.Context, spec ServiceSpec) error {
	if err := RegisterInvocation(ctx, cp.Client, spec.Invocation); err != nil {
		log.Printf("pubsub: failed to record invocation info: %v", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	teardown := cp.SubscribeAll(runCtx, spec.Channels)
	defer func() {
		cancel()
		teardown()
	}()
	return spec.Work(runCtx)
}

type Runtime struct {
	ControlPlane
	Session
	invocation   InvocationInfo
	spec         ServiceSpec
	redisFromEnv bool
}

func NewRuntime(client *redis.Client) Runtime { return newRuntime(client, false) }
func NewRuntimeFromEnv() Runtime               { return newRuntime(NewClientFromEnv(), true) }

func newRuntime(client *redis.Client, fromEnv bool) Runtime {
	return Runtime{ControlPlane: NewControlPlane(client), Session: NewSession(), redisFromEnv: fromEnv}
}

func (sr *Runtime) RecordInvocation(r *http.Request, requestID string) {
	sr.invocation = InvocationInfoFromRequest(r, requestID)
	if sr.redisFromEnv && (os.Getenv("REDIS_URI") == "" || os.Getenv("REDISCLI_AUTH") == "") {
		sr.Client = NewClientFromRequest(r)
	}
}

func (sr *Runtime) Configure(spec ServiceSpec) { sr.spec = spec }

func (sr *Runtime) ConfigureDefault(work func(ctx context.Context) error, extraChannels ChannelHandlers) {
	channels := make(ChannelHandlers, len(extraChannels)+1)
	channels[sr.ShutdownChannel()] = sr.DefaultShutdownHandler()
	for channel, handler := range extraChannels {
		channels[channel] = handler
	}
	sr.Configure(ServiceSpec{Channels: channels, Work: work})
}

func (sr *Runtime) Publish(channel string, event any) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal %T for %s: %w", event, channel, err)
	}
	if err := sr.Client.Publish(sr.Context(), channel, data).Err(); err != nil {
		return fmt.Errorf("publish to %s: %w", channel, err)
	}
	return nil
}

func (sr *Runtime) DefaultShutdownHandler() func(payload string) {
	label := sr.Namespace
	if label == "" {
		label = "service"
	}
	return func(payload string) {
		request := ParseShutdownRequest(payload)
		log.Printf("%s: shutting down: reason=%q", label, request.Reason)
		sr.Cancel()
	}
}

func (sr *Runtime) Start(ctx context.Context) {
	sr.Begin(ctx, func() {
		spec := sr.spec
		spec.Invocation = sr.invocation
		if err := sr.ControlPlane.Run(sr.Context(), spec); err != nil {
			log.Printf("pubsub: %v", err)
		}
	})
}
