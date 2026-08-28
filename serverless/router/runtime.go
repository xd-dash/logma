// Package router implements Logma's dynamic Redis Pub/Sub SSE runtime.
package router

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"

	"github.com/xd-dash/logma/serverless/pubsub"
)

const (
	inputBufferSize = 64
	eventBufferSize = 64
)

type runtimeMessage struct {
	channel string
	payload string
}

type subscriptionStopped struct {
	channel string
}

type subscription struct {
	channel string
	cancel  context.CancelFunc
}

type PublishRequest struct {
	Channel string          `json:"channel,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type Runtime struct {
	pubsub.ControlPlane
	pubsub.Session

	input  chan runtimeMessage
	events chan PublishRequest
	status chan subscriptionStopped

	invocation      pubsub.InvocationInfo
	channels        []string
	defaultChannels []string
}

func NewRuntime() *Runtime {
	return &Runtime{
		ControlPlane:    pubsub.NewControlPlane(pubsub.NewClientFromEnv()),
		Session:         pubsub.NewSession(),
		input:           make(chan runtimeMessage, inputBufferSize),
		events:          make(chan PublishRequest, eventBufferSize),
		status:          make(chan subscriptionStopped, inputBufferSize),
		defaultChannels: defaultSubscriptionsFromEnv(),
	}
}

func defaultSubscriptionsFromEnv() []string {
	raw := os.Getenv("REDIS_DEFAULT_SUBSCRIPTIONS")
	if raw == "" {
		return nil
	}
	var channels []string
	if err := json.Unmarshal([]byte(raw), &channels); err != nil {
		log.Printf("invalid REDIS_DEFAULT_SUBSCRIPTIONS (must be a JSON array of channel name strings): %v", err)
		return nil
	}
	return channels
}

func (rt *Runtime) RecordInvocation(r *http.Request, requestID string) {
	rt.invocation = pubsub.InvocationInfoFromRequest(r, requestID)
	if os.Getenv("REDIS_URI") == "" || os.Getenv("REDISCLI_AUTH") == "" {
		rt.Client = pubsub.NewClientFromRequest(r)
	}
}

func (rt *Runtime) Subscribe(channels []string)   { rt.channels = channels }
func (rt *Runtime) Events() <-chan PublishRequest { return rt.events }
func (rt *Runtime) Start(ctx context.Context)     { rt.Begin(ctx, rt.run) }

func (rt *Runtime) run() {
	subscriptions := make(map[string]*subscription)
	startSubscription := func(channel string) error { return rt.startSubscription(channel, subscriptions) }
	stopSubscription := func(channel string) {
		sub, ok := subscriptions[channel]
		if !ok {
			return
		}
		delete(subscriptions, channel)
		sub.cancel()
	}
	defer func() {
		for channel, sub := range subscriptions {
			delete(subscriptions, channel)
			sub.cancel()
		}
	}()

	if err := pubsub.RegisterInvocation(rt.Context(), rt.Client, rt.invocation); err != nil {
		log.Printf("failed to record invocation info: %v", err)
	}

	relayCtx, cancelRelays := context.WithCancel(rt.Context())
	var relays []*pubsub.Subscriber
	defer func() {
		cancelRelays()
		for _, relay := range relays {
			<-relay.Stopped()
		}
	}()

	handlers := make(map[string]Handle, len(Subscriptions))
	for base, handle := range Subscriptions {
		instanceChannel := rt.InstanceChannel(base)
		handlers[instanceChannel] = handle
		relays = append(relays, rt.Relay(relayCtx, base))
		if err := startSubscription(instanceChannel); err != nil {
			log.Printf("failed to initialize %q: %v", instanceChannel, err)
			return
		}
	}

	for _, channel := range rt.defaultChannels {
		if err := startSubscription(channel); err != nil {
			log.Printf("failed to subscribe to default channel %q: %v", channel, err)
		}
	}
	for _, channel := range rt.channels {
		if err := startSubscription(channel); err != nil {
			log.Printf("failed to subscribe to requested channel %q: %v", channel, err)
		}
	}

	log.Printf("Redis runtime started (instance=%s)", rt.InstanceID)
	for {
		select {
		case <-rt.Context().Done():
			return
		case message := <-rt.input:
			if handle, ok := handlers[message.channel]; ok {
				handle(&rt.Session, message.payload, startSubscription)
			} else {
				rt.handlePublish(message)
			}
		case stopped := <-rt.status:
			stopSubscription(stopped.channel)
			if rt.Context().Err() != nil {
				return
			}
			if _, registered := handlers[stopped.channel]; !registered {
				if err := startSubscription(stopped.channel); err != nil {
					log.Printf("failed to restart subscription %q: %v", stopped.channel, err)
				}
			}
		}
	}
}

func (rt *Runtime) handlePublish(message runtimeMessage) {
	if message.payload == "" || message.payload == "{}" {
		return
	}
	var publish PublishRequest
	if err := json.Unmarshal([]byte(message.payload), &publish); err != nil {
		log.Printf("invalid Redis message on %q: %v", message.channel, err)
		return
	}
	if publish.Channel == "" {
		publish.Channel = message.channel
	}
	select {
	case rt.events <- publish:
	default:
		log.Printf("events channel full; dropping message on %q", publish.Channel)
	}
}

func (rt *Runtime) startSubscription(channel string, subscriptions map[string]*subscription) error {
	if channel == "" {
		return errors.New("subscription channel is empty")
	}
	if _, exists := subscriptions[channel]; exists {
		return nil
	}

	ctx, cancel := context.WithCancel(rt.Context())
	subscriptions[channel] = &subscription{channel: channel, cancel: cancel}
	sub := pubsub.Subscribe(ctx, rt.Client, channel, func(payload string) {
		select {
		case rt.input <- runtimeMessage{channel: channel, payload: payload}:
		case <-ctx.Done():
		}
	})
	go func() {
		<-sub.Stopped()
		select {
		case rt.status <- subscriptionStopped{channel: channel}:
		case <-rt.Context().Done():
		}
	}()
	return nil
}
