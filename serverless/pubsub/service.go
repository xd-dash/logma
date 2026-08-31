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
		if instance != nil { subs = append(subs, instance) }
		if relay != nil { subs = append(subs, relay) }
	}
	return func() {
		for _, s := range subs {
			<-s.Stopped()
		}
	}
}

type ServiceSpec struct {
	Invocation    InvocationInfo
	Channels      ChannelHandlers
	Subscriptions []SubscriptionDescriptor
	Lifecycle     Policy
	Observer      Observer
	Work          func(ctx context.Context) error
}

func (cp ControlPlane) Run(ctx context.Context, spec ServiceSpec) error {
	if err := RegisterInvocationScoped(ctx, cp.Client, cp.Scope, spec.Invocation); err != nil {
		log.Printf("pubsub: failed to record invocation info: %v", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	teardown := cp.SubscribeAll(runCtx, spec.Channels)
	descriptors := normalizeDescriptors(spec.Channels, spec.Subscriptions)
	lease, err := cp.RegisterRuntime(runCtx, spec.Invocation, descriptors, spec.Lifecycle)
	if err != nil {
		log.Printf("pubsub: failed to register runtime state: %v", err)
	}
	defer func() {
		cancel()
		teardown()
		if lease != nil {
			if err := lease.Close(); err != nil {
				log.Printf("pubsub: failed to remove runtime state: %v", err)
			}
		}
	}()
	return spec.Work(runCtx)
}

type Runtime struct {
	ControlPlane
	Session
	invocation   InvocationInfo
	spec         ServiceSpec
	redisFromEnv bool
	lifecycle    *lifecycleGuard
}

func NewRuntime(client *redis.Client) Runtime { return newRuntime(client, false) }
func NewRuntimeFromEnv() Runtime              { return newRuntime(NewClientFromEnv(), true) }

func newRuntime(client *redis.Client, fromEnv bool) Runtime {
	return Runtime{ControlPlane: NewControlPlane(client), Session: NewSession(), redisFromEnv: fromEnv}
}

func (sr *Runtime) RecordInvocation(r *http.Request, requestID string) {
	sr.invocation = InvocationInfoFromRequest(r, requestID)
	if sr.redisFromEnv && os.Getenv("REDIS_URI") == "" && os.Getenv("REDIS_SOCKET") == "" {
		sr.Client = NewClientFromRequest(r)
	}
}

func (sr *Runtime) Configure(spec ServiceSpec) { sr.spec = spec }

func (sr *Runtime) SetObserver(observer Observer) { sr.spec.Observer = observer }

func (sr *Runtime) ConfigureDefault(work func(ctx context.Context) error, extraChannels ChannelHandlers) {
	sr.ConfigureDefaultWithLifecycle(PolicyNone, work, extraChannels)
}

func (sr *Runtime) ConfigureDefaultWithLifecycle(policy Policy, work func(ctx context.Context) error, extraChannels ChannelHandlers) {
	observer := sr.spec.Observer
	channels := make(ChannelHandlers, len(extraChannels)+1)
	shutdownChannel := sr.ShutdownChannel()
	channels[shutdownChannel] = sr.DefaultShutdownHandler()
	descriptors := []SubscriptionDescriptor{{ID: "shutdown", Channel: shutdownChannel, Callback: "internal:shutdown"}}
	for channel, handler := range extraChannels {
		channels[channel] = handler
		descriptors = append(descriptors, SubscriptionDescriptor{ID: cleanPart(channel), Channel: channel, Callback: "internal:handler"})
	}
	sr.Configure(ServiceSpec{Channels: channels, Subscriptions: descriptors, Lifecycle: policy, Observer: observer, Work: work})
}

func (sr *Runtime) observabilityBase(phase, status string) ObservabilityEvent {
	namespace := sr.Namespace
	if namespace == "" { namespace = sr.invocation.Service }
	return ObservabilityEvent{Kind: "fatline", Phase: phase, Status: status, Namespace: namespace, InstanceID: sr.InstanceID, RequestID: sr.invocation.RequestID, Policy: string(sr.spec.Lifecycle)}
}

func (sr *Runtime) Publish(channel string, event any) error {
	exhausted, err := sr.lifecycle.admitPublish(sr.Context())
	if err != nil {
		observed := sr.observabilityBase("publish_admission", "denied")
		observed.Channel = channel
		observed.Reason = "lifecycle_policy_denied"
		observe(sr.spec.Observer, sr.Context(), observed)
		return err
	}
	data, err := json.Marshal(event)
	if err != nil { return fmt.Errorf("marshal %T for %s: %w", event, channel, err) }
	publishErr := sr.Client.Publish(sr.Context(), channel, data).Err()
	sr.lifecycle.afterPublish(exhausted)
	observed := sr.observabilityBase("publish", "published")
	observed.Channel = channel
	observed.Payload = append(json.RawMessage(nil), data...)
	if publishErr != nil { observed.Status = "failed"; observed.Reason = "redis_publish_failed" }
	observe(sr.spec.Observer, sr.Context(), observed)
	if publishErr != nil { return fmt.Errorf("publish to %s: %w", channel, publishErr) }
	return nil
}

func (sr *Runtime) DefaultShutdownHandler() func(payload string) {
	label := sr.Namespace
	if label == "" { label = "service" }
	return func(payload string) {
		request := ParseShutdownRequest(payload)
		observed := sr.observabilityBase("shutdown_signal", "received")
		observed.Reason = request.Reason
		observe(sr.spec.Observer, sr.Context(), observed)
		log.Printf("%s: shutting down: reason=%q", label, request.Reason)
		sr.Cancel()
	}
}

func (sr *Runtime) Start(ctx context.Context) {
	sr.Begin(ctx, func() {
		spec := sr.spec
		spec.Invocation = sr.invocation
		work := spec.Work
		if work == nil { log.Printf("pubsub: service work is nil"); return }
		observe(spec.Observer, sr.Context(), sr.observabilityBase("runtime", "starting"))
		spec.Work = func(runCtx context.Context) error {
			guard, err := newLifecycleGuard(runCtx, sr.Client, sr.Cancel, sr.ControlPlane, spec.Invocation, spec.Lifecycle, spec.Observer)
			if err != nil { return err }
			sr.lifecycle = guard
			defer func() { guard.close(); sr.lifecycle = nil }()
			return work(runCtx)
		}
		if err := sr.ControlPlane.Run(sr.Context(), spec); err != nil {
			failed := sr.observabilityBase("runtime", "failed")
			failed.Reason = "runtime_failed"
			observe(spec.Observer, sr.Context(), failed)
			log.Printf("pubsub: %v", err)
			return
		}
		observe(spec.Observer, sr.Context(), sr.observabilityBase("runtime", "stopped"))
	})
}
