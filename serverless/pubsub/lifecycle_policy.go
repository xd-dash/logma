package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	ratelimiter "github.com/dash-xd/ratelimiter"
	minimalprofile "github.com/dash-xd/ratelimiter/profile/minimal"
	preflightprofile "github.com/dash-xd/ratelimiter/profile/preflight"
	"github.com/redis/go-redis/v9"
)

const (
	bootstrapLifecycleKeyspace = "logma:lifecycle"
	lifecycleTotalWindow       = 24 * time.Hour
)

type Policy string

const (
	PolicyNone           Policy = ""
	Policy3S             Policy = "3s"
	Policy3Publishes     Policy = "3-publishes"
	Policy30S            Policy = "30s"
	Policy30S64Publishes Policy = "30s-64-publishes"
	Policy5M             Policy = "5m"
	Policy20M            Policy = "20m"
)

type lifecyclePolicyConfig struct {
	timer        time.Duration
	tickEvery    time.Duration
	maxPublishes int64
	code         ratelimiter.PolicyCode
}

func (p Policy) policySpec() (ratelimiter.PolicySpec, error) {
	switch p {
	case PolicyNone:
		return ratelimiter.PolicySpec{}, nil
	case Policy3S:
		return ratelimiter.PolicySpec{Duration: ratelimiter.Duration3S}, nil
	case Policy3Publishes:
		publishes, err := ratelimiter.NewScaleClass(3)
		return ratelimiter.PolicySpec{Publishes: publishes}, err
	case Policy30S:
		return ratelimiter.PolicySpec{Duration: ratelimiter.Duration30S}, nil
	case Policy30S64Publishes:
		publishes, err := ratelimiter.NewScaleClass(64)
		return ratelimiter.PolicySpec{Publishes: publishes, Duration: ratelimiter.Duration30S}, err
	case Policy5M:
		return ratelimiter.PolicySpec{Duration: ratelimiter.Duration5M}, nil
	case Policy20M:
		return ratelimiter.PolicySpec{Duration: ratelimiter.Duration20M}, nil
	default:
		return ratelimiter.PolicySpec{}, fmt.Errorf("unknown lifecycle policy %q", p)
	}
}

func (p Policy) config() (lifecyclePolicyConfig, error) {
	if p == PolicyNone {
		return lifecyclePolicyConfig{}, nil
	}
	spec, err := p.policySpec()
	if err != nil {
		return lifecyclePolicyConfig{}, err
	}
	resolver := ratelimiter.TargetResolverFunc(func(ratelimiter.Input, ratelimiter.Stage) []ratelimiter.Target { return nil })
	compiled, err := ratelimiter.CompilePolicy(preflightprofile.New(resolver), spec, ratelimiter.EntitlementFor(spec))
	if err != nil {
		return lifecyclePolicyConfig{}, fmt.Errorf("compile lifecycle policy %q: %w", p, err)
	}
	return lifecyclePolicyConfig{
		timer:        compiled.Duration,
		tickEvery:    lifecycleTickEvery(compiled.Duration),
		maxPublishes: int64(compiled.Publishes),
		code:         compiled.Code,
	}, nil
}

func lifecycleTickEvery(duration time.Duration) time.Duration {
	switch {
	case duration == 0:
		return 0
	case duration <= 3*time.Second:
		return 250 * time.Millisecond
	case duration <= 30*time.Second:
		return 500 * time.Millisecond
	default:
		return time.Second
	}
}

var ErrLifecyclePublishLimit = errors.New("lifecycle publish limit reached")

// Bootstrap uses elevated FUNCTION LOAD authority. Runtime keyspace ownership is
// selected later from the ControlPlane scope and does not affect library loading.
func BootstrapLifecycleFunctions(ctx context.Context, client *redis.Client) error {
	store, err := ratelimiter.NewRedisStore(client, ratelimiter.RedisConfig{Keyspace: bootstrapLifecycleKeyspace})
	if err != nil {
		return err
	}
	noTargets := ratelimiter.TargetResolverFunc(func(ratelimiter.Input, ratelimiter.Stage) []ratelimiter.Target { return nil })
	return store.Bootstrap(ctx, minimalprofile.New(), preflightprofile.New(noTargets))
}

type lifecycleGuard struct {
	client           *redis.Client
	cancel           context.CancelFunc
	timerLimiter     *ratelimiter.Limiter
	totalLimiter     *ratelimiter.Limiter
	timerBucket      string
	totalBucket      string
	shutdownChannel  string
	cfg              lifecyclePolicyConfig
	shutdownOnce     sync.Once
	observer         Observer
	baseEvent        ObservabilityEvent
}

func newLifecycleGuard(ctx context.Context, client *redis.Client, cancel context.CancelFunc, cp ControlPlane, invocation InvocationInfo, policy Policy, observer Observer) (*lifecycleGuard, error) {
	cfg, err := policy.config()
	if err != nil {
		return nil, err
	}
	if policy == PolicyNone {
		return nil, nil
	}
	namespace := cp.Namespace
	if namespace == "" {
		namespace = invocation.Service
	}
	if namespace == "" {
		namespace = "service"
	}
	// Bucket is deliberately relative. RedisStore prefixes every key with the
	// worker-owned security scope so every FCALL key matches ~<scope>:*.
	baseBucket := fmt.Sprintf("logma:runtime:%s:%s:%s", cleanPart(namespace), cleanPart(cp.InstanceID), cleanPart(invocation.RequestID))
	guard := &lifecycleGuard{
		client:          client,
		cancel:          cancel,
		timerBucket:     baseBucket + ":timer",
		totalBucket:     baseBucket + ":publishes",
		shutdownChannel: cp.InstanceChannel(cp.ShutdownChannel()),
		cfg:             cfg,
		observer:        observer,
		baseEvent: ObservabilityEvent{
			Kind:       "fatline",
			Namespace:  namespace,
			InstanceID: cp.InstanceID,
			RequestID:  invocation.RequestID,
			Policy:     string(policy),
			PolicyCode: uint64(cfg.code),
		},
	}
	store, err := ratelimiter.NewRedisStore(client, ratelimiter.RedisConfig{Keyspace: cp.Scope.Name("ratelimiter", "lifecycle")})
	if err != nil {
		return nil, err
	}
	if cfg.maxPublishes > 0 {
		guard.totalLimiter, err = store.Limiter(minimalprofile.New())
		if err != nil {
			return nil, err
		}
	}
	if cfg.timer > 0 {
		resolver := ratelimiter.StaticTargets(map[ratelimiter.Stage][]ratelimiter.Target{
			ratelimiter.StageShutdown: {{
				Channel: guard.shutdownChannel,
				Purpose: ratelimiter.PurposeLifecycleControl,
				Data: ratelimiter.Metadata{
					"lifecycle_policy": string(policy),
					"policy_code":      strconv.FormatUint(uint64(cfg.code), 10),
					"runtime":          cp.Scope.Prefix(baseBucket),
				},
			}},
		})
		guard.timerLimiter, err = store.Limiter(preflightprofile.New(resolver))
		if err != nil {
			return nil, err
		}
		window := cfg.timer + time.Minute
		_, err = guard.timerLimiter.Check(ctx, ratelimiter.Input{
			Bucket: guard.timerBucket,
			Request: ratelimiter.Request{
				ID:        invocation.RequestID,
				Subject:   cp.InstanceID,
				Operation: "serverless.runtime",
				Resource:  namespace,
			},
			Preflight: ratelimiter.PreflightOptions{
				Shutdown: ratelimiter.ShutdownConditions{
					Timer: &ratelimiter.TimerCondition{After: cfg.timer},
				},
			},
		}, ratelimiter.Limit{MaxRequests: 1, Window: window})
		if err != nil {
			return nil, fmt.Errorf("arm lifecycle timer: %w", err)
		}
		guard.emit(ctx, "lifecycle", "armed", "")
		go guard.tickTimer(ctx)
	} else {
		guard.emit(ctx, "lifecycle", "active", "")
	}
	return guard, nil
}

func (g *lifecycleGuard) emit(ctx context.Context, phase, status, reason string) {
	if g == nil {
		return
	}
	event := g.baseEvent
	event.Phase = phase
	event.Status = status
	event.Reason = reason
	observe(g.observer, ctx, event)
}

func (g *lifecycleGuard) admitPublish(ctx context.Context) (bool, error) {
	if g == nil || g.totalLimiter == nil {
		return false, nil
	}
	decision, err := g.totalLimiter.Check(ctx, ratelimiter.Input{Bucket: g.totalBucket}, ratelimiter.Limit{MaxRequests: g.cfg.maxPublishes, Window: lifecycleTotalWindow})
	if err != nil {
		g.emit(ctx, "lifecycle_publish_limit", "error", "policy_check_failed")
		return false, fmt.Errorf("check lifecycle publish limit: %w", err)
	}
	if !decision.Allowed {
		g.emit(ctx, "lifecycle_publish_limit", "exhausted", "total_publishes")
		_ = g.shutdown("lifecycle:total_publishes")
		return false, ErrLifecyclePublishLimit
	}
	if decision.Remaining == 0 {
		g.emit(ctx, "lifecycle_publish_limit", "exhausted", "total_publishes")
	}
	return decision.Remaining == 0, nil
}

func (g *lifecycleGuard) afterPublish(exhausted bool) {
	if !exhausted {
		return
	}
	if err := g.shutdown("lifecycle:total_publishes"); err != nil {
		log.Printf("pubsub: publish lifecycle shutdown: %v", err)
	}
}

func (g *lifecycleGuard) shutdown(reason string) error {
	if g == nil {
		return nil
	}
	var shutdownErr error
	g.shutdownOnce.Do(func() {
		g.emit(context.Background(), "lifecycle_shutdown", "signaling", reason)
		payload, err := json.Marshal(ShutdownRequest{Reason: reason})
		if err != nil {
			shutdownErr = err
			g.cancel()
			return
		}
		publishCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, shutdownErr = g.client.Publish(publishCtx, g.shutdownChannel, payload).Result()
		if shutdownErr != nil {
			g.emit(publishCtx, "lifecycle_shutdown", "publish_failed", reason)
		} else {
			g.emit(publishCtx, "lifecycle_shutdown", "signaled", reason)
		}
		g.cancel()
	})
	return shutdownErr
}

func (g *lifecycleGuard) tickTimer(ctx context.Context) {
	if g == nil || g.timerLimiter == nil || g.cfg.tickEvery <= 0 {
		return
	}
	ticker := time.NewTicker(g.cfg.tickEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			result, err := g.timerLimiter.Tick(ctx, g.timerBucket)
			if err != nil {
				if ctx.Err() == nil {
					g.emit(ctx, "lifecycle_timer", "tick_failed", "timer_tick_failed")
					log.Printf("pubsub: lifecycle timer tick: %v", err)
				}
				continue
			}
			if result.PublishFailures > 0 {
				g.emit(ctx, "lifecycle_timer", "publish_failed", "shutdown_publish_failed")
				log.Printf("pubsub: lifecycle timer tick publish failures=%d pending=%d", result.PublishFailures, result.Pending)
			}
		}
	}
}

func (g *lifecycleGuard) close() {
	if g == nil || g.timerLimiter == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := g.timerLimiter.CancelTimer(ctx, g.timerBucket); err != nil {
		g.emit(ctx, "lifecycle", "cancel_failed", "timer_cancel_failed")
		log.Printf("pubsub: cancel lifecycle timer: %v", err)
		return
	}
	g.emit(ctx, "lifecycle", "closed", "")
}
