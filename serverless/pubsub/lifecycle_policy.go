package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	ratelimiter "github.com/dash-xd/ratelimiter"
	minimalprofile "github.com/dash-xd/ratelimiter/profile/minimal"
	preflightprofile "github.com/dash-xd/ratelimiter/profile/preflight"
	"github.com/redis/go-redis/v9"
)

const (
	lifecycleKeyspace    = "logma:lifecycle"
	lifecycleTotalWindow = 24 * time.Hour
)

// Policy selects a package-owned runtime limit profile. Names describe only the
// concrete mechanism/limit; deployment tiers, licensing, and environment labels
// belong outside this package.
type Policy string

const (
	PolicyNone           Policy = ""
	Policy3S             Policy = "3s"
	Policy3Publishes     Policy = "3-publishes"
	Policy30S64Publishes Policy = "30s-64-publishes"
	Policy20M            Policy = "20m"
)

type lifecyclePolicyConfig struct {
	timer        time.Duration
	tickEvery    time.Duration
	maxPublishes int64
}

func (p Policy) config() (lifecyclePolicyConfig, error) {
	switch p {
	case PolicyNone:
		return lifecyclePolicyConfig{}, nil
	case Policy3S:
		return lifecyclePolicyConfig{
			timer:     3 * time.Second,
			tickEvery: 250 * time.Millisecond,
		}, nil
	case Policy3Publishes:
		return lifecyclePolicyConfig{
			maxPublishes: 3,
		}, nil
	case Policy30S64Publishes:
		return lifecyclePolicyConfig{
			timer:        30 * time.Second,
			tickEvery:    500 * time.Millisecond,
			maxPublishes: 64,
		}, nil
	case Policy20M:
		return lifecyclePolicyConfig{
			timer:     20 * time.Minute,
			tickEvery: time.Second,
		}, nil
	default:
		return lifecyclePolicyConfig{}, fmt.Errorf("unknown lifecycle policy %q", p)
	}
}

var ErrLifecyclePublishLimit = errors.New("lifecycle publish limit reached")

// BootstrapLifecycleFunctions loads the Redis Functions required by the
// lifecycle policies. Call this from deployment/bootstrap code with credentials
// that may execute FUNCTION LOAD; runtime credentials only need FCALL/PUBLISH.
func BootstrapLifecycleFunctions(ctx context.Context, client *redis.Client) error {
	store, err := ratelimiter.NewRedisStore(client, ratelimiter.RedisConfig{
		Keyspace: lifecycleKeyspace,
	})
	if err != nil {
		return err
	}

	noTargets := ratelimiter.TargetResolverFunc(func(ratelimiter.Input, ratelimiter.Stage) []ratelimiter.Target {
		return nil
	})

	return store.Bootstrap(
		ctx,
		minimalprofile.New(),
		preflightprofile.New(noTargets),
	)
}

type lifecycleGuard struct {
	client          *redis.Client
	cancel          context.CancelFunc
	timerLimiter    *ratelimiter.Limiter
	totalLimiter    *ratelimiter.Limiter
	timerBucket     string
	totalBucket     string
	shutdownChannel string
	cfg             lifecyclePolicyConfig
	shutdownOnce    sync.Once
}

func newLifecycleGuard(
	ctx context.Context,
	client *redis.Client,
	cancel context.CancelFunc,
	cp ControlPlane,
	invocation InvocationInfo,
	policy Policy,
) (*lifecycleGuard, error) {
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

	baseBucket := RuntimeRecordKey(namespace, cp.InstanceID, invocation.RequestID)
	guard := &lifecycleGuard{
		client:          client,
		cancel:          cancel,
		timerBucket:     baseBucket + ":timer",
		totalBucket:     baseBucket + ":publishes",
		shutdownChannel: cp.InstanceChannel(cp.ShutdownChannel()),
		cfg:             cfg,
	}

	store, err := ratelimiter.NewRedisStore(client, ratelimiter.RedisConfig{
		Keyspace: lifecycleKeyspace,
	})
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
					"runtime":          baseBucket,
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
		}, ratelimiter.Limit{
			MaxRequests: 1,
			Window:      window,
		})
		if err != nil {
			return nil, fmt.Errorf("arm lifecycle timer: %w", err)
		}

		go guard.tickTimer(ctx)
	}

	return guard, nil
}

func (g *lifecycleGuard) admitPublish(ctx context.Context) (bool, error) {
	if g == nil || g.totalLimiter == nil {
		return false, nil
	}

	decision, err := g.totalLimiter.Check(ctx, ratelimiter.Input{
		Bucket: g.totalBucket,
	}, ratelimiter.Limit{
		MaxRequests: g.cfg.maxPublishes,
		Window:      lifecycleTotalWindow,
	})
	if err != nil {
		return false, fmt.Errorf("check lifecycle publish limit: %w", err)
	}
	if !decision.Allowed {
		_ = g.shutdown("lifecycle:total_publishes")
		return false, ErrLifecyclePublishLimit
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
		payload, err := json.Marshal(ShutdownRequest{Reason: reason})
		if err != nil {
			shutdownErr = err
			g.cancel()
			return
		}
		publishCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, shutdownErr = g.client.Publish(publishCtx, g.shutdownChannel, payload).Result()
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
					log.Printf("pubsub: lifecycle timer tick: %v", err)
				}
				continue
			}
			if result.PublishFailures > 0 {
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
		log.Printf("pubsub: cancel lifecycle timer: %v", err)
	}
}
