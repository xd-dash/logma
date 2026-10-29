package lifecycle

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	ratelimiter "github.com/dash-xd/ratelimiter"
	lifecycleprofile "github.com/dash-xd/ratelimiter/profile/lifecycle"
	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/serverless/keyspace"
)

const defaultKeyspace = "logma:lifecycle"
const lifecycleSubsystem = "logma:lifecycle"
const readinessRedisIOTimeout = 750 * time.Millisecond

type Runtime struct {
	state            FileStore
	client           *redis.Client
	limiter          *ratelimiter.Limiter
	readinessClient  *redis.Client
	readinessLimiter *ratelimiter.Limiter
	scope            string
}

func NewRuntime(ctx context.Context, stateDir string) (*Runtime, error) {
	scope := strings.TrimSpace(os.Getenv("FATLINE_SCOPE"))
	redisKeyspace, scope, err := lifecycleKeyspace(scope)
	if err != nil {
		return nil, err
	}
	client, err := ratelimiter.NewClientFromEnv()
	if err != nil {
		return nil, err
	}

	store, err := ratelimiter.NewRedisStore(client, ratelimiter.RedisConfig{Keyspace: redisKeyspace})
	if err != nil {
		_ = client.Close()
		return nil, err
	}

	resolver := ratelimiter.TargetResolverFunc(func(in ratelimiter.Input, stage ratelimiter.Stage) []ratelimiter.Target {
		if stage != ratelimiter.StageShutdown {
			return nil
		}
		channel := in.CallbackData["shutdown_channel"]
		if channel == "" {
			return nil
		}
		data := ratelimiter.Metadata{}
		for key, value := range in.CallbackData {
			if key == "shutdown_channel" {
				continue
			}
			data[key] = value
		}
		return []ratelimiter.Target{{
			Channel: channel,
			Purpose: ratelimiter.PurposeLifecycleControl,
			Data:    data,
		}}
	})
	profile := lifecycleprofile.New(resolver)
	readinessClient := newReadinessClient(client)
	readinessStore, err := ratelimiter.NewRedisStore(readinessClient, ratelimiter.RedisConfig{Keyspace: redisKeyspace})
	if err != nil {
		_ = readinessClient.Close()
		_ = client.Close()
		return nil, err
	}
	readinessLimiter, err := readinessStore.Limiter(profile)
	if err != nil {
		_ = readinessClient.Close()
		_ = client.Close()
		return nil, err
	}
	internalBootstrap, err := lifecycleBootstrapInternal()
	if err != nil {
		_ = readinessClient.Close()
		_ = client.Close()
		return nil, err
	}
	if internalBootstrap {
		if err := store.Bootstrap(ctx, profile); err != nil {
			_ = readinessClient.Close()
			_ = client.Close()
			return nil, fmt.Errorf("bootstrap lifecycle profile: %w", err)
		}
	}
	limiter, err := store.Limiter(profile)
	if err != nil {
		_ = readinessClient.Close()
		_ = client.Close()
		return nil, err
	}

	runtime := &Runtime{
		state:            FileStore{Dir: stateDir},
		client:           client,
		limiter:          limiter,
		readinessClient:  readinessClient,
		readinessLimiter: readinessLimiter,
		scope:            scope,
	}
	if _, err := runtime.RestoreAll(ctx); err != nil {
		_ = runtime.Close()
		return nil, fmt.Errorf("restore lifecycle registrations: %w", err)
	}
	return runtime, nil
}

func newReadinessClient(client *redis.Client) *redis.Client {
	options := *client.Options()
	options.ContextTimeoutEnabled = true
	options.DialTimeout = readinessRedisIOTimeout
	options.ReadTimeout = readinessRedisIOTimeout
	options.WriteTimeout = readinessRedisIOTimeout
	return redis.NewClient(&options)
}

func lifecycleKeyspace(scope string) (string, string, error) {
	if scope == "" {
		return defaultKeyspace, "", nil
	}
	parsed, err := keyspace.ParseScope(scope)
	if err != nil {
		return "", "", fmt.Errorf("invalid FATLINE_SCOPE: %w", err)
	}
	scope = string(parsed)
	return scope + ":" + lifecycleSubsystem, scope, nil
}

func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	var closeErr error
	if r.readinessClient != nil {
		closeErr = r.readinessClient.Close()
	}
	if r.client != nil {
		if err := r.client.Close(); closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

// Ready checks Redis transport and the lifecycle Function permission path.
// It uses an independently bounded Redis client so the HTTP health response is
// returned inside its request deadline even if the application pool is stuck.
func (r *Runtime) Ready(ctx context.Context) error {
	if r == nil || r.readinessClient == nil || r.readinessLimiter == nil {
		return errors.New("lifecycle runtime is not initialized")
	}
	if err := r.readinessClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	var probe [16]byte
	if _, err := rand.Read(probe[:]); err != nil {
		return fmt.Errorf("create readiness probe: %w", err)
	}
	if _, err := r.readinessLimiter.CancelTimer(ctx, "__fatline_readiness_probe__"+hex.EncodeToString(probe[:])); err != nil {
		return fmt.Errorf("lifecycle Function probe: %w", err)
	}
	return nil
}

func (r *Runtime) Register(ctx context.Context, req RegisterRequest) (Registration, bool, error) {
	if err := r.validateShutdownChannel(req.ShutdownChannel); err != nil {
		return Registration{}, false, err
	}
	if existing, err := r.state.Load(req.DeploymentID); err == nil {
		matches, matchErr := existing.MatchesRequest(req)
		if matchErr != nil {
			return Registration{}, false, matchErr
		}
		if !matches {
			return Registration{}, false, fmt.Errorf("deployment %q already has different lifecycle intent", req.DeploymentID)
		}
		dispatched, err := r.state.IsDispatched(existing.DeploymentID)
		if err != nil {
			return existing, false, fmt.Errorf("read lifecycle dispatch fence: %w", err)
		}
		if dispatched {
			return existing, false, nil
		}
		armed, err := r.arm(ctx, existing, false)
		if err != nil {
			return existing, false, fmt.Errorf("re-arm existing lifecycle registration: %w", err)
		}
		return existing, armed, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return Registration{}, false, fmt.Errorf("load existing lifecycle registration: %w", err)
	}

	reg, err := NewRegistration(req, time.Now().UTC())
	if err != nil {
		return Registration{}, false, err
	}
	if err := r.state.Create(reg); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return Registration{}, false, fmt.Errorf("persist lifecycle registration: %w", err)
		}
		existing, loadErr := r.state.Load(req.DeploymentID)
		if loadErr != nil {
			return Registration{}, false, fmt.Errorf("load concurrently-created lifecycle registration: %w", loadErr)
		}
		matches, matchErr := existing.MatchesRequest(req)
		if matchErr != nil {
			return Registration{}, false, matchErr
		}
		if !matches {
			return Registration{}, false, fmt.Errorf("deployment %q concurrently registered with different lifecycle intent", req.DeploymentID)
		}
		reg = existing
	}

	// Durable intent deliberately remains on disk if live arming fails. A retry or
	// process restart reconstructs this exact persisted absolute deadline.
	armed, err := r.arm(ctx, reg, true)
	if err != nil {
		return reg, false, fmt.Errorf("arm lifecycle registration: %w", err)
	}
	return reg, armed, nil
}

func (r *Runtime) validateShutdownChannel(channel string) error {
	if r.scope != "" && !strings.HasPrefix(channel, r.scope+":") {
		return fmt.Errorf("shutdown_channel must be within FATLINE_SCOPE %q", r.scope)
	}
	return nil
}

func (r *Runtime) RestoreAll(ctx context.Context) (int, error) {
	regs, err := r.state.LoadAll()
	if err != nil {
		return 0, err
	}
	restored := 0
	for _, reg := range regs {
		dispatched, err := r.state.IsDispatched(reg.DeploymentID)
		if err != nil {
			return restored, fmt.Errorf("read dispatch fence for %s: %w", reg.DeploymentID, err)
		}
		if dispatched {
			continue
		}
		if err := r.validateShutdownChannel(reg.ShutdownChannel); err != nil {
			return restored, fmt.Errorf("restore %s: %w", reg.DeploymentID, err)
		}
		armed, err := r.arm(ctx, reg, false)
		if err != nil {
			return restored, fmt.Errorf("restore %s: %w", reg.DeploymentID, err)
		}
		if armed {
			restored++
		}
	}
	return restored, nil
}

func (r *Runtime) List() ([]Registration, error) {
	regs, err := r.state.LoadAll()
	if err != nil {
		return nil, err
	}
	sort.Slice(regs, func(i, j int) bool {
		return regs[i].Deadline.Before(regs[j].Deadline)
	})
	return regs, nil
}

func (r *Runtime) TickAll(ctx context.Context) (map[string]ratelimiter.TickResult, error) {
	regs, err := r.state.LoadAll()
	if err != nil {
		return nil, err
	}
	results := make(map[string]ratelimiter.TickResult, len(regs))
	for _, reg := range regs {
		dispatched, err := r.state.IsDispatched(reg.DeploymentID)
		if err != nil {
			return results, fmt.Errorf("read dispatch fence for %s: %w", reg.DeploymentID, err)
		}
		if dispatched {
			continue
		}
		result, err := r.limiter.Tick(ctx, lifecycleBucket(reg.DeploymentID))
		if err != nil {
			return results, fmt.Errorf("tick %s: %w", reg.DeploymentID, err)
		}
		results[reg.DeploymentID] = result
		if result.Dispatched > 0 {
			if err := r.state.MarkDispatched(reg.DeploymentID); err != nil {
				return results, fmt.Errorf("persist dispatch fence for %s: %w", reg.DeploymentID, err)
			}
		}
	}
	return results, nil
}

func (r *Runtime) Remove(ctx context.Context, deploymentID string) (bool, error) {
	removed, err := r.limiter.CancelTimer(ctx, lifecycleBucket(deploymentID))
	if err != nil {
		return false, err
	}
	if err := r.state.Delete(deploymentID); err != nil {
		return removed, err
	}
	return removed, nil
}

func (r *Runtime) RunTicker(ctx context.Context, interval time.Duration, report func(error)) {
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Redis may restart independently while Logma stays healthy. Reconcile
	// future durable registrations on every pulse so a RAM-only Redis restart
	// cannot silently discard their timer keys. If Redis or its Functions are
	// unavailable, retry a full restore before ticking again; this also
	// reconstructs already-due timers once Redis recovers.
	fullRestoreRequired := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var err error
			if fullRestoreRequired {
				_, err = r.RestoreAll(ctx)
			} else {
				_, err = r.restoreFutureTimers(ctx)
			}
			if err != nil {
				fullRestoreRequired = true
				if report != nil {
					report(fmt.Errorf("reconcile lifecycle timers: %w", err))
				}
				continue
			}
			fullRestoreRequired = false

			if _, err := r.TickAll(ctx); err != nil {
				fullRestoreRequired = true
				if report != nil {
					report(err)
				}
			}
		}
	}
}

// restoreFutureTimers repairs missing RAM-only Redis timers without rearming
// registrations whose deadline has passed. Those registrations are left to
// TickAll, avoiding repeated dispatch after a successful timer removal.
func (r *Runtime) restoreFutureTimers(ctx context.Context) (int, error) {
	regs, err := r.state.LoadAll()
	if err != nil {
		return 0, err
	}
	restored := 0
	now := time.Now()
	for _, reg := range regs {
		dispatched, err := r.state.IsDispatched(reg.DeploymentID)
		if err != nil {
			return restored, fmt.Errorf("read dispatch fence for %s: %w", reg.DeploymentID, err)
		}
		if dispatched {
			continue
		}
		if err := r.validateShutdownChannel(reg.ShutdownChannel); err != nil {
			return restored, fmt.Errorf("restore %s: %w", reg.DeploymentID, err)
		}
		if !reg.Deadline.After(now) {
			continue
		}
		armed, err := r.arm(ctx, reg, false)
		if err != nil {
			return restored, fmt.Errorf("restore %s: %w", reg.DeploymentID, err)
		}
		if armed {
			restored++
		}
	}
	return restored, nil
}

func (r *Runtime) arm(ctx context.Context, reg Registration, reset bool) (bool, error) {
	callbackData := ratelimiter.Metadata{
		"deployment_id":    reg.DeploymentID,
		"policy_code":      reg.PolicyCode,
		"shutdown_channel": reg.ShutdownChannel,
		"activated_at":     reg.ActivatedAt.UTC().Format(time.RFC3339Nano),
		"deadline":         reg.Deadline.UTC().Format(time.RFC3339Nano),
	}
	if reg.PolicyName != "" {
		callbackData["policy_name"] = string(reg.PolicyName)
	}
	for key, value := range reg.Metadata {
		callbackData[key] = value
	}
	return r.limiter.ArmTimerAt(ctx, ratelimiter.Input{
		Bucket: lifecycleBucket(reg.DeploymentID),
		Request: ratelimiter.Request{
			ID:        reg.DeploymentID,
			Subject:   "deployment",
			Operation: "lifecycle.shutdown",
			Resource:  reg.DeploymentID,
		},
		CallbackData: callbackData,
	}, reg.Deadline, reset)
}

func lifecycleBucket(deploymentID string) string {
	return "deployment:" + deploymentID
}
