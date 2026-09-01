package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	ratelimiter "github.com/dash-xd/ratelimiter"
	lifecycleprofile "github.com/dash-xd/ratelimiter/profile/lifecycle"
	"github.com/redis/go-redis/v9"
)

const defaultKeyspace = "logma:lifecycle"

type Runtime struct {
	state   FileStore
	client  *redis.Client
	limiter *ratelimiter.Limiter
}

func NewRuntime(ctx context.Context, stateDir string) (*Runtime, error) {
	client, err := ratelimiter.NewClientFromEnv()
	if err != nil {
		return nil, err
	}

	store, err := ratelimiter.NewRedisStore(client, ratelimiter.RedisConfig{Keyspace: defaultKeyspace})
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
	if lifecycleBootstrapMode() == "internal" {
		if err := store.Bootstrap(ctx, profile); err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("bootstrap lifecycle profile: %w", err)
		}
	}
	limiter, err := store.Limiter(profile)
	if err != nil {
		_ = client.Close()
		return nil, err
	}

	runtime := &Runtime{
		state:   FileStore{Dir: stateDir},
		client:  client,
		limiter: limiter,
	}
	if _, err := runtime.RestoreAll(ctx); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("restore lifecycle registrations: %w", err)
	}
	return runtime, nil
}

func lifecycleBootstrapMode() string {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("LOGMA_RATELIMITER_BOOTSTRAP")))
	if mode == "" {
		return "internal"
	}
	return mode
}

func (r *Runtime) Close() error {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.Close()
}

func (r *Runtime) Register(ctx context.Context, req RegisterRequest) (Registration, bool, error) {
	if existing, err := r.state.Load(req.DeploymentID); err == nil {
		matches, matchErr := existing.MatchesRequest(req)
		if matchErr != nil {
			return Registration{}, false, matchErr
		}
		if !matches {
			return Registration{}, false, fmt.Errorf("deployment %q already has different lifecycle intent", req.DeploymentID)
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

func (r *Runtime) RestoreAll(ctx context.Context) (int, error) {
	regs, err := r.state.LoadAll()
	if err != nil {
		return 0, err
	}
	restored := 0
	for _, reg := range regs {
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
		result, err := r.limiter.Tick(ctx, lifecycleBucket(reg.DeploymentID))
		if err != nil {
			return results, fmt.Errorf("tick %s: %w", reg.DeploymentID, err)
		}
		results[reg.DeploymentID] = result
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
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.TickAll(ctx); err != nil && report != nil {
				report(err)
			}
		}
	}
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
