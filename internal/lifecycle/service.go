package lifecycle

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	ratelimiter "github.com/dash-xd/ratelimiter"
	lifecycleprofile "github.com/dash-xd/ratelimiter/profile/lifecycle"
	"github.com/redis/go-redis/v9"
)

const defaultTickInterval = time.Second

type Service struct {
	store        FileStore
	redisStore   *ratelimiter.RedisStore
	limiter      *ratelimiter.Limiter
	tickInterval time.Duration

	mu            sync.RWMutex
	registrations map[string]Registration
	cancel        context.CancelFunc
	done          chan struct{}
}

func NewService(client redis.UniversalClient, stateDir string, tickInterval time.Duration) (*Service, error) {
	if client == nil {
		return nil, fmt.Errorf("redis client is required")
	}
	if tickInterval <= 0 {
		tickInterval = defaultTickInterval
	}

	resolver := ratelimiter.TargetResolverFunc(func(in ratelimiter.Input, stage ratelimiter.Stage) []ratelimiter.Target {
		if stage != ratelimiter.StageShutdown {
			return nil
		}
		channel := in.CallbackData["shutdown_channel"]
		if channel == "" {
			return nil
		}
		data := make(ratelimiter.Metadata, len(in.CallbackData))
		for key, value := range in.CallbackData {
			if key != "shutdown_channel" {
				data[key] = value
			}
		}
		return []ratelimiter.Target{{
			Channel: channel,
			Purpose: ratelimiter.PurposeLifecycleControl,
			Data:    data,
		}}
	})
	profile := lifecycleprofile.New(resolver)
	redisStore, err := ratelimiter.NewRedisStore(client, ratelimiter.RedisConfig{Keyspace: "logma:lifecycle"})
	if err != nil {
		return nil, err
	}
	limiter, err := redisStore.Limiter(profile)
	if err != nil {
		return nil, err
	}

	return &Service{
		store:         FileStore{Dir: stateDir},
		redisStore:    redisStore,
		limiter:       limiter,
		tickInterval:  tickInterval,
		registrations: make(map[string]Registration),
		done:          make(chan struct{}),
	}, nil
}

func (s *Service) Start(ctx context.Context) error {
	if err := s.redisStore.Bootstrap(ctx, lifecycleprofile.New(s.resolver())); err != nil {
		return fmt.Errorf("bootstrap lifecycle profile: %w", err)
	}
	regs, err := s.store.LoadAll()
	if err != nil {
		return fmt.Errorf("load lifecycle registrations: %w", err)
	}
	for _, reg := range regs {
		if err := s.arm(ctx, reg, false); err != nil {
			return fmt.Errorf("reconstruct lifecycle %s: %w", reg.DeploymentID, err)
		}
		s.registrations[reg.DeploymentID] = reg
	}

	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	go s.run(runCtx)
	return nil
}

// resolver mirrors the resolver used to construct the limiter. It is kept as a
// method so Bootstrap receives the same profile behavior without exposing the
// resolver outside this package.
func (s *Service) resolver() ratelimiter.TargetResolver {
	return ratelimiter.TargetResolverFunc(func(in ratelimiter.Input, stage ratelimiter.Stage) []ratelimiter.Target {
		if stage != ratelimiter.StageShutdown {
			return nil
		}
		channel := in.CallbackData["shutdown_channel"]
		if channel == "" {
			return nil
		}
		data := make(ratelimiter.Metadata, len(in.CallbackData))
		for key, value := range in.CallbackData {
			if key != "shutdown_channel" {
				data[key] = value
			}
		}
		return []ratelimiter.Target{{Channel: channel, Purpose: ratelimiter.PurposeLifecycleControl, Data: data}}
	})
}

func (s *Service) Register(ctx context.Context, req RegisterRequest) (Registration, error) {
	reg, err := NewRegistration(req, time.Now())
	if err != nil {
		return Registration{}, err
	}
	if err := s.store.Save(reg); err != nil {
		return Registration{}, fmt.Errorf("persist lifecycle registration: %w", err)
	}
	if err := s.arm(ctx, reg, true); err != nil {
		_ = s.store.Delete(reg.DeploymentID)
		return Registration{}, fmt.Errorf("arm lifecycle registration: %w", err)
	}
	s.mu.Lock()
	s.registrations[reg.DeploymentID] = reg
	s.mu.Unlock()
	return reg, nil
}

func (s *Service) List() []Registration {
	s.mu.RLock()
	regs := make([]Registration, 0, len(s.registrations))
	for _, reg := range s.registrations {
		regs = append(regs, reg)
	}
	s.mu.RUnlock()
	sort.Slice(regs, func(i, j int) bool { return regs[i].Deadline.Before(regs[j].Deadline) })
	return regs
}

func (s *Service) Delete(ctx context.Context, deploymentID string) error {
	if deploymentID == "" {
		return fmt.Errorf("deployment id is required")
	}
	if _, err := s.limiter.CancelTimer(ctx, bucketFor(deploymentID)); err != nil {
		return err
	}
	if err := s.store.Delete(deploymentID); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.registrations, deploymentID)
	s.mu.Unlock()
	return nil
}

func (s *Service) Shutdown(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) arm(ctx context.Context, reg Registration, reset bool) error {
	remaining := time.Until(reg.Deadline)
	if remaining < time.Millisecond {
		remaining = time.Millisecond
	}
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
	in := ratelimiter.Input{
		Bucket: bucketFor(reg.DeploymentID),
		Request: ratelimiter.Request{
			ID:        reg.DeploymentID,
			Subject:   "deployment",
			Operation: "lifecycle-retention",
			Resource:  reg.DeploymentID,
		},
		CallbackData: callbackData,
		Preflight: ratelimiter.PreflightOptions{
			Shutdown: ratelimiter.ShutdownConditions{
				Timer: &ratelimiter.TimerCondition{After: remaining, Reset: reset},
			},
		},
	}
	_, err := s.limiter.Check(ctx, in, ratelimiter.Limit{MaxRequests: 1, Window: time.Millisecond})
	return err
}

func (s *Service) run(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *Service) tick(ctx context.Context) {
	regs := s.List()
	for _, reg := range regs {
		if _, err := s.limiter.Tick(ctx, bucketFor(reg.DeploymentID)); err != nil && ctx.Err() == nil {
			fmt.Printf("lifecycle tick failed for %s: %v\n", reg.DeploymentID, err)
		}
	}
}

func bucketFor(deploymentID string) string {
	return "deployment:" + deploymentID
}
