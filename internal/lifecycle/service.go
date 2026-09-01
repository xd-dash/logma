package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
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

	resolver := lifecycleResolver()
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

func lifecycleResolver() ratelimiter.TargetResolver {
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

func (s *Service) Start(ctx context.Context) error {
	if err := s.redisStore.Bootstrap(ctx, lifecycleprofile.New(lifecycleResolver())); err != nil {
		return fmt.Errorf("bootstrap lifecycle profile: %w", err)
	}
	regs, err := s.store.LoadAll()
	if err != nil {
		return fmt.Errorf("load lifecycle registrations: %w", err)
	}
	for _, reg := range regs {
		if _, err := s.arm(ctx, reg, false); err != nil {
			return fmt.Errorf("reconstruct lifecycle %s: %w", reg.DeploymentID, err)
		}
		s.registrations[reg.DeploymentID] = reg
	}

	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	go s.run(runCtx)
	return nil
}

func (s *Service) Register(ctx context.Context, req RegisterRequest) (Registration, error) {
	if existing, err := s.store.Load(req.DeploymentID); err == nil {
		matches, matchErr := existing.MatchesRequest(req)
		if matchErr != nil {
			return Registration{}, matchErr
		}
		if !matches {
			return Registration{}, fmt.Errorf("deployment %q already has a different lifecycle registration", req.DeploymentID)
		}
		if _, err := s.arm(ctx, existing, false); err != nil {
			return existing, fmt.Errorf("re-arm existing lifecycle registration: %w", err)
		}
		s.mu.Lock()
		s.registrations[existing.DeploymentID] = existing
		s.mu.Unlock()
		return existing, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return Registration{}, fmt.Errorf("load existing lifecycle registration: %w", err)
	}

	reg, err := NewRegistration(req, time.Now().UTC())
	if err != nil {
		return Registration{}, err
	}
	if err := s.store.Create(reg); err != nil {
		if errors.Is(err, os.ErrExist) {
			existing, loadErr := s.store.Load(req.DeploymentID)
			if loadErr != nil {
				return Registration{}, fmt.Errorf("load concurrently-created lifecycle registration: %w", loadErr)
			}
			matches, matchErr := existing.MatchesRequest(req)
			if matchErr != nil {
				return Registration{}, matchErr
			}
			if !matches {
				return Registration{}, fmt.Errorf("deployment %q concurrently registered with different lifecycle intent", req.DeploymentID)
			}
			reg = existing
		} else {
			return Registration{}, fmt.Errorf("persist lifecycle registration: %w", err)
		}
	}

	// The durable record is authoritative and deliberately remains present if
	// Redis arming fails. A subsequent retry/start reconstructs this same absolute
	// deadline instead of creating a new one.
	if _, err := s.arm(ctx, reg, true); err != nil {
		return reg, fmt.Errorf("arm lifecycle registration: %w", err)
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

// Delete is administrative unregister, not cleanup acknowledgement. Callers
// must not use it as evidence that deployment resources were destroyed.
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

func (s *Service) arm(ctx context.Context, reg Registration, reset bool) (bool, error) {
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
	return s.limiter.ArmTimerAt(ctx, ratelimiter.Input{
		Bucket: bucketFor(reg.DeploymentID),
		Request: ratelimiter.Request{
			ID:        reg.DeploymentID,
			Subject:   "deployment",
			Operation: "lifecycle-retention",
			Resource:  reg.DeploymentID,
		},
		CallbackData: callbackData,
	}, reg.Deadline, reset)
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
