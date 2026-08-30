package routes

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	ratelimiter "github.com/dash-xd/ratelimiter"
	logmalifecycle "github.com/xd-dash/logma/internal/lifecycle"
	"github.com/go-chi/chi/v5"
)

var (
	lifecycleOnce    sync.Once
	lifecycleService *logmalifecycle.Service
	lifecycleInitErr error
)

func mountLifecycleRoutes(r chi.Router) {
	if !lifecycleEnabled() {
		return
	}
	lifecycleOnce.Do(func() {
		stateDir := os.Getenv("LOGMA_LIFECYCLE_STATE_DIR")
		if stateDir == "" {
			stateDir = "/var/lib/logma/lifecycle"
		}
		tickInterval := time.Second
		if raw := os.Getenv("LOGMA_LIFECYCLE_TICK_INTERVAL"); raw != "" {
			parsed, err := time.ParseDuration(raw)
			if err != nil {
				lifecycleInitErr = fmt.Errorf("parse LOGMA_LIFECYCLE_TICK_INTERVAL: %w", err)
				return
			}
			tickInterval = parsed
		}
		lifecycleService, lifecycleInitErr = logmalifecycle.NewService(client, stateDir, tickInterval)
		if lifecycleInitErr != nil {
			return
		}
		lifecycleInitErr = lifecycleService.Start(rootCtx)
	})

	r.Route("/lifecycle/api/v0.0.1", func(r chi.Router) {
		r.Get("/policies", lifecyclePoliciesHandler)
		r.Get("/registrations", lifecycleRegistrationsHandler)
		r.Post("/registrations", lifecycleRegisterHandler)
		r.Delete("/registrations/{deploymentID}", lifecycleDeleteHandler)
	})
}

func lifecycleEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LOGMA_LIFECYCLE_ENABLED"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func requireLifecycleService(w http.ResponseWriter) *logmalifecycle.Service {
	if lifecycleInitErr != nil {
		http.Error(w, "Lifecycle service failed to initialize", http.StatusServiceUnavailable)
		return nil
	}
	if lifecycleService == nil {
		http.Error(w, "Lifecycle service is disabled", http.StatusNotFound)
		return nil
	}
	return lifecycleService
}

func lifecyclePoliciesHandler(w http.ResponseWriter, _ *http.Request) {
	if requireLifecycleService(w) == nil {
		return
	}
	names := []ratelimiter.LifecyclePolicyName{
		ratelimiter.LifecycleSmoke30S,
		ratelimiter.LifecycleSmoke1M,
		ratelimiter.LifecycleSandbox1D,
		ratelimiter.LifecycleSandbox3D,
		ratelimiter.LifecycleSandbox7D,
		ratelimiter.LifecycleSandbox14D,
		ratelimiter.LifecycleSandbox30D,
	}
	type policyResponse struct {
		Name            ratelimiter.LifecyclePolicyName `json:"name"`
		PolicyCode      string                          `json:"policy_code"`
		DurationSeconds int64                           `json:"duration_seconds"`
		Energy          uint16                          `json:"energy"`
	}
	out := make([]policyResponse, 0, len(names))
	for _, name := range names {
		policy, err := ratelimiter.NamedLifecyclePolicy(name)
		if err != nil {
			http.Error(w, "Failed to compile lifecycle policy", http.StatusInternalServerError)
			return
		}
		code, err := ratelimiter.EncodePolicy(policy)
		if err != nil {
			http.Error(w, "Failed to encode lifecycle policy", http.StatusInternalServerError)
			return
		}
		out = append(out, policyResponse{
			Name:            name,
			PolicyCode:      fmt.Sprintf("%d", uint64(code)),
			DurationSeconds: int64(policy.Duration.Duration() / time.Second),
			Energy:          policy.EnergyCost(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func lifecycleRegistrationsHandler(w http.ResponseWriter, _ *http.Request) {
	service := requireLifecycleService(w)
	if service == nil {
		return
	}
	writeJSON(w, http.StatusOK, service.List())
}

func lifecycleRegisterHandler(w http.ResponseWriter, r *http.Request) {
	service := requireLifecycleService(w)
	if service == nil {
		return
	}
	defer r.Body.Close()
	var req logmalifecycle.RegisterRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "Invalid lifecycle registration", http.StatusBadRequest)
		return
	}
	reg, err := service.Register(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusCreated, reg)
}

func lifecycleDeleteHandler(w http.ResponseWriter, r *http.Request) {
	service := requireLifecycleService(w)
	if service == nil {
		return
	}
	deploymentID := chi.URLParam(r, "deploymentID")
	if err := service.Delete(r.Context(), deploymentID); err != nil {
		http.Error(w, "Failed to cancel lifecycle registration", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		fmt.Printf("Error encoding lifecycle response: %v\n", err)
	}
}
