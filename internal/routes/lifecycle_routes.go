package routes

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	ratelimiter "github.com/dash-xd/ratelimiter"
	"github.com/go-chi/chi/v5"
	logmalifecycle "github.com/xd-dash/logma/internal/lifecycle"
)

const defaultLifecycleStateDir = "/var/lib/logma/lifecycle"

type lifecycleRegisterResponse struct {
	Registration logmalifecycle.Registration `json:"registration"`
	Armed        bool                        `json:"armed"`
}

type lifecyclePolicyDescription struct {
	Name       ratelimiter.LifecyclePolicyName `json:"name"`
	PolicyCode string                          `json:"policy_code"`
	Duration   string                          `json:"duration"`
	Energy     uint16                          `json:"energy"`
}

func NewLifecycleRouter() (http.Handler, error) {
	stateDir := strings.TrimSpace(os.Getenv("LOGMA_LIFECYCLE_STATE_DIR"))
	if stateDir == "" {
		stateDir = defaultLifecycleStateDir
	}
	runtime, err := logmalifecycle.NewRuntime(rootCtx, stateDir)
	if err != nil {
		return nil, err
	}

	interval := time.Second
	if raw := strings.TrimSpace(os.Getenv("LOGMA_LIFECYCLE_TICK_INTERVAL")); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed < 100*time.Millisecond {
			_ = runtime.Close()
			return nil, fmt.Errorf("invalid LOGMA_LIFECYCLE_TICK_INTERVAL %q", raw)
		}
		interval = parsed
	}
	go runtime.RunTicker(rootCtx, interval, func(err error) {
		fmt.Printf("lifecycle tick error: %v\n", err)
	})

	r := chi.NewRouter()
	r.Use(authenticateAPIKey)

	r.Get("/", func(w http.ResponseWriter, _ *http.Request) {
		regs, err := runtime.List()
		if err != nil {
			http.Error(w, "failed to list lifecycle registrations", http.StatusInternalServerError)
			return
		}
		writeLifecycleJSON(w, http.StatusOK, regs)
	})

	r.Post("/", func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()
		var input logmalifecycle.RegisterRequest
		if err := json.NewDecoder(req.Body).Decode(&input); err != nil {
			http.Error(w, "invalid lifecycle registration", http.StatusBadRequest)
			return
		}
		reg, armed, err := runtime.Register(req.Context(), input)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeLifecycleJSON(w, http.StatusCreated, lifecycleRegisterResponse{
			Registration: reg,
			Armed:        armed,
		})
	})

	r.Post("/tick", func(w http.ResponseWriter, req *http.Request) {
		results, err := runtime.TickAll(req.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeLifecycleJSON(w, http.StatusOK, results)
	})

	// Administrative unregister only. It cancels lifecycle intent and must not be
	// interpreted as teardown proof or cleanup acknowledgement.
	r.Delete("/{deploymentID}", func(w http.ResponseWriter, req *http.Request) {
		deploymentID := chi.URLParam(req, "deploymentID")
		removed, err := runtime.Remove(req.Context(), deploymentID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeLifecycleJSON(w, http.StatusOK, map[string]any{
			"deployment_id":        deploymentID,
			"timer_removed":        removed,
			"registration_removed": true,
		})
	})

	r.Get("/policies", func(w http.ResponseWriter, _ *http.Request) {
		names := []ratelimiter.LifecyclePolicyName{
			ratelimiter.LifecycleSmoke30S,
			ratelimiter.LifecycleSmoke1M,
			ratelimiter.LifecycleSmoke10M,
			ratelimiter.LifecycleSandbox1D,
			ratelimiter.LifecycleSandbox3D,
			ratelimiter.LifecycleSandbox7D,
			ratelimiter.LifecycleSandbox14D,
			ratelimiter.LifecycleSandbox30D,
		}
		policies := make([]lifecyclePolicyDescription, 0, len(names))
		for _, name := range names {
			policy, err := ratelimiter.NamedLifecyclePolicy(name)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			code, err := ratelimiter.EncodePolicy(policy)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			policies = append(policies, lifecyclePolicyDescription{
				Name:       name,
				PolicyCode: fmt.Sprintf("%d", uint64(code)),
				Duration:   policy.Duration.Duration().String(),
				Energy:     policy.EnergyCost(),
			})
		}
		writeLifecycleJSON(w, http.StatusOK, policies)
	})

	return r, nil
}

func writeLifecycleJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		fmt.Printf("lifecycle response encoding error: %v\n", err)
	}
}
